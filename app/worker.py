"""
扫描与秒传调度
"""
import hashlib
import os
import threading
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests

from .config import get_settings, load_state, save_state
from .logger import get_logger
from .notify import flush_pending, notify_batch
from .u115 import U115Client, U115Error, U115RiskError

logger = get_logger()

CHUNK = 4 * 1024 * 1024
PREID_SIZE = 128 * 1024 * 1024


def fmt_size(size: int) -> str:
    value = float(size or 0)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if value < 1024 or unit == "TB":
            return f"{value:.1f}{unit}" if unit != "B" else f"{int(value)}B"
        value /= 1024
    return f"{size}B"


def parse_hhmm(text: str) -> Optional[Tuple[int, int]]:
    raw = str(text or "").strip().replace("：", ":")
    try:
        if ":" in raw:
            hour, minute = (int(x) for x in raw.split(":")[:2])
        elif len(raw) in (3, 4) and raw.isdigit():
            hour, minute = int(raw[:-2]), int(raw[-2:])
        else:
            return None
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return hour, minute
    except Exception:
        pass
    return None


class Worker:
    def __init__(self, client: U115Client):
        self.client = client
        self.state: Dict[str, Any] = load_state()
        self.state.setdefault("files", {})
        self._lock = threading.Lock()
        # 网盘目录 cid 缓存：net_path -> cid（每轮扫描开始时清空）
        self._cid_cache: Dict[str, str] = {}
        # 文件系统事件：等待处理的路径 -> 最后一次事件时间
        self._pending: Dict[str, float] = {}
        self._pending_sig: Dict[str, Tuple[int, int]] = {}
        self._watch_stop: Optional[threading.Event] = None
        self._watch_thread: Optional[threading.Thread] = None
        self.event_state: Dict[str, Any] = {"count": 0, "last_event_at": 0, "error": ""}
        # 下载器监控：已完成任务推过来的文件（task_id -> {路径: Path}），等主循环取走
        self._forced: Dict[str, Dict[str, Path]] = {}
        # 每个待处理路径是被谁触发的：tr / mp
        self._forced_src: Dict[str, str] = {}
        self._forced_lock = threading.Lock()
        self._poll_stop: Optional[threading.Event] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._tr_session = ""
        # 还在下载中的种子路径：这些路径下的文件先不处理，等整个种子下完
        self._tr_incomplete: set = set()
        self._tr_incomplete_at: float = 0.0
        self.tr_state: Dict[str, Any] = {
            "connected": False,
            "error": "",
            "last_poll": 0,
            "completed": 0,
            "missing": {},
        }
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.running_scan = False
        self.last_run_at: float = float(self.state.get("last_run") or 0)
        self.last_results: List[Dict[str, Any]] = list(self.state.get("last_results") or [])
        self.last_error: str = ""
        # 种子秒传完成后延迟删除：hash -> {"name":..., "due": 时间戳, "files": n}
        # 落盘在 state["tr_remove"]，重启不清零
        self._tr_remove_queue: Dict[str, Any] = dict(self.state.get("tr_remove") or {})
        # 本轮种子处理进度：hash -> {"name":..., "total": n, "done": n}
        self._tr_progress: Dict[str, Any] = {}
        # 文件路径 -> 所属种子 hash（判断"整个种子传完"用）
        self._tr_file_hash: Dict[str, str] = {}
        # 115 风控冷却：命中 429/405 后暂停上传，到点自动恢复（落盘，重启不清零）
        self.risk_state: Dict[str, Any] = dict(self.state.get("risk") or {})
        self.risk_state.setdefault("until", 0.0)
        self.risk_state.setdefault("count", 0)
        self.risk_state.setdefault("last", "")

    # ---------------------------------------------------------------- 风控冷却

    def _risk_remain(self) -> float:
        """风控冷却还剩多少秒（<=0 表示已恢复）"""
        return max(0.0, float(self.risk_state.get("until") or 0) - time.time())

    def _enter_risk_cooldown(self, message: str) -> None:
        """命中风控：按配置暂停一段时间，期间的触发不丢，只是延后"""
        cfg = get_settings()
        minutes = max(0, min(1440, int(cfg.get("risk_cooldown_min") or 0)))
        self.risk_state["until"] = time.time() + minutes * 60
        self.risk_state["count"] = int(self.risk_state.get("count") or 0) + 1
        self.risk_state["last"] = time.strftime("%m-%d %H:%M:%S")
        self.state["risk"] = self.risk_state
        self._persist()
        logger.warning(
            f"[worker] 触发 115 风控，暂停上传 {minutes} 分钟：{message}"
        )

    def _risk_ready(self) -> bool:
        """每轮开始时检查：还在冷却就跳过这一轮，避免继续撞风控"""
        if self._risk_remain() <= 0:
            return True
        if not getattr(self, "_risk_logged", False):
            remain = int(self._risk_remain())
            logger.warning(f"[worker] 115 风控冷却中，剩余 {remain} 秒，跳过本轮上传")
            self._risk_logged = True
        return False

    # ---------------------------------------------------------------- 线程

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="scanner", daemon=True)
        self._thread.start()
        self._start_watcher()
        self._start_poller()

    def stop(self) -> None:
        self._stop.set()
        self._stop_watcher()
        self._stop_poller()

    def _loop(self) -> None:
        """
        只有三种触发方式，不做周期性扫盘：
          1. 下载器任务完成（Transmission 推过来）
          2. 文件系统事件（inotify）
          3. 每天固定时间全量扫一次
        """
        while not self._stop.is_set():
            cfg = get_settings()
            try:
                if cfg.get("enabled"):
                    if not cfg.get("tr_enabled"):
                        self._clear_stale_missing()
                    else:
                        # 路径映射配置变了：旧警告里的路径已失效，清掉重新判定
                        map_sig = str(cfg.get("tr_path_map") or [])
                        if getattr(self, "_map_sig", None) != map_sig:
                            self._map_sig = map_sig
                            self._clear_stale_missing()
                    if cfg.get("watch_events_enabled", True):
                        self._flush_events(cfg)
                    if self._forced:
                        with self._forced_lock:
                            snapshot = self._forced
                            snapshot_src = self._forced_src
                            self._forced = {}
                            self._forced_src = {}
                        groups: Dict[str, Dict[str, List[Path]]] = {}
                        for task_id, paths in snapshot.items():
                            for path in paths.values():
                                src = snapshot_src.get(path.as_posix(), "tr")
                                groups.setdefault(src, {}).setdefault(task_id, []).append(path)
                        for src, buckets in groups.items():
                            label = "下载器完成" if src == "tr" else "MP 整理完成"
                            detail = "、".join(f"{k}:{len(v)}" for k, v in buckets.items())
                            logger.info(f"[worker] {label}，立即处理 {detail}")
                            self.run_once(skip_stable=True, event_files=buckets, source=src)
                    self._maybe_daily_scan(cfg)
                    self._maybe_retry_held(cfg)
                    self._maybe_cleanup(cfg)
                    self._flush_notifications()
                    self._flush_torrent_removals()
            except Exception as err:  # 任何异常都不能让线程退出
                self.last_error = str(err)
                logger.warning(f"[worker] 本轮出错：{err}")
            self._stop.wait(2)

    def _clear_stale_missing(self) -> None:
        """下载器关闭后清掉残留的路径映射警告，避免页面上一直挂着历史错误"""
        if self.tr_state.get("missing"):
            self.tr_state["missing"] = {}

    def _maybe_retry_held(self, cfg: Dict[str, Any]) -> None:
        """
        到了「秒传重试间隔」就把之前被拦截的文件重新核实一次：
        115 上的内容什么时候齐不好说，不用等到第二天。
        """
        minutes = int(cfg.get("retry_check_min") or 0)
        if minutes <= 0:
            return
        now = time.time()
        last = float(self.state.get("last_held_retry") or 0)
        if now - last < minutes * 60:
            return

        # 找出到点的 held 记录，按任务分桶（key 形如 task_id|相对路径）
        buckets: Dict[str, List[Path]] = {}
        for key, record in (self.state.get("files") or {}).items():
            record = record or {}
            if record.get("status") != "held":
                continue
            held_at = float(record.get("held_at") or record.get("last_try") or 0)
            if held_at and now - held_at < minutes * 60:
                continue
            task_id, _, rel = str(key).partition("|")
            task = next(
                (t for t in self._enabled_tasks(cfg) if str(t.get("id") or "default") == task_id),
                None,
            )
            if not task or not rel:
                continue
            path = Path(str(task.get("watch_dir"))).expanduser() / rel
            try:
                if path.exists() and path.is_file():
                    buckets.setdefault(task_id, []).append(path)
            except OSError:
                continue

        self.state["last_held_retry"] = now
        self._persist()
        if not buckets:
            return
        total = sum(len(v) for v in buckets.values())
        logger.info(f"[worker] 到点重新核实 {total} 个被拦截的文件能否秒传")
        self.run_once(skip_stable=True, event_files=buckets, source="retry")

    # ---------------------------------------------------------------- 种子延迟删除

    def _queue_torrent_removal(self, torrent_hash: str, progress: Dict[str, Any]) -> None:
        """整个种子都秒传完成且本地文件已删：延迟一段时间后从下载器移除"""
        cfg = get_settings()
        if not cfg.get("tr_remove_after_done"):
            return
        delay = max(0, min(86400, int(cfg.get("tr_remove_delay_sec") or 0)))
        name = str(progress.get("name") or "")
        self._tr_remove_queue[torrent_hash] = {
            "name": name,
            "due": time.time() + delay,
            "files": int(progress.get("total") or 0),
        }
        self.state["tr_remove"] = self._tr_remove_queue
        self._persist()
        logger.info(f"[worker] 种子已全部秒传并删除本地文件，{delay} 秒后从下载器移除：{name}")

    def _flush_torrent_removals(self) -> None:
        """到点的种子调 torrent-remove；不删本地数据（本地已经由助手处理过）"""
        cfg = get_settings()
        if not cfg.get("tr_enabled") or not cfg.get("tr_remove_after_done"):
            return
        if not self._tr_remove_queue:
            return
        now = time.time()
        for torrent_hash, info in list(self._tr_remove_queue.items()):
            if float((info or {}).get("due") or 0) > now:
                continue
            name = str((info or {}).get("name") or torrent_hash[:8])
            data = self._tr_rpc(
                "torrent-remove",
                {"ids": [torrent_hash], "delete-local-data": False},
            )
            ok = bool(data) and str(data.get("result") or "") == "success"
            if ok:
                self._tr_remove_queue.pop(torrent_hash, None)
                self.tr_state["removed"] = int(self.tr_state.get("removed") or 0) + 1
                logger.info(f"[worker] 已从下载器移除种子：{name}")
            else:
                info["due"] = now + 60   # 失败 1 分钟后再试
                logger.warning(f"[worker] 移除种子失败，1 分钟后再试：{name}")
        self.state["tr_remove"] = self._tr_remove_queue
        self._persist()

    def _mark_tr_file_done(self, path: Path, item: Dict[str, Any]) -> None:
        """某个种子里的一个文件处理完了：累计进度，全齐了就排进删除队列"""
        try:
            torrent_hash = self._tr_file_hash.get(path.as_posix())
        except Exception:
            return
        if not torrent_hash:
            return
        progress = self._tr_progress.get(torrent_hash)
        if not progress:
            return
        progress["done"] = int(progress.get("done") or 0) + 1
        # 有任何一个文件保留了源文件（要做种），就不动这个种子
        if not item.get("local_deleted"):
            progress["keep"] = True
        if progress["done"] >= int(progress.get("total") or 0) and not progress.get("keep"):
            self._tr_progress.pop(torrent_hash, None)
            self._queue_torrent_removal(torrent_hash, progress)

    def _flush_notifications(self) -> None:
        """CMS 有 5 分钟最小间隔；间隔内来的触发先攒着，这里每轮检查一次到点补发"""
        try:
            for err in flush_pending():
                self.last_error = err
                logger.warning(f"[worker] 补发通知失败：{err}")
        except Exception as err:
            logger.warning(f"[worker] 补发通知出错：{err}")

    def _maybe_daily_scan(self, cfg: Dict[str, Any]) -> None:
        """
        每天到了设定时间，把监控目录完整扫一遍：
        处理"从别处移进来"的文件，也顺手重试之前秒传失败的文件。
        正在写入的文件会被 stable_seconds 挡掉
        """
        if not cfg.get("daily_scan_enabled", True):
            return
        parsed = parse_hhmm(str(cfg.get("daily_scan_time") or "09:00"))
        if not parsed:
            return
        now = datetime.now()
        today_key = now.strftime("%Y-%m-%d")
        if self.state.get("last_daily_scan") == today_key:
            return
        if (now.hour, now.minute) < parsed:
            return
        self.state["last_daily_scan"] = today_key
        self._persist()
        logger.info(f"[worker] 每日定时扫描开始（{cfg.get('daily_scan_time')}）")
        results = self.run_once(source="daily")
        logger.info(f"[worker] 每日定时扫描结束，处理 {len(results)} 个文件")

    # ---------------------------------------------------------------- 文件系统事件

    def _watcher_alive(self) -> bool:
        return bool(self._watch_thread and self._watch_thread.is_alive())

    def _start_watcher(self) -> None:
        """
        用 watchfiles(inotify) 监听各任务的监控目录。事件只做记录，真正的处理交给 _flush_events
        """
        cfg = get_settings()
        if not cfg.get("watch_events_enabled", True):
            return
        if self._watcher_alive():
            return
        try:
            import watchfiles  # noqa: F401  # 只用来确认依赖存在
        except Exception as err:
            self.event_state["error"] = f"watchfiles 不可用，已退化为定时扫描：{err}"
            logger.warning(f"[worker] {self.event_state['error']}")
            return
        dirs = [str(t.get("watch_dir")) for t in self._enabled_tasks(cfg) if t.get("watch_dir")]
        dirs = [d for d in dirs if Path(d).expanduser().is_dir()]
        if not dirs:
            self.event_state["error"] = "没有可监听的监控目录"
            return
        self._watch_stop = threading.Event()
        self.event_state["error"] = ""

        def _loop():
            from watchfiles import watch

            try:
                for changes in watch(
                    *dirs,
                    watch_filter=None,      # 不过滤扩展名/隐藏文件，全部交给我们判断
                    stop_event=self._watch_stop,
                    recursive=True,
                    debounce=1500,          # 合并短时间内的连续写事件
                    step=200,
                ):
                    if self._watch_stop.is_set():
                        break
                    now = time.time()
                    for _change, raw in changes:
                        self._pending[str(raw)] = now
                        self._pending_sig.pop(str(raw), None)
                    self.event_state["last_event_at"] = now
                    self.event_state["count"] = int(self.event_state.get("count") or 0) + 1
            except Exception as err:
                self.event_state["error"] = f"监听线程退出：{err}"
                logger.warning(f"[worker] {self.event_state['error']}")

        self._watch_thread = threading.Thread(target=_loop, name="watcher", daemon=True)
        self._watch_thread.start()
        self.event_state["dirs"] = dirs
        logger.info(f"[worker] 已启动文件系统监听：{dirs}")

    def refresh_watcher(self) -> None:
        """
        设置变更后重建监听（监控目录/模式可能变了）
        """
        self._stop_watcher()
        self._stop_poller()
        if get_settings().get("watch_events_enabled", True):
            self._start_watcher()
        self._start_poller()

    def _stop_watcher(self) -> None:
        if self._watch_stop:
            self._watch_stop.set()
        self._watch_thread = None
        self._pending.clear()
        self._pending_sig.clear()

    def _flush_events(self, cfg: Dict[str, Any]) -> None:
        """
        把"事件已经过去 stable_seconds、且文件大小/修改时间连续两次一致"的文件挑出来处理，
        只处理这些文件，不做全盘 walk
        """
        if not self._pending:
            return
        settle = max(2, int(cfg.get("stable_seconds") or 0))
        now = time.time()
        ready: List[Path] = []
        for raw, stamp in list(self._pending.items()):
            if now - float(stamp) < settle:
                continue
            path = Path(raw)
            if path.is_dir():
                # 某些实现只上报目录变化：把目录里的文件补进来（不递归，子目录自己会报事件）
                self._pending.pop(raw, None)
                try:
                    for child in path.iterdir():
                        if child.is_file() and self._acceptable(child, cfg):
                            self._pending.setdefault(str(child), now)
                except Exception:
                    pass
                continue
            if not path.exists() or not path.is_file():
                self._pending.pop(raw, None)
                self._pending_sig.pop(raw, None)
                continue
            try:
                stat = path.stat()
                sig = (int(stat.st_size), int(stat.st_mtime))
            except Exception:
                continue
            # 大小/时间还在变 → 继续等（重置计时）
            if self._pending_sig.get(raw) != sig:
                self._pending_sig[raw] = sig
                self._pending[raw] = now
                continue
            self._pending.pop(raw, None)
            self._pending_sig.pop(raw, None)
            if self._acceptable(path, cfg):
                ready.append(path)
        if not ready:
            return
        # 按任务分桶
        buckets: Dict[str, List[Path]] = {}
        for path in ready:
            for task in self._enabled_tasks(cfg):
                watch = Path(str(task.get("watch_dir"))).expanduser()
                try:
                    if path.is_relative_to(watch):
                        buckets.setdefault(str(task.get("id") or "default"), []).append(path)
                        break
                except Exception:
                    continue
        if buckets:
            self.run_once(skip_stable=True, event_files=buckets, source="event")

    # ---------------------------------------------------------------- 主流程

    def run_once(
        self,
        skip_stable: bool = False,
        event_files: Optional[Dict[str, List[Path]]] = None,
        source: str = "daily",
    ) -> List[Dict[str, Any]]:
        if self.running_scan:
            return []
        self.running_scan = True
        if event_files is None:
            self._cid_cache = {}   # 全量扫描才刷新目录缓存
        results: List[Dict[str, Any]] = []
        try:
            cfg = get_settings()
            if not self.client.is_authorized():
                self.last_error = "115 未授权，请先在网页里扫码登录"
                return []

            tasks = self._enabled_tasks(cfg)
            if not tasks:
                self.last_error = "还没有配置监控任务（本地目录 → 115 目录）"
                return []

            # 风控冷却期：整轮跳过，别继续撞 115。
            # 事件触发的文件已经被 _flush_events 从 _pending 弹出，必须放回去，否则会丢
            if not self._risk_ready():
                if event_files:
                    now = time.time()
                    for paths in event_files.values():
                        for p in paths:
                            self._pending[p.as_posix()] = now
                return []

            max_files = max(1, min(500, int(cfg.get("max_files_per_run") or 10)))
            delay = max(0, min(300, int(cfg.get("upload_delay_sec") or 0)))
            processed = 0
            skipped = 0
            self._risk_logged = False

            for task in tasks:
                if self._stop.is_set():
                    break
                watch = Path(str(task.get("watch_dir") or "")).expanduser()
                if not watch.exists() or not watch.is_dir():
                    self.last_error = f"[{task.get('name')}] 监控目录不存在：{watch}"
                    continue
                try:
                    cid = self.client.ensure_dir(str(task.get("target_dir") or "/"))
                except U115Error as err:
                    self.last_error = f"[{task.get('name')}] 解析网盘目录失败：{err}"
                    continue

                if event_files is not None:
                    candidates = [
                        p
                        for p in event_files.get(str(task.get("id") or "default"), [])
                        if self._acceptable(p, cfg)
                    ]
                else:
                    candidates = self._list_candidates(watch, cfg)

                # 没轮到的事件文件放回 _pending，下一轮继续；全量扫描的文件本来就会再扫到
                def requeue(paths: List[Path]) -> None:
                    nonlocal skipped
                    skipped += len(paths)
                    if event_files is not None:
                        now = time.time()
                        for p in paths:
                            self._pending[p.as_posix()] = now

                if self._risk_remain() > 0:
                    requeue(candidates)
                    continue

                for path in candidates:
                    if self._stop.is_set():
                        if event_files is not None:
                            requeue([p for p in candidates if p != path])
                        break
                    # 每轮上限：超出的放回队列，留到下一轮，不会丢
                    if processed >= max_files:
                        requeue(candidates[candidates.index(path):])
                        break
                    # 相邻文件之间强制间隔，降低 115 的请求速率
                    if processed > 0 and delay > 0:
                        self._stop.wait(delay)
                        if self._stop.is_set():
                            requeue(candidates[candidates.index(path):])
                            break
                    try:
                        item = self._process(
                            path, watch, cid, cfg, skip_stable, task, source
                        )
                        processed += 1
                    except U115RiskError as err:
                        self._enter_risk_cooldown(str(err))
                        # 当前这个文件也没传成功，和后面的一起放回队列
                        requeue(candidates[candidates.index(path):])
                        break
                    except Exception as err:
                        item = {
                            "file": path.name,
                            "task": task.get("name") or "",
                            "source": source,
                            "status": "失败",
                            "size": fmt_size(path.stat().st_size if path.exists() else 0),
                            "note": str(err),
                        }
                        self.last_error = str(err)
                    if item:
                        item.setdefault(
                            "cms", bool(task.get("cms_notify", True))
                        )
                        self._log_item(task, item)
                        results.append(item)
                        # 种子里的这个文件处理完了，累计进度（整种子齐了才排删除）
                        if item.get("status") == "成功" and source == "tr":
                            self._mark_tr_file_done(path, item)
                    self._persist()

            if skipped:
                logger.info(
                    f"[worker] 本轮限流：已处理 {processed} 个，{skipped} 个留到下一轮"
                    + (f"（风控冷却剩余 {int(self._risk_remain())} 秒）" if self._risk_remain() > 0 else "")
                )

            self.last_run_at = time.time()
            self.last_results = results[-50:]
            self.state["last_run"] = self.last_run_at
            self.state["last_results"] = self.last_results
            self._persist()

            # 定时重试只报"这次终于成功了"的，否则每轮都发一条失败通知会刷屏
            notify_items = results
            if source == "retry":
                notify_items = [r for r in results if r.get("status") == "成功"]
            if notify_items:
                for err in notify_batch(notify_items):
                    self.last_error = err
                    logger.warning(f"[worker] 通知失败：{err}")
        finally:
            self.running_scan = False
        return results

    def _process(
        self,
        path: Path,
        watch: Path,
        cid: str,
        cfg: Dict[str, Any],
        skip_stable: bool,
        task: Optional[Dict[str, Any]] = None,
        source: str = "daily",
    ) -> Optional[Dict[str, Any]]:
        task = task or {}
        stat = path.stat()
        size, mtime = int(stat.st_size), int(stat.st_mtime)
        task_id = str(task.get("id") or "default")
        rel_posix = path.relative_to(watch).as_posix()
        key = f"{task_id}|{rel_posix}"

        # 保持本地目录结构：/115/蜘蛛侠/第一季/第一集.mkv -> 网盘 {目标目录}/蜘蛛侠/第一季/
        net_dir = self._net_dir_for(task, rel_posix)
        try:
            target_cid = self._ensure_sub_dir(cid, task.get("target_dir") or "/", net_dir)
        except Exception as err:
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "status": "失败",
                "size": fmt_size(size),
                "note": f"创建网盘目录失败：{err}",
            }
        files = self.state["files"]
        record = files.get(key) or {}

        if record.get("size") != size or record.get("mtime") != mtime:
            record = {"size": size, "mtime": mtime, "first_seen": time.time(), "tries": 0}
            files[key] = record

        if record.get("status") == "done":
            return None

        # 文件可能还在写入（下载器/文件事件触发时已知写完，跳过这个检查）
        stable = max(0, int(cfg.get("stable_seconds") or 0))
        if not skip_stable and stable and time.time() - mtime < stable:
            return None

        # 网盘已有同名文件
        try:
            if self.client.find_file(target_cid, path.name):
                record["status"] = "done"
                record["done_at"] = time.time()
                record["note"] = "网盘已有同名文件"
                deleted = self._finish_local(path, cfg, task)
                return {
                    "file": rel_posix,
                    "task": task.get("name") or "",
                    "source": source,
                    "status": "成功",
                    "size": fmt_size(size),
                    "local_deleted": deleted,
                    "note": "网盘已有同名文件，"
                    + ("已删除源文件" if deleted else "保留源文件"),
                }
        except U115Error:
            pass

        sha1 = record.get("sha1")
        preid = record.get("preid")
        if not sha1 or not preid:
            sha1, preid = self._hash_pair(path)
            record["sha1"], record["preid"] = sha1, preid
        record["last_try"] = time.time()
        record["tries"] = int(record.get("tries") or 0) + 1
        record["source"] = source

        ok, detail = self.client.rapid_upload(
            target_cid,
            path.name,
            size,
            sha1,
            preid,
            sign_resolver=lambda check: self._sign_value(path, check),
        )

        if ok:
            record["status"] = "done"
            record["done_at"] = time.time()
            record["note"] = "秒传成功"
            deleted = self._finish_local(path, cfg, task)
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "source": source,
                "status": "成功",
                "size": fmt_size(size),
                "local_deleted": deleted,
                "note": "已复制到网盘，已删除源文件" if deleted else "已复制到网盘，保留源文件",
            }

        if detail.startswith("115 上没有相同内容"):
            # 开了"真实上传"就直接传，否则拦截等下次
            if cfg.get("real_upload"):
                logger.info(f"[worker] {rel_posix} 不能秒传，开始真实上传")
                uploaded, upload_detail = self.client.real_upload(
                    target_cid, path.name, path, sha1, preid
                )
                if uploaded:
                    record["status"] = "done"
                    record["done_at"] = time.time()
                    record["note"] = upload_detail
                    deleted = self._finish_local(path, cfg, task)
                    return {
                        "file": rel_posix,
                        "task": task.get("name") or "",
                        "source": source,
                        "status": "成功",
                        "size": fmt_size(size),
                        "local_deleted": deleted,
                        "note": "真实上传完成，已删除源文件"
                        if deleted
                        else "真实上传完成，保留源文件",
                    }
                record["status"] = "held"
                record["note"] = upload_detail
                logger.warning(f"[worker] {rel_posix} 真实上传失败：{upload_detail}")
                return {
                    "file": rel_posix,
                    "task": task.get("name") or "",
                    "source": source,
                    "status": "失败",
                    "size": fmt_size(size),
                    "note": f"真实上传失败：{upload_detail}",
                }

            record["status"] = "held"
            record["note"] = detail
            record["held_at"] = time.time()
            retry_min = int(cfg.get("retry_check_min") or 0)
            when = (
                f"{retry_min} 分钟后自动核实"
                if retry_min > 0
                else f"明天 {str(cfg.get('daily_scan_time') or '09:00')} 扫描时再试"
            )
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "source": source,
                "status": "拦截",
                "size": fmt_size(size),
                "note": f"暂不能秒传，{when}",
            }

        record["note"] = detail
        return {
            "file": rel_posix,
            "task": task.get("name") or "",
            "source": source,
            "status": "失败",
            "size": fmt_size(size),
            "note": detail,
        }

    @staticmethod
    def _log_item(task: Dict[str, Any], item: Dict[str, Any]) -> None:
        """
        每个文件的处理结果都单独记一条，排障时能直接搜文件名
        """
        name = item.get("file") or ""
        task_name = task.get("name") or ""
        prefix = f"[秒传] {task_name}/" if task_name else "[秒传] "
        note = item.get("note") or ""
        status = item.get("status")
        if status == "成功":
            logger.info(f"{prefix}{name} 成功（{item.get('size') or ''}）：{note}")
        elif status == "拦截":
            logger.info(f"{prefix}{name} 暂不能秒传：{note}")
        else:
            logger.warning(f"{prefix}{name} 失败：{note}")

    @staticmethod
    def _net_dir_for(task: Dict[str, Any], rel_posix: str) -> str:
        """
        本地相对路径 -> 网盘目录（保持目录结构）
        task.target_dir=/电影, rel=蜘蛛侠/第一季/第一集.mkv  ->  /电影/蜘蛛侠/第一季
        """
        base = str(task.get("target_dir") or "/").rstrip("/")
        parent = Path(rel_posix).parent.as_posix()
        if parent in ("", "."):
            return base or "/"
        return f"{base}/{parent}" if base else f"/{parent}"

    def _ensure_sub_dir(self, root_cid: str, base_dir: str, net_dir: str) -> str:
        """
        解析/创建网盘子目录的 cid。网盘里已有同名目录就直接复用（天然是"合并"）
        """
        base = str(base_dir or "/").rstrip("/") or "/"
        if not net_dir or net_dir in ("/", base):
            return root_cid   # 就是任务根目录，直接用已有的 cid
        cached = self._cid_cache.get(net_dir)
        if cached:
            return cached
        cid = self.client.ensure_dir(net_dir)
        self._cid_cache[net_dir] = cid
        return cid

    def _finish_local(
        self, path: Path, cfg: Dict[str, Any], task: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        秒传永远是"复制"到网盘（源文件本来就在），这里只处理本地源文件要不要删。
        任务可以单独声明「保留源文件」——比如下载目录还要做种/给 MP 整理，不能删。
        """
        if task and bool(task.get("keep_source")):
            return False
        if not cfg.get("delete_source", True):
            return False
        try:
            path.unlink()
            return True
        except Exception as err:
            logger.warning(f"[worker] 删除源文件失败：{path} - {err}")
            return False

    # ---------------------------------------------------------------- 工具

    @staticmethod
    def _enabled_tasks(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        tasks = cfg.get("tasks") or []
        return [
            t
            for t in tasks
            if isinstance(t, dict) and t.get("watch_dir") and t.get("enabled", True)
        ]

    # 下载中的临时文件不碰（否则会把半截文件传上去）
    IGNORE_SUFFIX = (
        ".part",
        ".!qb",
        ".!ut",
        ".tmp",
        ".crdownload",
        ".downloading",
        ".aria2",
        ".bc!",
        ".td",
    )

    def _acceptable(self, path: Path, cfg: Dict[str, Any]) -> bool:
        """
        单个文件是否该处理：不看扩展名，只排除隐藏文件和下载临时文件
        """
        try:
            path = Path(path)
            if not path.is_file():
                return False
            name = path.name
            if name.startswith("."):
                return False
            if name.lower().endswith(self.IGNORE_SUFFIX):
                return False
            if self._in_incomplete_torrent(path):
                return False
            size = path.stat().st_size
            min_size = max(0, int(cfg.get("min_size_mb") or 0)) * 1024 * 1024
            if size <= 0 or size < min_size:
                return False
            return True
        except Exception:
            return False

    def _in_incomplete_torrent(self, path: Path) -> bool:
        """
        文件是否属于"还没下完"的种子。是的话先跳过——等整个种子下完再一起秒传
        """
        if not self._tr_incomplete:
            return False
        # 下载器长时间连不上时，这份名单会过期，避免文件被永久跳过
        if time.time() - self._tr_incomplete_at > 1800:
            self._tr_incomplete = set()
            return False
        try:
            text = Path(path).as_posix()
        except Exception:
            return False
        for prefix in self._tr_incomplete:
            if text == prefix or text.startswith(prefix + "/"):
                return True
        return False

    def _list_candidates(self, watch: Path, cfg: Dict[str, Any]) -> List[Path]:
        """
        兜底全量扫描：监控目录下的所有文件都算候选（不看扩展名）
        """
        iterator = watch.rglob("*") if cfg.get("recursive", True) else watch.glob("*")
        result: List[Path] = []
        for path in iterator:
            if self._acceptable(path, cfg):
                result.append(path)
        result.sort(key=lambda p: p.stat().st_mtime)
        return result

    @staticmethod
    def _hash_pair(path: Path) -> Tuple[str, str]:
        sha1_all = hashlib.sha1()
        sha1_pre = hashlib.sha1()
        remain = PREID_SIZE
        with open(path, "rb") as handle:
            while True:
                chunk = handle.read(CHUNK)
                if not chunk:
                    break
                sha1_all.update(chunk)
                if remain > 0:
                    take = chunk[:remain]
                    sha1_pre.update(take)
                    remain -= len(take)
        return sha1_all.hexdigest(), sha1_pre.hexdigest()

    @staticmethod
    def _sign_value(path: Path, sign_check: str) -> Optional[str]:
        """
        sign_check 形如 "2392148-2392298"，回传该区间内容的 SHA1（大写）
        """
        try:
            start_text, end_text = str(sign_check).split("-")[:2]
            start, end = int(start_text), int(end_text)
            with open(path, "rb") as handle:
                handle.seek(start)
                chunk = handle.read(max(0, end - start) + 1)
            return hashlib.sha1(chunk).hexdigest().upper()
        except Exception:
            return None

    # ---------------------------------------------------------------- 下载器监控（Transmission）

    def _start_poller(self) -> None:
        cfg = get_settings()
        if not cfg.get("tr_enabled") or not str(cfg.get("tr_url") or "").strip():
            return
        if self._poll_thread and self._poll_thread.is_alive():
            return
        self._poll_stop = threading.Event()

        def _loop():
            while not self._poll_stop.is_set():
                try:
                    self._poll_transmission()
                except Exception as err:
                    self.tr_state["error"] = str(err)
                    logger.warning(f"[worker] 查询下载器出错：{err}")
                interval = max(5, int(get_settings().get("tr_poll_sec") or 30))
                self._poll_stop.wait(interval)

        self._poll_thread = threading.Thread(target=_loop, name="tr-poller", daemon=True)
        self._poll_thread.start()
        logger.info(f"[worker] 已启动下载器监控：{cfg.get('tr_url')}")

    def _stop_poller(self) -> None:
        if self._poll_stop:
            self._poll_stop.set()
        self._poll_thread = None

    def _tr_rpc(self, method: str, arguments: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Transmission RPC。首次会 409 并返回 X-Transmission-Session-Id，拿到后重试一次
        """
        cfg = get_settings()
        url = str(cfg.get("tr_url") or "").strip().rstrip("/")
        if not url:
            self.tr_state["error"] = "未填写下载器地址"
            return None
        if not url.endswith("/transmission/rpc"):
            url += "/transmission/rpc"
        auth = None
        user = str(cfg.get("tr_username") or "")
        if user:
            auth = (user, str(cfg.get("tr_password") or ""))
        headers = {}
        if self._tr_session:
            headers["X-Transmission-Session-Id"] = self._tr_session
        payload = {"method": method, "arguments": arguments}
        for _ in range(2):
            try:
                resp = requests.post(
                    url, json=payload, headers=headers, auth=auth, timeout=15
                )
            except Exception as err:
                self.tr_state["connected"] = False
                self.tr_state["error"] = f"连接失败：{err}"
                return None
            if resp.status_code == 409:
                self._tr_session = resp.headers.get("X-Transmission-Session-Id", "")
                headers["X-Transmission-Session-Id"] = self._tr_session
                continue
            if resp.status_code == 401:
                self.tr_state["connected"] = False
                self.tr_state["error"] = "账号或密码错误（401）"
                return None
            if resp.status_code != 200:
                self.tr_state["connected"] = False
                self.tr_state["error"] = f"HTTP {resp.status_code}"
                return None
            self.tr_state["connected"] = True
            self.tr_state["error"] = ""
            try:
                return resp.json()
            except Exception:
                return None
        return None

    def _poll_transmission(self) -> None:
        """
        查已完成的任务，把对应文件塞进 _forced，等主循环立即处理
        """
        cfg = get_settings()
        if not cfg.get("tr_enabled"):
            self._clear_stale_missing()
            return
        data = self._tr_rpc(
            "torrent-get",
            {
                "fields": [
                    "hashString",
                    "name",
                    "downloadDir",
                    "percentDone",
                    "status",
                    "isFinished",
                    "error",
                    "errorString",
                ]
            },
        )
        self.tr_state["last_poll"] = time.time()
        if not data:
            return
        torrents = ((data.get("arguments") or {}).get("torrents")) or []
        done_map = self.state.setdefault("tr_done", {})
        missing = self.tr_state.setdefault("missing", {})

        def _is_finished(torrent: Dict[str, Any]) -> bool:
            return (
                bool(torrent.get("isFinished"))
                or float(torrent.get("percentDone") or 0) >= 1
                or int(torrent.get("status") or 0) == 6
            )

        # 第一遍：先算清楚"还在下载"的种子，并立刻生效
        # （否则种子刚下完的那一轮，文件会被自己挡掉）
        new_incomplete = set()
        for torrent in torrents:
            if not _is_finished(torrent):
                path = self._resolve_download_path(torrent, cfg)
                if path:
                    new_incomplete.add(path.as_posix())
        if self._tr_incomplete != new_incomplete:
            logger.info(
                f"[worker] 仍在下载中的种子 {len(new_incomplete)} 个，其中的文件暂不处理"
            )
        self._tr_incomplete = new_incomplete
        self._tr_incomplete_at = time.time()

        # 第二遍：处理已完成的任务
        for torrent in torrents:
            torrent_hash = str(torrent.get("hashString") or "")
            if not torrent_hash:
                continue
            if not _is_finished(torrent):
                continue
            if torrent_hash in done_map:
                continue
            if int(torrent.get("error") or 0) != 0:
                continue
            path = self._resolve_download_path(torrent, cfg)
            name = str(torrent.get("name") or "")
            if not path or not path.exists():
                # 两种"找不到"要分开处理，否则会把"文件已被移走"错报成"映射配错"：
                #   1) 映射后的父目录存在 → 映射是通的，只是这个种子的内容不在了
                #      （MP 整理移走 / 秒传后删了源文件 / 手动删了），是残留种子
                #   2) 父目录也不存在 → 真的是路径映射配错
                parent_ok = False
                if path:
                    try:
                        parent_ok = Path(path).parent.exists()
                    except OSError:
                        parent_ok = False
                if parent_ok:
                    stale = self.tr_state.setdefault("stale", {})
                    if torrent_hash not in stale:
                        stale[torrent_hash] = {
                            "name": name,
                            "path": str(path),
                            "at": time.strftime("%m-%d %H:%M"),
                        }
                        logger.info(
                            f"[worker] 种子内容已不存在（文件被移走或已秒传删除），忽略：{name}"
                        )
                        # 开了自动清理就顺手把这个残留种子从下载器移除
                        if cfg.get("tr_remove_after_done"):
                            self._queue_torrent_removal(
                                torrent_hash, {"name": name, "total": 0}
                            )
                    missing.pop(torrent_hash, None)
                    done_map[torrent_hash] = time.time()
                    self._persist()
                    continue
                if torrent_hash not in missing:
                    missing[torrent_hash] = {
                        "name": name,
                        "path": str(path or ""),
                        "reason": (
                            f"找不到 {path}，它所在的上级目录也不存在，"
                            "检查「路径映射」里「本容器路径」是否写对"
                        ),
                        "at": time.strftime("%m-%d %H:%M"),
                    }
                    logger.info(
                        f"[worker] 下载已完成但路径映射指向的目录不存在：{name} -> {path}"
                    )
                continue
            files = self._files_from_torrent(path, cfg)
            if not files:
                if torrent_hash not in missing:
                    missing[torrent_hash] = {
                        "name": name,
                        "path": str(path),
                        "reason": "目录里没有可处理的文件（可能全是临时文件）",
                        "at": time.strftime("%m-%d %H:%M"),
                    }
                    logger.info(f"[worker] 下载已完成但目录里没有可处理的文件：{path}")
                continue
            buckets: Dict[str, List[Path]] = {}
            for file_path in files:
                task_id = self._task_id_for(file_path, cfg)
                if task_id:
                    buckets.setdefault(task_id, []).append(file_path)
            if not buckets:
                if torrent_hash not in missing:
                    missing[torrent_hash] = {
                        "name": name,
                        "path": str(path),
                        "reason": "不在任何监控目录里，检查路径映射或加一条任务",
                        "at": time.strftime("%m-%d %H:%M"),
                    }
                    logger.info(f"[worker] 下载完成，但不在任何监控目录里：{path}")
                continue
            with self._forced_lock:
                for task_id, items in buckets.items():
                    self._forced.setdefault(task_id, {})
                    for item in items:
                        self._forced[task_id][item.as_posix()] = item
                        self._forced_src[item.as_posix()] = "tr"
                        # 记住这个文件属于哪个种子，处理完才能判断"整种子是否都传完"
                        self._tr_file_hash[item.as_posix()] = torrent_hash
            self._tr_progress[torrent_hash] = {
                "name": name,
                "total": len(files),
                "done": 0,
            }
            missing.pop(torrent_hash, None)
            done_map[torrent_hash] = time.time()
            self.tr_state["completed"] = int(self.tr_state.get("completed") or 0) + 1
            self._persist()
            logger.info(f"[worker] 下载完成，准备秒传：{name}（{len(files)} 个文件）")


    @staticmethod
    def _map_path(raw_path: str, cfg: Dict[str, Any]) -> str:
        """
        路径映射：把下载器里的路径换成容器里能看到的位置（长前缀优先）
        """
        text = str(raw_path or "")
        maps = cfg.get("tr_path_map") or []
        for item in sorted(
            [m for m in maps if isinstance(m, dict)],
            key=lambda m: len(str(m.get("from") or "")),
            reverse=True,
        ):
            src = str(item.get("from") or "").rstrip("/")
            dst = str(item.get("to") or "").rstrip("/")
            if not src:
                continue
            if text == src or text.startswith(src + "/"):
                return dst + text[len(src) :]
        return text

    def _resolve_download_path(
        self, torrent: Dict[str, Any], cfg: Dict[str, Any]
    ) -> Optional[Path]:
        name = str(torrent.get("name") or "").strip()
        base = str(torrent.get("downloadDir") or "").strip()
        if not name:
            return None
        raw = f"{base.rstrip('/')}/{name}" if base else name
        return Path(self._map_path(raw, cfg))

    def _files_from_torrent(self, path: Path, cfg: Dict[str, Any]) -> List[Path]:
        """
        种子可能是单文件，也可能是一个目录（目录就递归取里面的文件）
        """
        try:
            if path.is_file():
                return [path] if self._acceptable(path, cfg) else []
            if not path.is_dir():
                return []
            iterator = path.rglob("*") if cfg.get("recursive", True) else path.glob("*")
            return [p for p in iterator if self._acceptable(p, cfg)]
        except Exception:
            return []

    def _task_id_for(self, path: Path, cfg: Dict[str, Any]) -> Optional[str]:
        for task in self._enabled_tasks(cfg):
            watch = Path(str(task.get("watch_dir"))).expanduser()
            try:
                if path.is_relative_to(watch):
                    return str(task.get("id") or "default")
            except Exception:
                continue
        return None

    # ---------------------------------------------------------------- 空目录清理

    def _maybe_cleanup(self, cfg: Dict[str, Any]) -> None:
        if not cfg.get("cleanup_enabled"):
            return
        parsed = parse_hhmm(str(cfg.get("cleanup_time") or "03:30"))
        if not parsed:
            return
        now = datetime.now()
        today_key = now.strftime("%Y-%m-%d")
        if self.state.get("last_cleanup_date") == today_key:
            return
        if (now.hour, now.minute) < parsed:
            return
        removed = self.cleanup_empty_dirs()
        self.state["last_cleanup_date"] = today_key
        self.state["last_cleanup_count"] = len(removed)
        self._persist()
        if removed:
            logger.info(f"[worker] 清理空目录 {len(removed)} 个")

    def cleanup_empty_dirs(self) -> List[str]:
        """
        逐个任务的监控目录清理空子目录；每个监控根目录本身永远保留
        """
        cfg = get_settings()
        removed: List[str] = []
        for task in self._enabled_tasks(cfg):
            root = Path(str(task.get("watch_dir") or "")).expanduser()
            if not root.exists() or not root.is_dir():
                continue
            task_name = str(task.get("name") or "")
            for current, _dirs, _files in os.walk(root, topdown=False):
                cur = Path(current)
                if cur == root:
                    continue  # 监控目录本身永远保留
                try:
                    if any(cur.iterdir()):
                        continue
                    cur.rmdir()
                    removed.append(f"{task_name}/{cur.relative_to(root).as_posix()}".lstrip("/"))
                except OSError:
                    continue
        return removed

    # ---------------------------------------------------------------- 状态

    def _persist(self) -> None:
        with self._lock:
            # 已处理的下载任务只保留最近 5000 条，避免 state.json 无限膨胀
            done = self.state.get("tr_done")
            if isinstance(done, dict) and len(done) > 5000:
                keep = sorted(done.items(), key=lambda kv: kv[1], reverse=True)[:4000]
                self.state["tr_done"] = dict(keep)
            missing_tr = self.tr_state.get("missing")
            if isinstance(missing_tr, dict) and len(missing_tr) > 200:
                self.tr_state["missing"] = dict(list(missing_tr.items())[-100:])
            # 残留种子列表同理，只留最近 200 条
            stale_tr = self.tr_state.get("stale")
            if isinstance(stale_tr, dict) and len(stale_tr) > 200:
                self.tr_state["stale"] = dict(list(stale_tr.items())[-100:])
            files = self.state.get("files") or {}
            if len(files) > 2000:
                keep = sorted(
                    files.items(),
                    key=lambda kv: float((kv[1] or {}).get("last_try") or 0),
                    reverse=True,
                )[:2000]
                self.state["files"] = dict(keep)
            save_state(self.state)

    def reset_records(self) -> None:
        self.state["files"] = {}
        self.state["last_results"] = []
        self.last_results = []
        self._persist()

    def files_view(self) -> List[Dict[str, Any]]:
        rows: List[Dict[str, Any]] = []
        cfg = get_settings()
        task_names = {
            str(t.get("id") or "default"): (t.get("name") or "")
            for t in (cfg.get("tasks") or [])
            if isinstance(t, dict)
        }
        for key, value in (self.state.get("files") or {}).items():
            value = value or {}
            last_try = float(value.get("last_try") or 0)
            done_at = float(value.get("done_at") or 0)
            next_text = ""
            if value.get("status") == "held":
                retry_min = int(cfg.get("retry_check_min") or 0)
                if retry_min > 0:
                    held_at = float(value.get("held_at") or value.get("last_try") or 0)
                    due = held_at + retry_min * 60
                    next_text = (
                        time.strftime("%m-%d %H:%M", time.localtime(due))
                        if due > time.time()
                        else "即将重试"
                    )
                else:
                    next_text = "每天 " + str(cfg.get("daily_scan_time") or "09:00")
            task_id, _, rel = key.partition("|")
            rows.append(
                {
                    "key": rel or key,
                    "task": task_id,
                    "task_name": task_names.get(task_id) or task_id,
                    "source": value.get("source") or "",
                    "size": fmt_size(int(value.get("size") or 0)),
                    "status": value.get("status") or "",
                    "tries": value.get("tries") or 0,
                    "note": value.get("note") or "",
                    "last_try": time.strftime("%m-%d %H:%M", time.localtime(last_try))
                    if last_try
                    else "",
                    "done_at": time.strftime("%Y-%m-%d %H:%M", time.localtime(done_at))
                    if done_at
                    else "",
                    "sort_ts": done_at or last_try
                    or float(value.get("first_seen") or 0),
                    "next_try": next_text,
                }
            )
        rows.sort(key=lambda r: r.get("sort_ts") or 0, reverse=True)
        for row in rows:
            row.pop("sort_ts", None)
        return rows

    def files_page(self, page: int = 1, size: int = 30) -> Dict[str, Any]:
        """
        文件记录分页（新的在前）
        """
        rows = self.files_view()
        total = len(rows)
        size = max(1, min(200, int(size or 30)))
        pages = max(1, (total + size - 1) // size)
        page = max(1, min(pages, int(page or 1)))
        start = (page - 1) * size
        return {
            "rows": rows[start : start + size],
            "total": total,
            "page": page,
            "size": size,
            "pages": pages,
        }
