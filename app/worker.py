"""
扫描与秒传调度
"""
import hashlib
import os
import threading
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from .config import get_settings, load_state, save_state
from .notify import notify_batch
from .u115 import U115Client, U115Error

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
        self._thread: Optional[threading.Thread] = None
        self._stop = threading.Event()
        self.running_scan = False
        self.last_run_at: float = float(self.state.get("last_run") or 0)
        self.last_results: List[Dict[str, Any]] = list(self.state.get("last_results") or [])
        self.last_error: str = ""

    # ---------------------------------------------------------------- 线程

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._loop, name="scanner", daemon=True)
        self._thread.start()
        self._start_watcher()

    def stop(self) -> None:
        self._stop.set()
        self._stop_watcher()

    def _loop(self) -> None:
        """
        事件模式：每 2 秒处理一次"已写稳"的文件事件，另外每 scan_interval_sec 做一次兜底全量扫描
        轮询模式：完全按 scan_interval_sec 全量扫描
        """
        last_full_scan = 0.0
        while not self._stop.is_set():
            cfg = get_settings()
            mode = str(cfg.get("watch_mode") or "event")
            event_mode = mode == "event" and self._watcher_alive()
            try:
                if cfg.get("enabled"):
                    full_interval = max(30, int(cfg.get("scan_interval_sec") or 900))
                    if not event_mode or time.time() - last_full_scan >= full_interval:
                        self.run_once()
                        last_full_scan = time.time()
                    elif event_mode:
                        self._flush_events(cfg)
                    self._maybe_cleanup(cfg)
            except Exception as err:  # 任何异常都不能让线程退出
                self.last_error = str(err)
                print(f"[worker] 本轮出错：{err}")
            self._stop.wait(2 if event_mode else 10)

    # ---------------------------------------------------------------- 文件系统事件

    def _watcher_alive(self) -> bool:
        return bool(self._watch_thread and self._watch_thread.is_alive())

    def _start_watcher(self) -> None:
        """
        用 watchfiles(inotify) 监听各任务的监控目录。事件只做记录，真正的处理交给 _flush_events
        """
        cfg = get_settings()
        if str(cfg.get("watch_mode") or "event") != "event":
            return
        if self._watcher_alive():
            return
        try:
            import watchfiles  # noqa: F401
        except Exception as err:
            self.event_state["error"] = f"watchfiles 不可用，已退化为定时扫描：{err}"
            print(f"[worker] {self.event_state['error']}")
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
                print(f"[worker] {self.event_state['error']}")

        self._watch_thread = threading.Thread(target=_loop, name="watcher", daemon=True)
        self._watch_thread.start()
        self.event_state["dirs"] = dirs
        print(f"[worker] 已启动文件系统监听：{dirs}")

    def refresh_watcher(self) -> None:
        """
        设置变更后重建监听（监控目录/模式可能变了）
        """
        self._stop_watcher()
        if str(get_settings().get("watch_mode") or "event") != "event":
            return
        self._start_watcher()

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
            self.run_once(force=True, event_files=buckets)

    # ---------------------------------------------------------------- 主流程

    def run_once(
        self,
        force: bool = False,
        event_files: Optional[Dict[str, List[Path]]] = None,
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
                for path in candidates:
                    if self._stop.is_set():
                        break
                    try:
                        item = self._process(path, watch, cid, cfg, force, task)
                    except Exception as err:
                        item = {
                            "file": path.name,
                            "task": task.get("name") or "",
                            "status": "失败",
                            "size": fmt_size(path.stat().st_size if path.exists() else 0),
                            "note": str(err),
                        }
                        self.last_error = str(err)
                    if item:
                        results.append(item)
                    self._persist()

            self.last_run_at = time.time()
            self.last_results = results[-50:]
            self.state["last_run"] = self.last_run_at
            self.state["last_results"] = self.last_results
            self._persist()

            if results:
                for err in notify_batch(results):
                    self.last_error = err
                    print(f"[worker] 通知失败：{err}")
        finally:
            self.running_scan = False
        return results

    def _process(
        self,
        path: Path,
        watch: Path,
        cid: str,
        cfg: Dict[str, Any],
        force: bool,
        task: Optional[Dict[str, Any]] = None,
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

        # 文件可能还在写入
        stable = max(0, int(cfg.get("stable_seconds") or 0))
        if not force and stable and time.time() - mtime < stable:
            return None

        # 不能秒传的，等到「第二天 retry_time」再试
        last_try = float(record.get("last_try") or 0)
        if not force and last_try:
            next_at = self._next_retry_ts(last_try, str(cfg.get("retry_time") or "09:00"))
            if next_at and time.time() < next_at:
                return None

        # 网盘已有同名文件
        try:
            if self.client.find_file(target_cid, path.name):
                record["status"] = "done"
                record["note"] = "网盘已有同名文件"
                deleted = self._finish_local(path, cfg)
                return {
                    "file": rel_posix,
                    "task": task.get("name") or "",
                    "status": "成功",
                    "size": fmt_size(size),
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
            record["note"] = "秒传成功"
            deleted = self._finish_local(path, cfg)
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "status": "成功",
                "size": fmt_size(size),
                "note": "已复制到网盘，已删除源文件" if deleted else "已复制到网盘，保留源文件",
            }

        if detail.startswith("115 上没有相同内容"):
            next_at = self._next_retry_ts(record["last_try"], str(cfg.get("retry_time") or "09:00"))
            next_text = (
                time.strftime("%m-%d %H:%M", time.localtime(next_at)) if next_at else "明天"
            )
            record["status"] = "held"
            record["note"] = detail
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "status": "拦截",
                "size": fmt_size(size),
                "note": f"暂不能秒传，{next_text} 再试",
            }

        record["note"] = detail
        return {
            "file": rel_posix,
            "task": task.get("name") or "",
            "status": "失败",
            "size": fmt_size(size),
            "note": detail,
        }

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

    def _finish_local(self, path: Path, cfg: Dict[str, Any]) -> bool:
        """
        秒传永远是"复制"到网盘（源文件本来就在），这里只处理本地源文件要不要删
        """
        if not cfg.get("delete_source", True):
            return False
        try:
            path.unlink()
            return True
        except Exception as err:
            print(f"[worker] 删除源文件失败：{path} - {err}")
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
            size = path.stat().st_size
            min_size = max(0, int(cfg.get("min_size_mb") or 0)) * 1024 * 1024
            if size <= 0 or size < min_size:
                return False
            return True
        except Exception:
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

    @staticmethod
    def _next_retry_ts(last_try: float, retry_time: str) -> Optional[float]:
        parsed = parse_hhmm(retry_time)
        if not parsed:
            return None
        hour, minute = parsed
        base = datetime.fromtimestamp(float(last_try or time.time()))
        target = (base + timedelta(days=1)).replace(
            hour=hour, minute=minute, second=0, microsecond=0
        )
        return target.timestamp()

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
            print(f"[worker] 清理空目录 {len(removed)} 个")

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
        for key, value in (self.state.get("files") or {}).items():
            value = value or {}
            last_try = float(value.get("last_try") or 0)
            next_text = ""
            if value.get("status") == "held" and last_try:
                next_at = self._next_retry_ts(last_try, str(cfg.get("retry_time") or "09:00"))
                if next_at:
                    next_text = time.strftime("%m-%d %H:%M", time.localtime(next_at))
            task_id, _, rel = key.partition("|")
            rows.append(
                {
                    "key": rel or key,
                    "task": task_id,
                    "size": fmt_size(int(value.get("size") or 0)),
                    "status": value.get("status") or "",
                    "tries": value.get("tries") or 0,
                    "note": value.get("note") or "",
                    "last_try": time.strftime("%m-%d %H:%M", time.localtime(last_try))
                    if last_try
                    else "",
                    "next_try": next_text,
                }
            )
        rows.sort(key=lambda r: r.get("last_try") or "", reverse=True)
        return rows
