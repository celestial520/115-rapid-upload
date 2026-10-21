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

import requests

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
        # 下载器监控：已完成任务推过来的文件（task_id -> {路径: Path}），等主循环取走
        self._forced: Dict[str, Dict[str, Path]] = {}
        # 每个待处理路径是被谁触发的：tr / mp
        self._forced_src: Dict[str, str] = {}
        self._poll_stop: Optional[threading.Event] = None
        self._poll_thread: Optional[threading.Thread] = None
        self._tr_session = ""
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
                    if cfg.get("watch_events_enabled", True):
                        self._flush_events(cfg)
                    if self._forced:
                        groups: Dict[str, Dict[str, List[Path]]] = {}
                        for task_id, paths in self._forced.items():
                            for path in paths.values():
                                src = self._forced_src.get(path.as_posix(), "tr")
                                groups.setdefault(src, {}).setdefault(task_id, []).append(path)
                        self._forced = {}
                        self._forced_src = {}
                        for src, buckets in groups.items():
                            label = "下载器完成" if src == "tr" else "MP 整理完成"
                            detail = "、".join(f"{k}:{len(v)}" for k, v in buckets.items())
                            print(f"[worker] {label}，立即处理 {detail}")
                            self.run_once(skip_stable=True, event_files=buckets, source=src)
                    self._maybe_daily_scan(cfg)
                    self._maybe_cleanup(cfg)
            except Exception as err:  # 任何异常都不能让线程退出
                self.last_error = str(err)
                print(f"[worker] 本轮出错：{err}")
            self._stop.wait(2)

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
        print(f"[worker] 每日定时扫描开始（{cfg.get('daily_scan_time')}）")
        results = self.run_once(source="daily")
        print(f"[worker] 每日定时扫描结束，处理 {len(results)} 个文件")

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
                        item = self._process(
                            path, watch, cid, cfg, skip_stable, task, source
                        )
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
                record["note"] = "网盘已有同名文件"
                deleted = self._finish_local(path, cfg, task)
                return {
                    "file": rel_posix,
                    "task": task.get("name") or "",
                    "source": source,
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
            record["note"] = "秒传成功"
            deleted = self._finish_local(path, cfg, task)
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "source": source,
                "status": "成功",
                "size": fmt_size(size),
                "note": "已复制到网盘，已删除源文件" if deleted else "已复制到网盘，保留源文件",
            }

        if detail.startswith("115 上没有相同内容"):
            scan_time = str(cfg.get("daily_scan_time") or "09:00")
            record["status"] = "held"
            record["note"] = detail
            return {
                "file": rel_posix,
                "task": task.get("name") or "",
                "source": source,
                "status": "拦截",
                "size": fmt_size(size),
                "note": f"暂不能秒传，明天 {scan_time} 扫描时再试",
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
                    print(f"[worker] 查询下载器出错：{err}")
                interval = max(5, int(get_settings().get("tr_poll_sec") or 30))
                self._poll_stop.wait(interval)

        self._poll_thread = threading.Thread(target=_loop, name="tr-poller", daemon=True)
        self._poll_thread.start()
        print(f"[worker] 已启动下载器监控：{cfg.get('tr_url')}")

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

        for torrent in torrents:
            torrent_hash = str(torrent.get("hashString") or "")
            if not torrent_hash or torrent_hash in done_map:
                continue
            finished = (
                bool(torrent.get("isFinished"))
                or float(torrent.get("percentDone") or 0) >= 1
                or int(torrent.get("status") or 0) == 6
            )
            if not finished:
                continue
            if int(torrent.get("error") or 0) != 0:
                continue
            path = self._resolve_download_path(torrent, cfg)
            name = str(torrent.get("name") or "")
            if not path or not path.exists():
                if torrent_hash not in missing:
                    missing[torrent_hash] = {
                        "name": name,
                        "path": str(path or ""),
                        "reason": "本地找不到，检查下载器的路径映射",
                        "at": time.strftime("%m-%d %H:%M"),
                    }
                    print(
                        f"[worker] 下载已完成但本地找不到：{name} -> {path}（检查路径映射）"
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
                    print(f"[worker] 下载已完成但目录里没有可处理的文件：{path}")
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
                    print(f"[worker] 下载完成，但不在任何监控目录里：{path}")
                continue
            for task_id, items in buckets.items():
                self._forced.setdefault(task_id, {})
                for item in items:
                    self._forced[task_id][item.as_posix()] = item
                    self._forced_src[item.as_posix()] = "tr"
            missing.pop(torrent_hash, None)
            done_map[torrent_hash] = time.time()
            self.tr_state["completed"] = int(self.tr_state.get("completed") or 0) + 1
            self._persist()
            print(f"[worker] 下载完成，准备秒传：{name}（{len(files)} 个文件）")

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
            if value.get("status") == "held":
                next_text = "每天 " + str(cfg.get("daily_scan_time") or "09:00")
            task_id, _, rel = key.partition("|")
            rows.append(
                {
                    "key": rel or key,
                    "task": task_id,
                    "source": value.get("source") or "",
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
