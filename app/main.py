"""
115秒传助手 · Web 控制台
"""
import io
import time
from pathlib import Path
from typing import Any, Dict, List

import qrcode
import qrcode.image.svg
from fastapi import Body, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

from .logger import LOG_FILE, get_logger, setup_logging, tail as log_tail

setup_logging()
logger = get_logger()

from .auth import COOKIE_NAME, SESSION_TTL, Auth
from .config import DATA_DIR, DEFAULTS, get_settings, load_settings, save_settings
from .u115 import U115Client, U115Error
from .worker import Worker

BASE_DIR = Path(__file__).resolve().parent
TEMPLATE = BASE_DIR / "templates" / "index.html"
LOGIN_TEMPLATE = BASE_DIR / "templates" / "login.html"

load_settings()
auth = Auth()
client = U115Client()
worker = Worker(client)

app = FastAPI(title="115秒传助手", docs_url=None, redoc_url=None)

PUBLIC_PATHS = {
    "/login",
    "/api/login",
    "/favicon.ico",
    "/api/health",
}


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    """
    除了登录页和登录接口，其它都要先登录
    """
    path = request.url.path
    if path in PUBLIC_PATHS or path.startswith("/api/login"):
        return await call_next(request)
    username = auth.check_token(request.cookies.get(COOKIE_NAME))
    if not username:
        if path.startswith("/api/"):
            return JSONResponse({"detail": "未登录"}, status_code=401)
        return RedirectResponse("/login", status_code=302)
    request.state.username = username
    return await call_next(request)


@app.get("/login", response_class=HTMLResponse)
def login_page(request: Request) -> Response:
    if auth.check_token(request.cookies.get(COOKIE_NAME)):
        return RedirectResponse("/", status_code=302)
    return HTMLResponse(LOGIN_TEMPLATE.read_text(encoding="utf-8"))


_login_fail: Dict[str, Dict[str, float]] = {}
_LOGIN_MAX_DELAY = 3.0


def _login_throttle(client_key: str, failed: bool) -> None:
    """
    登录失败次数越多，下一次越慢，挡一下暴力破解
    """
    import time as _time

    record = _login_fail.setdefault(client_key, {"count": 0, "last": 0.0})
    if not failed:
        record["count"] = 0
        return
    record["count"] = min(record["count"] + 1, 20)
    record["last"] = _time.time()
    if record["count"] >= 5:
        _time.sleep(min(_LOGIN_MAX_DELAY, 0.4 * (record["count"] - 4)))


@app.post("/api/login")
def api_login(payload: Dict[str, Any] = Body(...), request: Request = None) -> Response:
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    client_key = (request.client.host if request and request.client else "unknown")
    if not auth.verify(username, password):
        logger.warning(f"网页登录失败：{username or '(空)'}（来自 {client_key}）")
        _login_throttle(client_key, True)
        raise HTTPException(status_code=401, detail="账号或密码不正确")
    _login_throttle(client_key, False)
    logger.info(f"网页登录成功：{username}（来自 {client_key}）")
    token = auth.issue_token(username)
    response = JSONResponse({"ok": True, "username": username})
    response.set_cookie(
        COOKIE_NAME,
        token,
        max_age=SESSION_TTL,
        httponly=True,
        samesite="lax",
        path="/",
    )
    return response


@app.post("/api/logout")
def api_web_logout() -> Response:
    response = JSONResponse({"ok": True})
    response.delete_cookie(COOKIE_NAME, path="/")
    return response


@app.get("/api/account")
def api_account(request: Request) -> Dict[str, Any]:
    return {"username": getattr(request.state, "username", auth.username())}


@app.post("/api/password")
def api_password(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    old = str(payload.get("old_password") or "")
    new = str(payload.get("new_password") or "")
    confirm = str(payload.get("confirm_password") or new)
    if new != confirm:
        raise HTTPException(status_code=400, detail="两次输入的新密码不一致")
    error = auth.change_password(old, new)
    if error:
        raise HTTPException(status_code=400, detail=error)
    # 密码变了，重新签发一个会话，避免自己被踢下线
    token = auth.issue_token(auth.username())
    response = JSONResponse({"ok": True})
    response.set_cookie(
        COOKIE_NAME, token, max_age=SESSION_TTL, httponly=True, samesite="lax", path="/"
    )
    return response


@app.post("/api/tr/test")
def api_tr_test() -> Dict[str, Any]:
    """测试 Transmission 连接（会顺带把当前已完成的任务推一次）"""
    data = worker._tr_rpc("session-get", {"fields": ["version"]})
    if not data:
        raise HTTPException(
            status_code=400, detail=worker.tr_state.get("error") or "连接失败"
        )
    arguments = data.get("arguments") or {}
    return {"ok": True, "version": arguments.get("version") or "未知"}


@app.post("/api/wecom/test")
def api_wecom_test() -> Dict[str, Any]:
    """发一条测试消息，方便确认 corpid/secret/agentid/代理 是否配对"""
    from .notify import send_wecom

    error = send_wecom(
        "115秒传助手 · 测试消息",
        "这是一条测试通知，收到说明企业微信配置正确。",
        require_enabled=False,
    )
    if error:
        raise HTTPException(status_code=400, detail=error)
    return {"ok": True}


@app.get("/api/health")
def api_health() -> Dict[str, Any]:
    return {"ok": True}


@app.on_event("startup")
def on_startup() -> None:
    cfg = get_settings()
    tasks = cfg.get("tasks") or []
    logger.info(
        "115秒传助手启动：数据目录=%s，任务 %d 个，日志=%s"
        % (DATA_DIR, len(tasks), LOG_FILE)
    )
    if not tasks:
        logger.warning("还没有配置监控任务，请到网页「设置 → 监控任务」添加")
    if not client.is_authorized():
        logger.warning("115 尚未授权，请到网页扫码登录")
    worker.start()


@app.on_event("shutdown")
def on_shutdown() -> None:
    worker.stop()


@app.get("/", response_class=HTMLResponse)
def index() -> str:
    return TEMPLATE.read_text(encoding="utf-8")


# ------------------------------------------------------------------ 状态与设置


@app.get("/api/status")
def api_status() -> Dict[str, Any]:
    cfg = get_settings()
    token = client.token_info()
    return {
        "version": "1.0",
        "enabled": bool(cfg.get("enabled")),
        "tasks": cfg.get("tasks") or [],
        "task_count": len([t for t in (cfg.get("tasks") or []) if t.get("enabled", True)]),
        "delete_source": bool(cfg.get("delete_source", True)),
        "daily_scan_enabled": bool(cfg.get("daily_scan_enabled", True)),
        "daily_scan_time": cfg.get("daily_scan_time"),
        "last_daily_scan": worker.state.get("last_daily_scan") or "",
        "cleanup_enabled": cfg.get("cleanup_enabled"),
        "cleanup_time": cfg.get("cleanup_time"),
        "watch_events_enabled": bool(cfg.get("watch_events_enabled", True)),
        "watch_event_alive": worker._watcher_alive(),
        "watch_event_error": worker.event_state.get("error") or "",
        "watch_event_count": worker.event_state.get("count") or 0,
        "watch_event_last": time.strftime(
            "%m-%d %H:%M:%S",
            time.localtime(float(worker.event_state.get("last_event_at") or 0)),
        )
        if worker.event_state.get("last_event_at")
        else "",
        "pending_count": len(worker._pending),
        "tr_enabled": bool(cfg.get("tr_enabled")),
        "tr_url": cfg.get("tr_url"),
        "tr_connected": bool(worker.tr_state.get("connected")),
        "tr_error": worker.tr_state.get("error") or "",
        "tr_completed": worker.tr_state.get("completed") or 0,
        "tr_unmatched": [
            v
            for v in list((worker.tr_state.get("missing") or {}).values())[:10]
            if isinstance(v, dict)
        ],
        "tr_last_poll": time.strftime(
            "%m-%d %H:%M:%S", time.localtime(float(worker.tr_state.get("last_poll") or 0))
        )
        if worker.tr_state.get("last_poll")
        else "",
        "u115_authorized": client.is_authorized(),
        "u115_authorized_at": time.strftime(
            "%Y-%m-%d %H:%M", time.localtime(int(token.get("refresh_time") or 0))
        )
        if token.get("refresh_time")
        else "",
        "running_scan": worker.running_scan,
        "last_run_at": time.strftime("%m-%d %H:%M:%S", time.localtime(worker.last_run_at))
        if worker.last_run_at
        else "",
        "last_error": worker.last_error,
        "last_results": worker.last_results,
        "files": worker.files_view(),
        "last_cleanup_count": worker.state.get("last_cleanup_count") or 0,
    }


@app.get("/api/settings")
def api_settings() -> Dict[str, Any]:
    cfg = dict(get_settings())
    for secret_key in ("wecom_secret", "tr_password"):
        if cfg.get(secret_key):
            cfg[secret_key] = "******"
    return {"settings": cfg, "defaults": DEFAULTS}


@app.post("/api/settings")
def api_save_settings(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:
    current = get_settings()
    # 前端回传 ****** 表示没改密钥
    if payload.get("wecom_secret") == "******":
        payload["wecom_secret"] = current.get("wecom_secret", "")
    if payload.get("tr_password") == "******":
        payload["tr_password"] = current.get("tr_password", "")
    save_settings(payload)
    worker.last_error = ""
    worker.refresh_watcher()   # 监控目录/模式可能变了
    cfg = get_settings()
    tasks = cfg.get("tasks") or []
    logger.info(
        "设置已保存：任务 %d 个（启用 %d），文件事件=%s，每日扫描=%s %s"
        % (
            len(tasks),
            len([t for t in tasks if t.get("enabled", True)]),
            "开" if cfg.get("watch_events_enabled", True) else "关",
            "开" if cfg.get("daily_scan_enabled", True) else "关",
            cfg.get("daily_scan_time") or "",
        )
    )
    return {"ok": True, "settings": api_settings()["settings"]}


@app.post("/api/run")
def api_run(force: bool = Query(True)) -> Dict[str, Any]:
    if worker.running_scan:
        raise HTTPException(status_code=409, detail="上一轮还在处理中，稍等几秒再试")
    if not client.is_authorized():
        raise HTTPException(status_code=400, detail="115 未授权，请先扫码登录")
    logger.info(f"手动触发扫描（跳过静置={bool(force)}）")
    results = worker.run_once(skip_stable=not bool(force), source="manual")
    return {"ok": True, "count": len(results), "results": results}


@app.post("/api/reset")
def api_reset() -> Dict[str, Any]:
    worker.reset_records()
    return {"ok": True}


# ------------------------------------------------------------------ 115 扫码


@app.get("/api/115/qrcode")
def api_qrcode() -> Dict[str, Any]:
    try:
        data = client.qr_create()
    except U115Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    content = str(data.get("qrcode") or "")
    svg = ""
    if content:
        image = qrcode.make(content, image_factory=qrcode.image.svg.SvgPathImage)
        buffer = io.BytesIO()
        image.save(buffer)
        svg = buffer.getvalue().decode("utf-8")
    return {"ok": True, "qrcode": svg, "content": content}


@app.get("/api/115/qrcode/status")
def api_qrcode_status() -> Dict[str, Any]:
    try:
        result = client.qr_status()
        if result.get("status") == 2:
            logger.info("115 扫码授权成功")
        return {"ok": True, **result}
    except U115Error as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/api/115/logout")
def api_u115_logout() -> Dict[str, Any]:
    logger.info("已退出 115 授权，需要重新扫码")
    client.logout()
    return {"ok": True}


# ------------------------------------------------------------------ 目录选择


# 容器内不需要作为监控目录出现的路径
_SYSTEM_PREFIXES = (
    "/proc",
    "/sys",
    "/dev",
    "/run",
    "/etc/hosts",
    "/etc/hostname",
    "/etc/resolv.conf",
    "/etc/localtime",
    "/etc/timezone",
    "/config",          # 本工具自己的数据目录，compose 里固定挂在 /config
)
# 这些是内核/容器内部用的文件系统，不是数据盘
_SKIP_FS = {
    "proc", "sysfs", "devpts", "mqueue", "cgroup", "cgroup2", "devtmpfs",
    "shm", "securityfs", "debugfs", "tracefs", "pstore", "bpf", "autofs",
    "ramfs", "binfmt_misc", "nsfs", "tmpfs", "overlay",
}


def _read_mounts(proc_path: str = "/proc/self/mountinfo") -> List[Dict[str, str]]:
    """
    从 /proc/self/mountinfo 里读出容器内的挂载点（也就是 compose 里 volumes 挂进来的目录）
    """
    data_dir = str(DATA_DIR).rstrip("/")
    seen = set()
    result: List[Dict[str, str]] = []
    try:
        with open(proc_path, "r", encoding="utf-8", errors="ignore") as handle:
            lines = handle.readlines()
    except Exception:
        return result
    for line in lines:
        try:
            left, right = line.split(" - ", 1)
            fields = left.split()
            mount_point = fields[4].replace("\\040", " ")
            right_fields = right.split()
            fstype = right_fields[0] if right_fields else ""
            source = right_fields[1] if len(right_fields) > 1 else ""
        except Exception:
            continue
        if mount_point == "/" or mount_point in seen:
            continue
        if any(
            mount_point == prefix or mount_point.startswith(prefix + "/")
            for prefix in _SYSTEM_PREFIXES
        ):
            continue
        if data_dir and (
            mount_point == data_dir or mount_point.startswith(data_dir + "/")
        ):
            continue
        if fstype in _SKIP_FS:
            continue
        seen.add(mount_point)
        result.append({"path": mount_point, "fstype": fstype, "source": source})
    result.sort(key=lambda x: x["path"])
    return result


@app.get("/api/logs")
def api_logs(
    lines: int = Query(200, ge=20, le=2000),
    keyword: str = Query("", description="按关键字过滤，例如 ERROR"),
) -> Dict[str, Any]:
    size = LOG_FILE.stat().st_size if LOG_FILE.exists() else 0
    return {
        "content": log_tail(lines, keyword),
        "file": str(LOG_FILE),
        "size": size,
    }


@app.get("/api/logs/download")
def api_logs_download():
    if not LOG_FILE.exists():
        raise HTTPException(status_code=404, detail="日志文件还没有生成")
    return FileResponse(
        LOG_FILE, filename="115-rapid-assistant.log", media_type="text/plain"
    )


@app.get("/api/mounts")
def api_mounts() -> Dict[str, Any]:
    """
    返回容器内的挂载点，前端拿来做"挂载目录"快捷选择
    """
    return {"mounts": _read_mounts()}


@app.get("/api/fs")
def api_fs(path: str = Query("/")) -> Dict[str, Any]:
    """
    浏览容器内目录，给"本地监控目录"做选择器
    """
    base = Path(path or "/")
    if not base.exists() or not base.is_dir():
        raise HTTPException(status_code=400, detail=f"目录不存在：{path}")
    dirs: List[Dict[str, str]] = []
    try:
        for child in sorted(base.iterdir()):
            try:
                if child.is_dir() and not child.name.startswith("."):
                    dirs.append({"name": child.name, "path": child.as_posix()})
            except OSError:
                continue
    except PermissionError:
        raise HTTPException(status_code=403, detail="没有权限读取该目录")
    return {
        "path": base.as_posix(),
        "parent": base.parent.as_posix() if base.as_posix() != "/" else "",
        "dirs": dirs,
    }


@app.get("/api/115/dirs")
def api_115_dirs(path: str = Query("/")) -> Dict[str, Any]:
    """
    列出 115 网盘某个目录下的一级文件夹，给"115 目标目录"做选择器
    """
    if not client.is_authorized():
        raise HTTPException(status_code=400, detail="115 未授权")
    try:
        cid = client.ensure_dir(path)
        items = client.list_dir(cid)
    except U115Error as err:
        raise HTTPException(status_code=400, detail=str(err))
    dirs = [
        {"name": item.get("fn"), "path": (path.rstrip("/") + "/" + str(item.get("fn")))}
        for item in items
        if item.get("fc") == "0"
    ]
    return {"path": path, "dirs": dirs}
