"""
115秒传助手 · Web 控制台
"""
import base64
import io
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

import qrcode
import qrcode.image.svg
from fastapi import Body, FastAPI, HTTPException, Query, Request, Response
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from .auth import COOKIE_NAME, SESSION_TTL, Auth
from .config import DEFAULTS, get_settings, load_settings, save_settings
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

PUBLIC_PATHS = {"/login", "/api/login", "/favicon.ico", "/api/health"}


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


@app.post("/api/login")
def api_login(payload: Dict[str, Any] = Body(...)) -> Response:
    username = str(payload.get("username") or "").strip()
    password = str(payload.get("password") or "")
    if not auth.verify(username, password):
        raise HTTPException(status_code=401, detail="账号或密码不正确")
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
def api_logout() -> Response:
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
        "retry_time": cfg.get("retry_time"),
        "cleanup_enabled": cfg.get("cleanup_enabled"),
        "cleanup_time": cfg.get("cleanup_time"),
        "scan_interval_sec": cfg.get("scan_interval_sec"),
        "watch_mode": cfg.get("watch_mode"),
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
    return {"ok": True, "settings": api_settings()["settings"]}


@app.post("/api/run")
def api_run(force: bool = Query(True)) -> Dict[str, Any]:
    if not client.is_authorized():
        raise HTTPException(status_code=400, detail="115 未授权，请先扫码登录")
    results = worker.run_once(force=bool(force))
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
        return {"ok": True, **client.qr_status()}
    except U115Error as err:
        raise HTTPException(status_code=400, detail=str(err))


@app.post("/api/115/logout")
def api_logout() -> Dict[str, Any]:
    client.logout()
    return {"ok": True}


# ------------------------------------------------------------------ 目录选择


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
