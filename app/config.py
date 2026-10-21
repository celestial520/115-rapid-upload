"""
配置管理：JSON 落盘，带默认值与原子写入
"""
import json
import os
import threading
from pathlib import Path
from typing import Any, Dict

DATA_DIR = Path(os.environ.get("DATA_DIR", "/config"))
SETTINGS_FILE = DATA_DIR / "settings.json"
STATE_FILE = DATA_DIR / "state.json"
TOKEN_FILE = DATA_DIR / "115_token.json"
LOG_DIR = DATA_DIR / "logs"

DEFAULTS: Dict[str, Any] = {
    # ---- 基础 ----
    "enabled": True,
    # 多套「本地目录 -> 115 目录」映射
    # [{"id": "t1", "name": "电影", "watch_dir": "/watch/movies",
    #   "target_dir": "/电影", "enabled": True}]
    "tasks": [],
    "delete_source": True,            # 秒传成功后删除本地源文件（独立开关）
    # 不再按扩展名过滤：监控目录里所有文件都会上传（隐藏文件和下载临时文件除外）
    "min_size_mb": 0,
    "recursive": True,
    "stable_seconds": 30,             # 最后一次变化后静置多久才处理（事件触发时=响应延迟）
    # 文件系统事件（inotify）：监控目录一有变化就处理，几乎不扫盘
    "watch_events_enabled": True,
    # 每天固定时间全量扫一次（处理"从别处移进来"的文件，也顺手重试之前失败的文件）
    "daily_scan_enabled": True,
    "daily_scan_time": "09:00",
    # ---- 空目录清理 ----
    "cleanup_enabled": True,
    "cleanup_time": "03:30",
    # ---- 外部触发（给 MoviePilot 插件用）----
    "api_key": "",                    # 首次启动自动生成
    # ---- 115 开放平台 ----
    "u115_client_id": "100197847",
    # ---- Transmission 下载器监控 ----
    "tr_enabled": False,
    "tr_url": "",                     # 例如 http://192.168.1.10:9091
    "tr_username": "",
    "tr_password": "",
    "tr_poll_sec": 30,                # 查询下载器状态的间隔（只是 RPC，不碰磁盘）
    # 路径映射：下载器里的路径 -> 本容器里能看到的路径
    # [{"from": "/downloads", "to": "/watch"}]
    "tr_path_map": [],
    # ---- 企业微信通知 ----
    "wecom_enabled": False,
    "wecom_corpid": "",
    "wecom_secret": "",
    "wecom_agentid": "",
    "wecom_proxy": "",                # 转发代理，如 http://proxy.example.com:8980
    "wecom_touser": "@all",
    "wecom_markdown": True,
    # ---- CMS 通知 ----
    "cms_enabled": False,
    "cms_domain": "",
    "cms_token": "cloud_media_sync",
    "cms_type": "lift_sync",
    # ---- 通知内容 ----
    "notify_on_hold": True,
}

_lock = threading.Lock()
_settings: Dict[str, Any] = {}


def _atomic_write(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def load_settings() -> Dict[str, Any]:
    global _settings
    with _lock:
        merged = dict(DEFAULTS)
        if SETTINGS_FILE.exists():
            try:
                raw = json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
                if isinstance(raw, dict):
                    for key, value in raw.items():
                        if key in merged:
                            merged[key] = value
                    # 兼容旧配置：以前用 transfer_mode("move"/"copy") 表示要不要删源文件
                    if "delete_source" not in raw and "transfer_mode" in raw:
                        merged["delete_source"] = str(raw.get("transfer_mode")) != "copy"
                    # 兼容旧配置：单个 watch_dir/target_dir -> tasks
                    if not merged.get("tasks") and raw.get("watch_dir"):
                        merged["tasks"] = [
                            {
                                "id": "default",
                                "name": "默认",
                                "watch_dir": raw.get("watch_dir"),
                                "target_dir": raw.get("target_dir") or "/",
                                "enabled": True,
                            }
                        ]
            except Exception as err:  # 配置坏了就用默认值，别让服务起不来
                print(f"[config] 读取配置失败，使用默认值：{err}")
        _settings = merged
        return merged


def ensure_api_key() -> str:
    """
    保证有一个外部触发密钥；没有就生成并落盘
    """
    import secrets

    settings = get_settings()
    key = str(settings.get("api_key") or "").strip()
    if key:
        return key
    key = secrets.token_hex(24)
    save_settings({"api_key": key})
    return key


def get_settings() -> Dict[str, Any]:
    if not _settings:
        return load_settings()
    return _settings


def save_settings(new_values: Dict[str, Any]) -> Dict[str, Any]:
    global _settings
    with _lock:
        merged = dict(_settings or DEFAULTS)
        for key, value in (new_values or {}).items():
            if key in DEFAULTS:
                merged[key] = value
        _settings = merged
        _atomic_write(SETTINGS_FILE, merged)
        return merged


def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            raw = json.loads(STATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
        except Exception as err:
            print(f"[config] 读取状态失败：{err}")
    return {}


def save_state(state: Dict[str, Any]) -> None:
    with _lock:
        _atomic_write(STATE_FILE, state)


def load_token() -> Dict[str, Any]:
    if TOKEN_FILE.exists():
        try:
            raw = json.loads(TOKEN_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
        except Exception:
            pass
    return {}


def save_token(token: Dict[str, Any]) -> None:
    with _lock:
        _atomic_write(TOKEN_FILE, token)
