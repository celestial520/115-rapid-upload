"""
通知：企业微信应用消息 + CMS 增量同步
"""
import json
import threading
import time
from typing import Any, Dict, List, Optional

import requests

from .config import DATA_DIR, _atomic_write, get_settings
from .logger import get_logger

_wecom_token_cache: Dict[str, Any] = {"token": "", "expire_at": 0}


DEFAULT_WECOM_BASE = "https://qyapi.weixin.qq.com"
logger = get_logger()

# ------------------------------------------------------------------ CMS 通知节流
# CMS 是"扫目录"型接口：短时间连续触发会互相打断，结果就是漏整理。
# 做最小间隔（默认 5 分钟，设置页可改）：
#   · 间隔内来的触发不丢弃，先记 pending
#   · 到点由 flush_pending() 合并补发一次
#   · 状态落在 /config/notify_gate.json，重建容器也不会立刻重发
GATE_FILE = DATA_DIR / "notify_gate.json"
_gate_lock = threading.Lock()
_gate: Optional[Dict[str, Any]] = None


def _gate_blank() -> Dict[str, Any]:
    return {"last": 0.0, "pending": False, "last_error": ""}


def _gate_load() -> Dict[str, Any]:
    global _gate
    if _gate is not None:
        return _gate
    state = _gate_blank()
    if GATE_FILE.exists():
        try:
            raw = json.loads(GATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                state["last"] = float(raw.get("last") or 0)
                state["pending"] = bool(raw.get("pending"))
                state["last_error"] = str(raw.get("last_error") or "")
        except Exception as err:
            logger.warning(f"[CMS] 读取节流状态失败，按未触发处理：{err}")
    _gate = state
    return _gate


def _gate_save() -> None:
    if _gate is None:
        return
    try:
        _atomic_write(GATE_FILE, _gate)
    except Exception as err:
        logger.warning(f"[CMS] 保存节流状态失败：{err}")


def notify_cooldown_seconds() -> int:
    """两次触发之间的最小间隔（秒）；0 = 不限制"""
    try:
        minutes = int(float(str(get_settings().get("notify_cooldown_min", 5)).strip()))
    except Exception:
        minutes = 5
    return max(0, min(1440, minutes)) * 60


def _gate_remain() -> float:
    """还要等多少秒才能触发（<=0 表示现在就可以）"""
    cooldown = notify_cooldown_seconds()
    if cooldown <= 0:
        return 0.0
    return _gate_load()["last"] + cooldown - time.time()


def request_cms() -> Optional[str]:
    """
    带最小间隔地通知 CMS 做增量同步。
    间隔内不发，只记 pending，等主循环到点补发；企业微信不受此限制。
    """
    with _gate_lock:
        item = _gate_load()
        remain = _gate_remain()
        if remain > 0:
            item["pending"] = True
            _gate_save()
            minutes = max(1, notify_cooldown_seconds() // 60)
            logger.info(
                f"[CMS] 距上次触发不足 {minutes} 分钟，本轮合并，约 {int(remain)} 秒后补发"
            )
            return None
        item["last"] = time.time()
        item["pending"] = False
        _gate_save()

    err = trigger_cms()

    with _gate_lock:
        item = _gate_load()
        if err:
            # 发送失败：保留 pending，到点后再试一次，别丢掉这次整理
            item["pending"] = True
        item["last_error"] = err or ""
        _gate_save()
    return err


def flush_pending() -> List[str]:
    """主循环每轮调一次：间隔到了就把攒下的 CMS 触发补发一次"""
    cooldown = notify_cooldown_seconds()
    with _gate_lock:
        item = _gate_load()
        if not item["pending"] or (cooldown > 0 and time.time() - item["last"] < cooldown):
            return []
    logger.info("[CMS] 间隔已到，补发之前合并的通知")
    err = request_cms()
    return [err] if err else []


def notify_gate_info() -> Dict[str, Any]:
    """给 /api/status 用：上次触发时间、是否还有待补发的触发"""
    cooldown = notify_cooldown_seconds()
    with _gate_lock:
        item = _gate_load()
        last = float(item.get("last") or 0)
        return {
            "last": time.strftime("%m-%d %H:%M:%S", time.localtime(last)) if last else "",
            "pending": bool(item.get("pending")),
            "wait_sec": max(0, int(last + cooldown - time.time())) if cooldown > 0 else 0,
            "error": item.get("last_error") or "",
        }


def _truncate(text: str, limit: int = 1800) -> str:
    return text if len(text) <= limit else text[: limit - 3] + "..."


def _wecom_base() -> str:
    """
    企业微信 API 的基地址。
    配置了「代理地址」就用代理（把 host 换掉，路径不变），和 MoviePilot 的做法一致：
        https://qyapi.weixin.qq.com/cgi-bin/gettoken
        -> http://代理:端口/cgi-bin/gettoken
    """
    proxy = str(get_settings().get("wecom_proxy") or "").strip().rstrip("/")
    return proxy or DEFAULT_WECOM_BASE


def send_wecom(title: str, content: str, require_enabled: bool = True) -> Optional[str]:
    """
    企业微信应用消息。返回 None 表示成功，否则返回错误说明
    require_enabled=False 时忽略「启用」开关（用于"发送测试消息"）
    """
    cfg = get_settings()
    if require_enabled and not cfg.get("wecom_enabled"):
        return "企业微信通知未启用"
    corpid = str(cfg.get("wecom_corpid") or "").strip()
    secret = str(cfg.get("wecom_secret") or "").strip()
    agentid = str(cfg.get("wecom_agentid") or "").strip()
    touser = str(cfg.get("wecom_touser") or "@all").strip() or "@all"
    if not (corpid and secret and agentid):
        return "企业微信配置不完整（corpid / secret / agentid 必填）"

    try:
        token = _wecom_access_token(corpid, secret)
        if not token:
            return "获取企业微信 access_token 失败"
        # 固定用纯文本：markdown 在企业微信 App 能看，但转发到微信会显示
        # "暂不支持此消息类型，请在企业微信中查看"，所以统一走 text
        payload = {
            "touser": touser,
            "msgtype": "text",
            "agentid": int(agentid),
            "safe": 0,
            "text": {"content": _truncate(f"{title}\n{content}")},
        }
        resp = requests.post(
            f"{_wecom_base()}/cgi-bin/message/send?access_token={token}",
            json=payload,
            timeout=20,
        )
        data = resp.json()
        if data.get("errcode") not in (0, None):
            message = f"企业微信返回错误：{data.get('errcode')} {data.get('errmsg')}"
            logger.warning(f"[通知] 企业微信发送失败：{message}")
            return message
        logger.info(f"[通知] 企业微信已发送：{title}（发给 {touser}）")
        return None
    except Exception as err:
        return f"企业微信发送异常：{err}"


def _wecom_access_token(corpid: str, secret: str) -> Optional[str]:
    now = time.time()
    cache_key = f"{corpid}|{_wecom_base()}"
    if (
        _wecom_token_cache.get("token")
        and _wecom_token_cache.get("key") == cache_key
        and _wecom_token_cache["expire_at"] > now
    ):
        return _wecom_token_cache["token"]
    resp = requests.get(
        f"{_wecom_base()}/cgi-bin/gettoken",
        params={"corpid": corpid, "corpsecret": secret},
        timeout=20,
    )
    data = resp.json()
    token = data.get("access_token")
    if not token:
        return None
    _wecom_token_cache["token"] = token
    _wecom_token_cache["key"] = cache_key
    _wecom_token_cache["expire_at"] = now + int(data.get("expires_in") or 7200) - 300
    return token


def trigger_cms() -> Optional[str]:
    """
    通知 CMS 做增量同步（生成 strm）。返回 None 表示成功
    """
    cfg = get_settings()
    if not cfg.get("cms_enabled"):
        return None
    domain = str(cfg.get("cms_domain") or "").strip().rstrip("/")
    token = str(cfg.get("cms_token") or "").strip()
    sync_type = str(cfg.get("cms_type") or "lift_sync").strip()
    if not (domain and token):
        return "CMS 配置不完整（地址 / token 必填）"
    url = f"{domain}/api/sync/lift_by_token"
    logger.info(f"[CMS] 通知增量同步：{domain}（type={sync_type}）")
    try:
        resp = requests.get(
            url, params={"token": token, "type": sync_type}, timeout=20
        )
        if resp.status_code >= 400:
            message = f"CMS 返回 HTTP {resp.status_code}"
            logger.warning(f"[CMS] 通知失败：{message}")
            return message
        try:
            data = resp.json()
            if isinstance(data, dict) and data.get("success") is False:
                message = f"CMS 返回失败：{data.get('message') or data}"
                logger.warning(f"[CMS] 通知失败：{message}")
                return message
            logger.info(f"[CMS] 通知成功：{str(data)[:150]}")
        except Exception:
            logger.info(f"[CMS] 通知成功：{(resp.text or '')[:150]}")
        return None
    except Exception as err:
        message = f"CMS 通知异常：{err}"
        logger.warning(f"[CMS] {message}")
        return message


def _build_text(results: List[Dict[str, Any]]) -> tuple:
    """
    生成标题 + 正文（企业微信 markdown）。只讲清楚：哪些文件已经秒传成功
    """
    cfg = get_settings()
    succeeded = [r for r in results if r.get("status") == "成功"]
    held = [r for r in results if r.get("status") == "拦截"]
    failed = [r for r in results if r.get("status") == "失败"]
    show_hold = bool(cfg.get("notify_on_hold", True))

    # 多个任务时才带任务名前缀，单任务直接用相对路径
    tasks = {r.get("task") for r in results if r.get("task")}
    multi = len(tasks) > 1

    def label(item: Dict[str, Any]) -> str:
        name = str(item.get("file") or "")
        return f"{item.get('task')}/{name}" if multi and item.get("task") else name

    title = "115秒传助手"
    if succeeded:
        title = f"115秒传助手：{len(succeeded)} 个文件已入库"
    elif failed:
        title = "115秒传助手：处理失败"
    elif held:
        title = "115秒传助手：暂不能秒传"

    lines: List[str] = []
    if succeeded:
        lines.append(f"【已秒传成功 {len(succeeded)} 个】")
        for item in succeeded[:15]:
            size = item.get("size") or ""
            lines.append(f"· {label(item)}" + (f"  （{size}）" if size else ""))
        if len(succeeded) > 15:
            lines.append(f"…… 还有 {len(succeeded) - 15} 个")
    if show_hold and held:
        if lines:
            lines.append("")
        next_hint = ""
        for item in held:
            note = str(item.get("note") or "")
            if "再试" in note:
                next_hint = note[note.find("，") + 1 :] if "，" in note else ""
                break
        lines.append(f"【暂不能秒传 {len(held)} 个】" + (f" {next_hint}" if next_hint else ""))
        for item in held[:10]:
            lines.append(f"· {label(item)}")
        if len(held) > 10:
            lines.append(f"…… 还有 {len(held) - 10} 个")
    if failed:
        if lines:
            lines.append("")
        lines.append(f"【失败 {len(failed)} 个】")
        for item in failed[:10]:
            lines.append(f"· {label(item)}：{item.get('note') or ''}")
    return title, "\n".join(lines)


def notify_batch(results: List[Dict[str, Any]]) -> List[str]:
    """
    一轮处理结束后统一通知。返回错误信息列表（空 = 全部成功）
    """
    errors: List[str] = []
    if not results:
        return errors

    succeeded = [r for r in results if r.get("status") == "成功"]
    held = [r for r in results if r.get("status") == "拦截"]
    failed = [r for r in results if r.get("status") == "失败"]
    show_hold = bool(get_settings().get("notify_on_hold", True))

    # 只有"成功 / 失败 / 首次拦截"才值得发通知，避免刷屏
    if not succeeded and not failed and not (show_hold and held):
        return errors

    # 没配企业微信就别记"通知失败"，只做 CMS
    if get_settings().get("wecom_enabled"):
        title, content = _build_text(results)
        err = send_wecom(title, content)
        if err:
            errors.append(err)

    # 只有真的往 115 里写了东西，才需要让 CMS 同步；
    # 任务可以单独关掉（比如下载目录只做秒传、不进媒体库）
    cms_items = [r for r in succeeded if r.get("cms", True)]
    if cms_items:
        cms_err = request_cms()
        if cms_err:
            errors.append(cms_err)
    return errors
