"""
通知：企业微信应用消息 + CMS 增量同步
"""
import json
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .config import DATA_DIR, _atomic_write, get_settings
from .logger import get_logger

_wecom_token_cache: Dict[str, Any] = {"token": "", "expire_at": 0}


DEFAULT_WECOM_BASE = "https://qyapi.weixin.qq.com"
logger = get_logger()

# ------------------------------------------------------------------ CMS 通知闸门
# CMS 的 /api/sync/lift_by_token 只是「创建一次增量同步任务」，返回 200 不等于整理完成。
# 连续秒传多集时如果每成功一集就打一次，会有两个问题：
#   1. CMS 侧任务互相打断 / 被去重丢弃，先扫到的批次之后入库的文件就漏整理了
#   2. 115 目录列表有短暂延迟，刚秒传完立刻通知，CMS 可能压根扫不到刚入库的文件
# 所以这里改成「防抖 + 节流 + 失败补发」：
#   · 每次秒传成功只登记待发，并把触发时间往后推 quiet_sec（等这一批下完）
#   · 距上次发送不足 cooldown_sec 时继续等，到点由主循环合并补发一次
#   · 发送失败（含 CMS 返回 code != 200）保留待发状态，稍后自动重试
#   · 状态落盘，容器重启也不会把攒下的通知弄丢
GATE_FILE = DATA_DIR / "cms_gate.json"
GATE_RETRY_SEC = 60
GATE_MAX_WAIT_SEC = 900  # 连续不断有新文件时，最多也只推迟 15 分钟就得发一次

_gate_lock = threading.Lock()
_gate: Optional[Dict[str, Any]] = None
_gate_sending = False


def _gate_blank() -> Dict[str, Any]:
    return {
        "pending": False,
        "due_at": 0.0,
        "first_pending_at": 0.0,
        "last_sent": 0.0,
        "last_request_at": 0.0,
        "last_error": "",
        "last_result": "",
        "count": 0,
    }


def _gate_load() -> Dict[str, Any]:
    global _gate
    if _gate is not None:
        return _gate
    state = _gate_blank()
    try:
        if GATE_FILE.exists():
            raw = json.loads(GATE_FILE.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for key in state:
                    if key in raw and isinstance(raw[key], type(state[key])):
                        state[key] = raw[key]
    except Exception as err:
        logger.warning(f"[CMS] 读取通知状态失败，按未触发处理：{err}")
    _gate = state
    return _gate


def _gate_save() -> None:
    if _gate is not None:
        try:
            _atomic_write(GATE_FILE, _gate)
        except Exception as err:
            logger.warning(f"[CMS] 保存通知状态失败：{err}")


def gate_config() -> Tuple[int, int]:
    """（静默秒数, 两次通知最小间隔秒数）"""
    cfg = get_settings()

    def _int(key: str, default: int, low: int, high: int) -> int:
        try:
            value = int(float(str(cfg.get(key, default)).strip()))
        except Exception:
            value = default
        return max(low, min(high, value))

    return _int("cms_quiet_sec", 90, 0, 600), _int("cms_cooldown_sec", 300, 0, 3600)


def request_cms(reason: str = "") -> None:
    """
    一批文件秒传成功：只登记待发，不立刻打 CMS。
    真正发送交给 flush_cms()，由 worker 主循环每轮调用。
    """
    if not get_settings().get("cms_enabled"):
        return
    quiet, cooldown = gate_config()
    now = time.time()
    with _gate_lock:
        item = _gate_load()
        last_sent = float(item.get("last_sent") or 0)
        if not item.get("pending"):
            item["first_pending_at"] = now
        first = float(item.get("first_pending_at") or now)
        # 正常是「静默期过了 + 距上次发送够久」；但连续来新文件会不断把静默期往后推，
        # 所以用第一批的时间做锚点封顶，保证最多等 15 分钟一定会通知一次
        max_wait = max(GATE_MAX_WAIT_SEC, cooldown)
        due = max(now + quiet, last_sent + cooldown)
        item["pending"] = True
        item["due_at"] = min(due, first + max_wait)
        item["last_request_at"] = now
        item["last_error"] = ""
        item["count"] = int(item.get("count") or 0) + 1
        wait = max(0, int(item["due_at"] - now))
        _gate_save()
    logger.info(
        f"[CMS] 登记待发（{reason or '秒传成功'}），累计 {item['count']} 次触发合并为一次，"
        f"约 {wait} 秒后通知"
    )


def flush_cms() -> Optional[str]:
    """
    主循环每轮调用：到点把攒下的触发合并发一次。
    返回 None = 没到点 / 发送成功；返回字符串 = 发送失败（已安排重试）
    """
    global _gate_sending
    now = time.time()
    with _gate_lock:
        item = _gate_load()
        if _gate_sending or not item.get("pending"):
            return None
        if now < float(item.get("due_at") or 0):
            return None
        count = int(item.get("count") or 0)
        _gate_sending = True
        started = now

    logger.info(f"[CMS] 静默期结束，合并补发增量同步（本批累计触发 {count} 次）")
    try:
        err = trigger_cms()
    except Exception as ex:  # trigger_cms 内部已经兜了一层，这里再兜一次防止主循环挂掉
        err = f"CMS 通知异常：{ex}"

    with _gate_lock:
        item = _gate_load()
        _gate_sending = False
        item["last_sent"] = time.time()
        if err:
            item["pending"] = True
            item["due_at"] = time.time() + GATE_RETRY_SEC
            item["first_pending_at"] = time.time()  # 重试重新计时，别被旧锚点卡住
            item["last_error"] = str(err)
            item["count"] = count
        else:
            item["last_error"] = ""
            item["last_result"] = time.strftime("%Y-%m-%d %H:%M:%S")
            # 发送期间又来了新的秒传成功 → 保留待发，等这批也静默完再补一次
            if float(item.get("last_request_at") or 0) <= started:
                item["pending"] = False
                item["first_pending_at"] = 0.0
                item["count"] = 0
            else:
                item["first_pending_at"] = time.time()
        _gate_save()
    if err:
        logger.warning(f"[CMS] 通知失败，{GATE_RETRY_SEC} 秒后自动重试：{err}")
    return err


def cms_gate_info() -> Dict[str, Any]:
    """给 /api/status 用：是否有待发通知、还要等多久、上次发送结果"""
    quiet, cooldown = gate_config()
    with _gate_lock:
        item = dict(_gate_load())
    due = float(item.get("due_at") or 0)
    last_sent = float(item.get("last_sent") or 0)
    return {
        "pending": bool(item.get("pending")),
        "wait_sec": max(0, int(due - time.time())) if item.get("pending") else 0,
        "last_sent": time.strftime("%m-%d %H:%M:%S", time.localtime(last_sent))
        if last_sent
        else "",
        "last_error": item.get("last_error") or "",
        "last_result": item.get("last_result") or "",
        "quiet_sec": quiet,
        "cooldown_sec": cooldown,
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
        except Exception:
            # 不是 JSON 也可能是反代返回的成功页，按成功处理但留个痕
            logger.info(f"[CMS] 通知已发出（非 JSON 响应）：{(resp.text or '')[:150]}")
            return None
        if isinstance(data, dict):
            # 关键：CMS 用 code 表示结果（code=200 才是「创建任务成功」）。
            # 旧版只看 success 字段，CMS 返回「任务正在运行」之类的 code!=200 时
            # 会被误判成功、不再重试，这正是连续秒传时漏整理的原因之一。
            if data.get("success") is False:
                message = f"CMS 返回失败：{data.get('message') or data.get('msg') or data}"
                logger.warning(f"[CMS] 通知失败：{message}")
                return message
            code = data.get("code")
            if code is not None and str(code) not in ("200", "0"):
                message = f"CMS 返回失败：code={code} {data.get('msg') or data.get('message') or ''}".strip()
                logger.warning(f"[CMS] 通知失败：{message}")
                return message
        logger.info(f"[CMS] 通知成功：{str(data)[:150]}")
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
    cms_items = [r for r in succeeded if r.get("cms") is not False]
    if cms_items:
        # 不直接打 CMS：登记待发，等连续秒传的这一批全部完成、静默期过了再合并触发一次
        request_cms(f"{len(cms_items)} 个文件秒传成功")
    elif succeeded:
        logger.info("[CMS] 本轮成功文件都关闭了 CMS 通知，跳过")
    return errors
