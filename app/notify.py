"""
通知：企业微信应用消息 + CMS 增量同步 + Muvyo 目录监控整理
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

# ------------------------------------------------------------------ 通知节流
# CMS / Muvyo 都是"扫目录"型接口：短时间连续触发会互相打断，结果就是漏整理。
# 所以每个通道各自做最小间隔（默认 5 分钟，设置页可改）：
#   · 间隔内来的触发不丢弃，先攒着（pending）
#   · 到点由 flush_pending() 合并补发一次
#   · 状态落在 /config/notify_gate.json，重建容器也不会立刻重发
GATE_FILE = DATA_DIR / "notify_gate.json"
MAX_PENDING_DIRS = 50
CHANNEL_LABEL = {"cms": "CMS", "muvyo": "Muvyo"}

_gate_lock = threading.Lock()
_gate: Optional[Dict[str, Any]] = None


def _gate_blank() -> Dict[str, Any]:
    return {
        "cms": {"last": 0.0, "pending": False, "dirs": [], "last_error": ""},
        "muvyo": {"last": 0.0, "pending": False, "dirs": [], "last_error": ""},
    }


def _gate_load() -> Dict[str, Any]:
    global _gate
    if _gate is not None:
        return _gate
    state = _gate_blank()
    if GATE_FILE.exists():
        try:
            raw = json.loads(GATE_FILE.read_text(encoding="utf-8"))
            for channel, item in (raw or {}).items():
                if channel not in state or not isinstance(item, dict):
                    continue
                state[channel]["last"] = float(item.get("last") or 0)
                state[channel]["pending"] = bool(item.get("pending"))
                state[channel]["dirs"] = [
                    str(d) for d in (item.get("dirs") or []) if str(d or "").strip()
                ][:MAX_PENDING_DIRS]
                state[channel]["last_error"] = str(item.get("last_error") or "")
        except Exception as err:
            logger.warning(f"[通知] 读取节流状态失败，按未触发处理：{err}")
    _gate = state
    return _gate


def _gate_save() -> None:
    if _gate is None:
        return
    try:
        _atomic_write(GATE_FILE, _gate)
    except Exception as err:
        logger.warning(f"[通知] 保存节流状态失败：{err}")


def notify_cooldown_seconds() -> int:
    """两次触发之间的最小间隔（秒）；0 = 不限制"""
    try:
        minutes = int(float(str(get_settings().get("notify_cooldown_min", 5)).strip()))
    except Exception:
        minutes = 5
    return max(0, min(1440, minutes)) * 60


def _gate_remain(channel: str) -> float:
    """还要等多少秒才能触发（<=0 表示现在就可以）"""
    cooldown = notify_cooldown_seconds()
    if cooldown <= 0:
        return 0.0
    return _gate_load()[channel]["last"] + cooldown - time.time()


def _merge_dirs(item: Dict[str, Any], dirs: Optional[List[str]]) -> None:
    merged = list(item.get("dirs") or [])
    for path in dirs or []:
        path = str(path or "").strip() or "/"
        if path not in merged:
            merged.append(path)
    item["dirs"] = merged[:MAX_PENDING_DIRS]


# 这些错误重试也没用（配置 / 权限问题），记一次就够了，别每 5 分钟撞一次
_PERMANENT_HINTS = (
    "未找到",
    "没有监控",
    "不存在",
    "配置不完整",
    "not found",
    "invalid",
    "无权限",
    "HTTP 401",
    "HTTP 403",
)


def _looks_permanent(message: str) -> bool:
    text = str(message or "").lower()
    return any(hint.lower() in text for hint in _PERMANENT_HINTS)


def _send_channel(channel: str, dirs: List[str]) -> Optional[str]:
    cfg = get_settings()
    if channel == "cms":
        if not cfg.get("cms_enabled"):
            return None
        return trigger_cms()
    if not cfg.get("muvyo_enabled"):
        return None
    return trigger_muvyo(dirs)


def _gate_run(
    channel: str, dirs: Optional[List[str]] = None, force: bool = False
) -> Optional[str]:
    """
    节流 → 发送 → 记账。间隔内不发，只记 pending，等 flush_pending 补
    """
    label = CHANNEL_LABEL.get(channel, channel)
    with _gate_lock:
        item = _gate_load()[channel]
        _merge_dirs(item, dirs)
        remain = 0.0 if force else _gate_remain(channel)
        if remain > 0:
            item["pending"] = True
            _gate_save()
            minutes = max(1, notify_cooldown_seconds() // 60)
            logger.info(
                f"[{label}] 距上次触发不足 {minutes} 分钟，本轮合并，约 {int(remain)} 秒后补发"
            )
            return None
        batch = list(item.get("dirs") or [])
        item["dirs"] = []
        item["last"] = time.time()
        item["pending"] = False
        _gate_save()

    if batch and channel != "cms":
        logger.info(f"[{label}] 本次触发目录：{'、'.join(batch)}")

    err = _send_channel(channel, batch)

    with _gate_lock:
        item = _gate_load()[channel]
        if err:
            # 网络抖动之类的失败：把目录还回去，下个间隔再补，别丢
            # 配置问题（目录没规则、Key 不对）：重试也没用，不排重试，只记一次错误
            retry = not _looks_permanent(err)
            if retry:
                _merge_dirs(item, batch)
            item["pending"] = retry
            item["last_error"] = err
            if not retry:
                logger.warning(f"[{label}] 属于配置问题，不再自动重试：{err}")
        else:
            item["last_error"] = ""
        _gate_save()
    return err


def flush_pending() -> List[str]:
    """
    主循环每两秒调一次：间隔到了就把攒下的触发合并补发
    返回错误信息列表（空 = 没有任何需要补发的，或都成功了）
    """
    errors: List[str] = []
    cooldown = notify_cooldown_seconds()
    now = time.time()
    ready: List[str] = []
    with _gate_lock:
        for channel in ("cms", "muvyo"):
            item = _gate_load()[channel]
            if not item["pending"]:
                continue
            if cooldown > 0 and now - item["last"] < cooldown:
                continue
            ready.append(channel)
    for channel in ready:
        label = CHANNEL_LABEL.get(channel, channel)
        logger.info(f"[{label}] 间隔已到，补发之前合并的通知")
        err = _gate_run(channel, force=True)
        if err:
            errors.append(err)
    return errors


def request_cms() -> Optional[str]:
    """带最小间隔地通知 CMS 做增量同步"""
    return _gate_run("cms")


def request_muvyo(target_dirs: Optional[List[str]] = None) -> Optional[str]:
    """带最小间隔地通知 Muvyo 整理目录"""
    return _gate_run("muvyo", target_dirs)


def notify_gate_info() -> Dict[str, Any]:
    """给 /api/status 用：各通道上次触发时间、是否还有待补发的触发"""
    cooldown = notify_cooldown_seconds()
    now = time.time()
    out: Dict[str, Any] = {}
    with _gate_lock:
        for channel in ("cms", "muvyo"):
            item = _gate_load()[channel]
            last = float(item.get("last") or 0)
            remain = max(0.0, last + cooldown - now) if cooldown > 0 else 0.0
            out[channel] = {
                "last": time.strftime("%m-%d %H:%M:%S", time.localtime(last))
                if last
                else "",
                "pending": bool(item.get("pending")),
                "dirs": len(item.get("dirs") or []),
                "wait_sec": int(remain),
                "error": item.get("last_error") or "",
            }
    return out


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


def trigger_muvyo_report(
    target_dirs: Optional[List[str]] = None, require_enabled: bool = True
) -> Dict[str, Any]:
    """
    通知 Muvyo 走一遍「目录监控整理」：转存/上传进网盘的目录，让它扫到并生成 STRM。
    接口：POST /api/v1/monitor/trigger-organize
      - name       监控规则名（推荐，同目录挂了多个网盘账号时只有它能分清）
      - source_dir 监控源目录（该目录只对应一条规则时可用）

    逐个目录触发，一个目录报错不影响其它目录。返回明细：
      ok      是否全部成功
      sent    成功的标签（规则名 / 目录）
      missing Muvyo 里找不到监控规则的目录（配置问题，重试也不会好）
      failed  其它失败 [{"label":..., "error":...}]
      error   汇总给用户看的一句话（None = 没问题）
    """
    report: Dict[str, Any] = {
        "ok": True,
        "sent": [],
        "missing": [],
        "failed": [],
        "error": None,
    }
    cfg = get_settings()
    if require_enabled and not cfg.get("muvyo_enabled"):
        report.update(ok=False, error="Muvyo 通知未启用")
        return report
    base = str(cfg.get("muvyo_url") or "").strip().rstrip("/")
    apikey = str(cfg.get("muvyo_apikey") or "").strip()
    rule = str(cfg.get("muvyo_rule") or "").strip()
    if not base:
        report.update(ok=False, error="Muvyo 配置不完整（管理面板地址必填）")
        return report
    if not apikey:
        report.update(ok=False, error="Muvyo 配置不完整（API Key 必填）")
        return report

    url = f"{base}/api/v1/monitor/trigger-organize"
    headers = {"X-API-Key": apikey}

    # 填了规则名就一次搞定；否则按各自任务的 115 目标目录分别触发
    calls: List[Dict[str, str]] = []
    if rule:
        calls.append({"name": rule})
    else:
        for path in target_dirs or []:
            path = str(path or "").strip() or "/"
            if path not in [c.get("source_dir") for c in calls]:
                calls.append({"source_dir": path})

    for params in calls:
        label = params.get("name") or params.get("source_dir") or "-"
        logger.info(f"[Muvyo] 触发目录监控整理：{label}")
        message = ""
        try:
            resp = requests.post(url, params=params, headers=headers, timeout=20)
            body = (resp.text or "").strip()
            if resp.status_code >= 400:
                message = f"Muvyo 返回 HTTP {resp.status_code}：{body[:200]}"
            else:
                try:
                    data = resp.json()
                except Exception:
                    data = None
                if isinstance(data, dict) and data.get("success") is False:
                    message = f"Muvyo 返回失败：{data.get('message') or data}"
                else:
                    logger.info(f"[Muvyo] 通知成功：{str(data or body)[:150]}")
        except Exception as err:
            message = f"Muvyo 通知异常：{err}"

        if not message:
            report["sent"].append(label)
            continue

        logger.warning(f"[Muvyo] 通知失败：{message}")
        # 「未找到监控目录为 X 的规则」是配置问题：这条目录在 Muvyo 里没建规则
        if "source_dir" in params and "未找到" in message:
            report["missing"].append(params["source_dir"])
        else:
            report["failed"].append({"label": label, "error": message})

    parts: List[str] = []
    if report["missing"]:
        shown = "、".join(report["missing"][:3])
        more = " 等" if len(report["missing"]) > 3 else ""
        parts.append(f"Muvyo 里没有监控「{shown}」{more}的规则")
    for item in report["failed"][:3]:
        parts.append(item["error"])
    if parts:
        report["ok"] = False
        report["error"] = "；".join(parts)
        # 全部目录都不在 Muvyo 的监控规则里时，补一句怎么办
        if report["missing"] and not report["sent"] and not report["failed"]:
            report["error"] += "（在 Muvyo 加一条监控这个目录的规则，或在设置里填「监控规则名」）"
    return report


def trigger_muvyo(
    target_dirs: Optional[List[str]] = None, require_enabled: bool = True
) -> Optional[str]:
    """返回 None 表示成功，否则返回错误说明（给"测试触发"用，不做节流）"""
    return trigger_muvyo_report(target_dirs, require_enabled).get("error")


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

    # 同理，Muvyo 也有独立开关 + 任务级勾选
    muvyo_dirs = [
        str(r.get("target_dir") or "/") for r in succeeded if r.get("muvyo", True)
    ]
    # 没开开关就当没配，不能把"未启用"记成错误（默认就是关的）
    if muvyo_dirs and get_settings().get("muvyo_enabled"):
        muvyo_err = request_muvyo(muvyo_dirs)
        if muvyo_err:
            errors.append(muvyo_err)
    return errors
