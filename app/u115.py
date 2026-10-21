"""
115 开放平台客户端（只做本工具需要的事）

扫码授权流程（PKCE 设备码，官方开放平台）：
  1) POST passportapi.115.com/open/authDeviceCode   拿到 uid/time/sign 和二维码内容
  2) 手机 115 App 扫码并确认
  3) GET  qrcodeapi.115.com/get/status/             轮询状态，status==2 表示已确认
  4) POST passportapi.115.com/open/deviceCodeToToken 换 access_token / refresh_token
之后所有接口都带 Authorization: Bearer access_token，快过期时用 refresh_token 续期。
"""
import base64
import hashlib
import secrets
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .config import load_token, save_token, get_settings

API_BASE = "https://proapi.115.com"
PASSPORT = "https://passportapi.115.com"
QRCODE_API = "https://qrcodeapi.115.com"

_UA = "W115Rapid/1.0"


class U115Error(Exception):
    pass


class U115Client:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": _UA,
                "Accept": "application/json",
                "Content-Type": "application/x-www-form-urlencoded",
            }
        )
        self._token_lock = threading.Lock()
        self._qr_state: Dict[str, Any] = {}
        self._token: Dict[str, Any] = load_token()

    # ---------------------------------------------------------------- 授权状态

    def token_info(self) -> Dict[str, Any]:
        return self._token or {}

    def is_authorized(self) -> bool:
        return bool((self._token or {}).get("refresh_token"))

    def logout(self) -> None:
        self._token = {}
        save_token({})

    def access_token(self) -> Optional[str]:
        """
        取 access_token，快过期时自动刷新
        """
        with self._token_lock:
            token = self._token or {}
            refresh_token = token.get("refresh_token")
            if not refresh_token:
                return None
            expires_in = int(token.get("expires_in") or 0)
            refresh_time = int(token.get("refresh_time") or 0)
            # 提前 5 分钟续期
            if expires_in and refresh_time + expires_in - 300 < int(time.time()):
                new_token = self._refresh(refresh_token)
                if not new_token:
                    return None
                token = {**new_token, "refresh_time": int(time.time())}
                self._token = token
                save_token(token)
            access = token.get("access_token")
            if access:
                self.session.headers.update({"Authorization": f"Bearer {access}"})
            return access

    def _refresh(self, refresh_token: str) -> Optional[Dict[str, Any]]:
        try:
            resp = requests.post(
                f"{PASSPORT}/open/refreshToken",
                data={"refresh_token": refresh_token},
                timeout=20,
                headers={"User-Agent": _UA},
            )
            data = resp.json()
        except Exception as err:
            print(f"[115] 刷新 token 异常：{err}")
            return None
        if data.get("code") != 0:
            print(f"[115] 刷新 token 失败：{data.get('message')}")
            return None
        return data.get("data")

    # ---------------------------------------------------------------- 扫码登录

    def qr_create(self) -> Dict[str, Any]:
        """
        生成扫码用的设备码
        """
        client_id = str(get_settings().get("u115_client_id") or "100197847")
        verifier = secrets.token_urlsafe(96)[:128]
        challenge = base64.b64encode(
            hashlib.sha256(verifier.encode("utf-8")).digest()
        ).decode("utf-8")
        resp = requests.post(
            f"{PASSPORT}/open/authDeviceCode",
            data={
                "client_id": client_id,
                "code_challenge": challenge,
                "code_challenge_method": "sha256",
            },
            timeout=20,
            headers={"User-Agent": _UA},
        )
        data = resp.json()
        if data.get("code") != 0:
            raise U115Error(data.get("message") or "获取二维码失败")
        payload = data.get("data") or {}
        self._qr_state = {
            "uid": payload.get("uid"),
            "time": payload.get("time"),
            "sign": payload.get("sign"),
            "code_verifier": verifier,
            "created": time.time(),
        }
        return {"qrcode": payload.get("qrcode"), "uid": payload.get("uid")}

    def qr_status(self) -> Dict[str, Any]:
        """
        查询扫码状态：0=待扫码 1=已扫码待确认 2=已确认 其余=过期/取消
        """
        state = self._qr_state or {}
        if not state.get("uid"):
            raise U115Error("请先获取二维码")
        try:
            resp = requests.get(
                f"{QRCODE_API}/get/status/",
                params={
                    "uid": state["uid"],
                    "time": state["time"],
                    "sign": state["sign"],
                    "_": int(time.time() * 1000),
                },
                # 这个接口是长轮询：没变化就一直挂着，所以超时当"还没扫"
                timeout=(5, 25),
                headers={"User-Agent": _UA},
            )
        except requests.exceptions.Timeout:
            return {"status": 0, "msg": "等待扫码…"}
        except Exception as err:
            raise U115Error(f"查询扫码状态失败：{err}")
        data = resp.json()
        if data.get("code") != 0 or not data.get("data"):
            raise U115Error(data.get("message") or "查询状态失败")
        status = int(data["data"].get("status") or 0)
        msg = data["data"].get("msg") or ""
        if status == 2:
            token = self._device_code_to_token(state)
            self._token = {**token, "refresh_time": int(time.time())}
            save_token(self._token)
            self._qr_state = {}
            return {"status": 2, "msg": "授权成功"}
        return {"status": status, "msg": msg}

    def _device_code_to_token(self, state: Dict[str, Any]) -> Dict[str, Any]:
        resp = requests.post(
            f"{PASSPORT}/open/deviceCodeToToken",
            data={"uid": state["uid"], "code_verifier": state["code_verifier"]},
            timeout=20,
            headers={"User-Agent": _UA},
        )
        data = resp.json()
        if data.get("code") != 0:
            raise U115Error(data.get("message") or "换取 token 失败")
        return data.get("data") or {}

    # ---------------------------------------------------------------- 接口封装

    def request(
        self,
        method: str,
        endpoint: str,
        *,
        params: Optional[dict] = None,
        data: Optional[dict] = None,
        accepted_codes: Tuple[int, ...] = (0, 20004),
        timeout: int = 30,
    ) -> Optional[Dict[str, Any]]:
        if not self.access_token():
            raise U115Error("115 未授权，请先在网页里扫码登录")
        resp = self.session.request(
            method, f"{API_BASE}{endpoint}", params=params, data=data, timeout=timeout
        )
        if resp.status_code == 429:
            raise U115Error("115 接口限流（429），稍后再试")
        resp.raise_for_status()
        payload = resp.json()
        if payload.get("code") not in accepted_codes:
            raise U115Error(payload.get("message") or f"接口 {endpoint} 返回异常")
        return payload

    # ---------------------------------------------------------------- 目录

    def list_dir(self, cid: str) -> List[Dict[str, Any]]:
        items: List[Dict[str, Any]] = []
        offset = 0
        while True:
            payload = self.request(
                "GET",
                "/open/ufile/files",
                params={
                    "cid": int(cid or 0),
                    "limit": 1000,
                    "offset": offset,
                    "cur": "true",
                    "show_dir": 1,
                },
            )
            chunk = (payload or {}).get("data") or []
            items.extend(chunk)
            if len(chunk) < 1000:
                break
            offset += len(chunk)
        return items

    def ensure_dir(self, path: str) -> str:
        """
        按路径逐级解析目录 CID，不存在则创建
        """
        segments = [s for s in str(path or "/").split("/") if s]
        cid = "0"
        for name in segments:
            found = None
            for item in self.list_dir(cid):
                if item.get("fc") == "0" and item.get("fn") == name:
                    found = str(item.get("fid"))
                    break
            if found:
                cid = found
                continue
            payload = self.request(
                "POST",
                "/open/folder/add",
                data={
                    "pid": int(cid or 0),
                    "file_name": name,
                },
            )
            created = (payload or {}).get("data") or {}
            new_cid = created.get("cid") or created.get("file_id")
            if not new_cid:
                raise U115Error(f"创建网盘目录失败：{name}")
            cid = str(new_cid)
        return cid

    def find_file(self, cid: str, name: str) -> Optional[Dict[str, Any]]:
        for item in self.list_dir(cid):
            if item.get("fc") == "1" and item.get("fn") == name:
                return item
        return None

    # ---------------------------------------------------------------- 秒传

    def rapid_upload(
        self,
        cid: str,
        name: str,
        size: int,
        sha1: str,
        preid: str,
        sign_resolver=None,
    ) -> Tuple[bool, str]:
        """
        只做秒传：能秒传就创建网盘文件，不能就返回失败（不做真实上传）
        返回 (是否成功, 说明)
        """
        init_data = {
            "file_name": name,
            "file_size": size,
            "target": f"U_1_{cid}",
            "fileid": sha1,
            "preid": preid,
        }
        payload = self.request(
            "POST",
            "/open/upload/init",
            data=init_data,
            accepted_codes=(0, 20004, 700, 701),
        )
        data = (payload or {}).get("data") or {}

        # 115 的二次认证：要求回传某个字节区间的 SHA1
        code = data.get("code")
        sign_check = data.get("sign_check")
        if code in (700, 701) and sign_check and sign_resolver:
            sign_val = sign_resolver(sign_check)
            if sign_val:
                init_data.update(
                    {
                        "pick_code": data.get("pick_code"),
                        "sign_key": data.get("sign_key"),
                        "sign_val": sign_val,
                    }
                )
                payload = self.request(
                    "POST",
                    "/open/upload/init",
                    data=init_data,
                    accepted_codes=(0, 20004, 700, 701),
                )
                data = (payload or {}).get("data") or {}

        status = data.get("status")
        if status == 2:
            return True, "秒传成功"
        if status == 1:
            return False, "115 上没有相同内容，需要真实上传（本工具只做秒传）"
        return False, f"未命中秒传（status={status}, code={code}）"
