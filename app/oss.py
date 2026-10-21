"""
阿里 OSS 分片上传（115 开放平台的上传流程要用）

只实现 115 需要的三件事：
  1. InitiateMultipartUpload  POST  /{object}?uploads&sequential
  2. UploadPart               PUT   /{object}?partNumber=N&uploadId=X
  3. CompleteMultipartUpload  POST  /{object}?uploadId=X   （带 x-oss-callback）

用 OSS V1 签名（STS 临时凭证时额外带 x-oss-security-token）。
不依赖 oss2，少一个 C 扩展依赖，镜像更小。
"""
import base64
import hashlib
import hmac
import re
from email.utils import formatdate
from typing import List, Optional, Tuple
from urllib.parse import quote

import requests


class OssError(Exception):
    pass


class OssMultipart:
    def __init__(
        self,
        endpoint: str,
        bucket: str,
        access_key_id: str,
        access_key_secret: str,
        security_token: Optional[str] = None,
        timeout: int = 120,
    ):
        endpoint = str(endpoint or "").strip()
        endpoint = endpoint.replace("https://", "").replace("http://", "").strip("/")
        self.endpoint = endpoint
        self.bucket = bucket
        self.ak = access_key_id
        self.sk = access_key_secret
        self.token = security_token
        self.timeout = timeout
        self.session = requests.Session()

    # ---------------------------------------------------------------- URL

    def _url(self, object_name: str, params: Optional[dict] = None) -> str:
        obj = quote(str(object_name).lstrip("/"), safe="/")
        url = f"https://{self.bucket}.{self.endpoint}/{obj}"
        if params:
            parts = []
            for key, value in params.items():
                if value == "" or value is None:
                    parts.append(key)
                else:
                    parts.append(f"{key}={quote(str(value), safe='')}")
            if parts:
                url += "?" + "&".join(parts)
        return url

    # ---------------------------------------------------------------- 签名

    def _signed_headers(
        self, method: str, object_name: str, params: Optional[dict], extra: Optional[dict] = None
    ) -> dict:
        date = formatdate(usegmt=True)
        headers = {"Date": date}
        if self.token:
            headers["x-oss-security-token"] = self.token
        if extra:
            headers.update(extra)

        # x-oss-* 头参与签名，必须小写并按字典序
        oss_headers = {
            key.lower(): str(value)
            for key, value in headers.items()
            if key.lower().startswith("x-oss-")
        }
        canonical_oss = "".join(f"{k}:{oss_headers[k]}\n" for k in sorted(oss_headers))

        resource = f"/{self.bucket}/{str(object_name).lstrip('/')}"
        if params:
            sub = sorted((k, "" if v is None else str(v)) for k, v in params.items())
            resource += "?" + "&".join(
                key if value == "" else f"{key}={value}" for key, value in sub
            )

        string_to_sign = (
            f"{method}\n"
            f"{headers.get('Content-MD5', '')}\n"
            f"{headers.get('Content-Type', '')}\n"
            f"{date}\n"
            f"{canonical_oss}"
            f"{resource}"
        )
        signature = base64.b64encode(
            hmac.new(
                self.sk.encode("utf-8"),
                string_to_sign.encode("utf-8"),
                hashlib.sha1,
            ).digest()
        ).decode()
        headers["Authorization"] = f"OSS {self.ak}:{signature}"
        return headers

    # ---------------------------------------------------------------- 分片

    def init_multipart(self, object_name: str, sequential: bool = True) -> str:
        params = {"uploads": ""}
        if sequential:
            params["sequential"] = ""
        headers = self._signed_headers("POST", object_name, params)
        resp = self.session.post(
            self._url(object_name, params), headers=headers, timeout=self.timeout
        )
        if resp.status_code != 200:
            raise OssError(f"初始化分片失败：HTTP {resp.status_code} {resp.text[:200]}")
        match = re.search(r"<UploadId>(.*?)</UploadId>", resp.text or "")
        if not match:
            raise OssError("初始化分片失败：未返回 UploadId")
        return match.group(1)

    def upload_part(
        self, object_name: str, upload_id: str, part_number: int, data: bytes
    ) -> str:
        params = {"partNumber": str(part_number), "uploadId": upload_id}
        headers = self._signed_headers(
            "PUT", object_name, params, {"Content-Length": str(len(data))}
        )
        resp = self.session.put(
            self._url(object_name, params),
            headers=headers,
            data=data,
            timeout=self.timeout,
        )
        if resp.status_code != 200:
            raise OssError(
                f"分片 {part_number} 上传失败：HTTP {resp.status_code} {resp.text[:200]}"
            )
        etag = resp.headers.get("ETag") or resp.headers.get("etag")
        if not etag:
            raise OssError(f"分片 {part_number} 未返回 ETag")
        return etag

    def complete(
        self,
        object_name: str,
        upload_id: str,
        parts: List[Tuple[int, str]],
        callback_b64: str,
        callback_var_b64: Optional[str] = None,
        forbid_overwrite: bool = False,
    ) -> dict:
        """
        完成分片并触发 115 的回调（回调返回 JSON 里 state 为 true 才算成功）
        """
        body = (
            "<CompleteMultipartUpload>"
            + "".join(
                f"<Part><PartNumber>{number}</PartNumber><ETag>{etag}</ETag></Part>"
                for number, etag in parts
            )
            + "</CompleteMultipartUpload>"
        ).encode("utf-8")
        params = {"uploadId": upload_id}
        extra = {
            "Content-Type": "application/xml",
            "Content-MD5": base64.b64encode(hashlib.md5(body).digest()).decode(),
            "x-oss-callback": callback_b64,
            "x-oss-forbid-overwrite": "true" if forbid_overwrite else "false",
        }
        if callback_var_b64:
            extra["x-oss-callback-var"] = callback_var_b64
        headers = self._signed_headers("POST", object_name, params, extra)
        resp = self.session.post(
            self._url(object_name, params),
            headers=headers,
            data=body,
            timeout=self.timeout,
        )
        if resp.status_code not in (200, 203):
            raise OssError(f"完成上传失败：HTTP {resp.status_code} {resp.text[:200]}")
        try:
            return resp.json()
        except Exception:
            return {"state": True, "raw": (resp.text or "")[:200]}
