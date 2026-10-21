"""
登录认证：账号密码 + 签名 Cookie 会话

- 首次启动自动创建 admin / admin
- 密码用 PBKDF2-HMAC-SHA256 加盐存储，不存明文
- 会话是 HMAC 签名的 cookie，7 天有效，改密码后旧会话自动失效（密码哈希参与签名）
- 忘记密码：删掉 /config/auth.json，重启即恢复 admin/admin
"""
import base64
import hashlib
import hmac
import json
import secrets
import time
from typing import Optional

from .config import DATA_DIR

AUTH_FILE = DATA_DIR / "auth.json"
COOKIE_NAME = "u115_session"
SESSION_TTL = 7 * 24 * 3600
ITERATIONS = 200_000

DEFAULT_USER = "admin"
DEFAULT_PASSWORD = "admin"


def _hash(password: str, salt: bytes) -> str:
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt, ITERATIONS)
    return base64.b64encode(digest).decode()


class Auth:
    def __init__(self) -> None:
        self._data = None

    # ---------------------------------------------------------------- 读写

    def _load(self) -> dict:
        if self._data:
            return self._data
        if AUTH_FILE.exists():
            try:
                data = json.loads(AUTH_FILE.read_text(encoding="utf-8"))
                if isinstance(data, dict) and data.get("username") and data.get("hash"):
                    self._data = data
                    return data
            except Exception as err:
                print(f"[auth] 读取账号文件失败，将重置为默认账号：{err}")
        # 首次运行 / 文件损坏 → 建默认账号
        self._data = self._create(DEFAULT_USER, DEFAULT_PASSWORD)
        return self._data

    def _create(self, username: str, password: str) -> dict:
        salt = secrets.token_bytes(16)
        data = {
            "username": username,
            "salt": base64.b64encode(salt).decode(),
            "hash": _hash(password, salt),
            "iterations": ITERATIONS,
            "secret": secrets.token_hex(32),
            "updated_at": int(time.time()),
        }
        self._save(data)
        return data

    def _save(self, data: dict) -> None:
        DATA_DIR.mkdir(parents=True, exist_ok=True)
        tmp = AUTH_FILE.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp.replace(AUTH_FILE)
        try:
            AUTH_FILE.chmod(0o600)
        except Exception:
            pass
        self._data = data

    # ---------------------------------------------------------------- 校验

    def username(self) -> str:
        return str(self._load().get("username") or DEFAULT_USER)

    def verify(self, username: str, password: str) -> bool:
        data = self._load()
        if not username or username != data.get("username"):
            return False
        try:
            salt = base64.b64decode(data.get("salt") or "")
        except Exception:
            return False
        return hmac.compare_digest(_hash(password, salt), data.get("hash") or "")

    def change_password(self, old_password: str, new_password: str) -> Optional[str]:
        """
        返回 None 表示成功，否则返回错误说明
        """
        data = self._load()
        if not self.verify(data.get("username"), old_password):
            return "当前密码不正确"
        if not new_password or len(new_password) < 4:
            return "新密码至少 4 位"
        salt = secrets.token_bytes(16)
        data = dict(data)
        data["salt"] = base64.b64encode(salt).decode()
        data["hash"] = _hash(new_password, salt)
        data["iterations"] = ITERATIONS
        # 换掉会话密钥 → 其他设备上已登录的会话立刻失效
        data["secret"] = secrets.token_hex(32)
        data["updated_at"] = int(time.time())
        self._save(data)
        return None

    # ---------------------------------------------------------------- 会话

    def issue_token(self, username: str) -> str:
        data = self._load()
        expire = int(time.time()) + SESSION_TTL
        payload = f"{username}|{expire}"
        sign = hmac.new(
            bytes.fromhex(data["secret"]), payload.encode(), hashlib.sha256
        ).hexdigest()[:32]
        return base64.urlsafe_b64encode(f"{payload}|{sign}".encode()).decode()

    def check_token(self, token: Optional[str]) -> Optional[str]:
        if not token:
            return None
        data = self._load()
        try:
            raw = base64.urlsafe_b64decode(token.encode()).decode()
            username, expire_text, sign = raw.rsplit("|", 2)
            if int(expire_text) < int(time.time()):
                return None
            payload = f"{username}|{expire_text}"
            expect = hmac.new(
                bytes.fromhex(data["secret"]), payload.encode(), hashlib.sha256
            ).hexdigest()[:32]
            if not hmac.compare_digest(sign, expect):
                return None
            if username != data.get("username"):
                return None
            return username
        except Exception:
            return None
