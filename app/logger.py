"""
日志：同时写文件（带轮转）和 stdout，方便 docker logs 与网页查看

注意：任何密钥（115 token / API Key / 企业微信 secret / 下载器密码）都不允许写进日志。
"""
import logging
import logging.handlers
import sys

from .config import DATA_DIR

LOG_DIR = DATA_DIR / "logs"
LOG_FILE = LOG_DIR / "app.log"
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUPS = 3

_root_ready = False


def setup_logging(level: int = logging.INFO) -> logging.Logger:
    """
    初始化日志（重复调用无副作用）
    """
    global _root_ready
    logger = logging.getLogger("u115")
    if _root_ready:
        return logger

    LOG_DIR.mkdir(parents=True, exist_ok=True)
    logger.setLevel(level)
    logger.propagate = False

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s", "%Y-%m-%d %H:%M:%S"
    )

    try:
        file_handler = logging.handlers.RotatingFileHandler(
            LOG_FILE,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUPS,
            encoding="utf-8",
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    except Exception as err:  # 磁盘只读之类的，至少不影响服务
        print(f"[logger] 日志文件不可写，仅输出到控制台：{err}")

    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    _root_ready = True
    return logger


def get_logger() -> logging.Logger:
    return logging.getLogger("u115")


def tail(lines: int = 200, keyword: str = "") -> str:
    """
    读日志末尾几行（keyword 非空时按关键字过滤）
    """
    try:
        if not LOG_FILE.exists():
            return ""
        with LOG_FILE.open("r", encoding="utf-8", errors="ignore") as handle:
            data = handle.readlines()
    except Exception as err:
        return f"读取日志失败：{err}"
    if keyword:
        data = [line for line in data if keyword.lower() in line.lower()]
    data = data[-max(1, lines):]
    return "".join(data)
