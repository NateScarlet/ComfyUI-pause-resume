import logging
import os
from logging.handlers import RotatingFileHandler

LOG_FILENAME = "gateway.log"
_MAX_BYTES = 5 * 1024 * 1024
_BACKUP_COUNT = 3
_LOG_FORMAT = "[GATEWAY] %(asctime)s [%(levelname)s] %(message)s"
_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"


def configure_logging(log_dir: str, debug: bool = False) -> str:
    """把 root logger 配成 stderr + 数据目录下的滚动文件，返回日志文件路径。

    只在组合根调用一次；重复调用会替换旧 handler（force=True），避免重复落盘。
    """
    os.makedirs(log_dir, exist_ok=True)
    log_path = os.path.join(log_dir, LOG_FILENAME)

    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=_MAX_BYTES,
        backupCount=_BACKUP_COUNT,
        encoding="utf-8",
    )
    stream_handler = logging.StreamHandler()

    logging.basicConfig(
        level=logging.DEBUG if debug else logging.INFO,
        format=_LOG_FORMAT,
        datefmt=_DATE_FORMAT,
        handlers=[stream_handler, file_handler],
        force=True,
    )
    return log_path
