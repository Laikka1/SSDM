import json
import logging
import os
import sys
from datetime import datetime, timezone


class _PlainFormatter(logging.Formatter):
    def __init__(self, component: str, scope: str):
        super().__init__()
        self.prefix = f"{component}/{scope}"

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        message = record.getMessage()
        extra = getattr(record, "extra", None)
        if isinstance(extra, dict) and extra:
            fields = "  ".join(f"{key}={value}" for key, value in extra.items())
            message = f"{message}  [{fields}]"
        return f"{timestamp} {record.levelname:<8} {self.prefix:<24}| {message}"


def get_json_logger(component: str, scope: str, log_dir: str = "logs/") -> logging.Logger:
    """Create the shared human-readable logger used by pipeline processes."""
    if not os.path.isabs(log_dir):
        repo_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        log_dir = os.path.join(repo_root, log_dir)
    os.makedirs(log_dir, exist_ok=True)

    logger_name = f"{component}_{scope}"
    logger = logging.getLogger(logger_name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    formatter = _PlainFormatter(component, scope)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    file_path = os.path.join(log_dir, f"{component}_{timestamp}.log")

    file_handler = logging.FileHandler(file_path)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(formatter)
    logger.addHandler(stdout_handler)
    return logger


def safe_json_dumps(payload: dict) -> str:
    return json.dumps(payload, default=str, ensure_ascii=True)