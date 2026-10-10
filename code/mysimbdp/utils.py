
import logging
import json
import os
import sys
import time
from datetime import datetime, timezone

try:
    import pymongo
except ImportError:  # pragma: no cover
    pymongo = None


# Level label widths for alignment
_LEVEL_LABELS = {
    "DEBUG":    "DEBUG   ",
    "INFO":     "INFO    ",
    "WARNING":  "WARNING ",
    "ERROR":    "ERROR   ",
    "CRITICAL": "CRITICAL",
}

def get_mongo_client(mongo_uri: str) -> pymongo.MongoClient:
    if pymongo is None:
        raise RuntimeError("pymongo is not installed in the current environment")
    client = pymongo.MongoClient(
        mongo_uri,
        # Cap how long any single socket operation (e.g. insert_many) can block.
        # Without this, a saturated MongoDB primary blocks the caller indefinitely.
        socketTimeoutMS=15000,
        # Cap how long the driver will wait to find a usable server before raising.
        serverSelectionTimeoutMS=10000,
    )
    # Verify connectivity (will raise on failure)
    client.admin.command("ping")
    return client


class _PlainFormatter(logging.Formatter):
    """Human-readable single-line format for stdout.

    Example:
        16:15:11 INFO     producer/tenant_a  | Starting — 3 file(s) on topic tenant_a_airbnb
        16:15:12 INFO     producer/tenant_a  | calendar_bologna.csv.gz  rows_sent=4823
        16:15:13 WARNING  producer/tenant_a  | Delivery failed for offset 99
    """

    def __init__(self, component: str, tenant_id: str):
        super().__init__()
        self.prefix = f"{component}/{tenant_id}"

    def _format_metrics_extra(self, extra: dict) -> str:
        preferred_order = [
            "event_age_ms",
            "processing_latency_ms",
            "window_processing_time_ms",
            "tenant_id",
            "window",
            "group_value",
            "metric",
            "value",
            "unit",
            "stage",
            "normalized",
            "dropped",
            "received",
            "samples",
            "component",
            "topics",
            "partitions",
            "group_id",
        ]

        ordered_keys = [k for k in preferred_order if k in extra]
        ordered_keys.extend(k for k in extra.keys() if k not in ordered_keys)

        def _format_value(key: str, value) -> str:
            if value is None:
                return "None"

            if key in {"event_age_ms", "processing_latency_ms"}:
                try:
                    return f"{float(value):.3f}".ljust(16)
                except (TypeError, ValueError):
                    return str(value)

            if key == "window_processing_time_ms":
                try:
                    return f"{float(value):.3f}".ljust(8)
                except (TypeError, ValueError):
                    return str(value)

            if isinstance(value, float):
                return f"{value:.3f}"

            return str(value)

        return "  ".join(f"{k}={_format_value(k, extra.get(k))}" for k in ordered_keys)

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
        level = _LEVEL_LABELS.get(record.levelname, record.levelname.ljust(8))
        msg = record.getMessage()

        extra = getattr(record, "extra", None)
        if extra and isinstance(extra, dict):
            if record.name.startswith("metrics_"):
                kv = self._format_metrics_extra(extra)
            else:
                kv = "  ".join(f"{k}={v}" for k, v in extra.items())
            msg = f"{msg}  [{kv}]"

        return f"{ts} {level} {self.prefix:<30}| {msg}"


class _MetricsJsonFormatter(logging.Formatter):
    """Strict JSON-lines formatter for metrics logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload = getattr(record, "extra", None)
        if not isinstance(payload, dict):
            payload = {"metric": "log_message", "value": record.getMessage()}
        if "ts" not in payload:
            payload["ts"] = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        return json.dumps(payload, ensure_ascii=True)


def get_json_logger(
    component: str,
    tenant_id: str,
    log_dir: str = "logs/",
) -> logging.Logger:
    """Return a logger that writes JSON lines to a timestamped file AND stdout.

    Args:
        component: Component name (e.g., 'producer', 'streamingestworker').
        tenant_id: Tenant identifier (e.g., 'tenant_a').
        log_dir: Directory for log files (created if missing).

    Returns:
        A configured logging.Logger instance.
    """
    # Resolve log_dir relative to the repo root (Assignment-2 root, two levels above utils/)
    if not os.path.isabs(log_dir):
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(repo_root, log_dir)

    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    if component == "streamworker":
        file_stem = "streamworker"
    elif component == "producer":
        file_stem = "producer"
    else:
        file_stem = f"{component}_{tenant_id}"

    log_file = os.path.join(log_dir, f"{file_stem}_{ts}.log")

    logger_name = f"{component}_{tenant_id}"
    logger = logging.getLogger(logger_name)

    # Avoid duplicate handlers on re-import
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG)
    plain_formatter = _PlainFormatter(component, tenant_id)

    # File handler — plain text (same as stdout, easy to read with tail/cat)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(plain_formatter)
    logger.addHandler(fh)

    # Stdout handler — plain text
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.DEBUG)
    sh.setFormatter(plain_formatter)
    logger.addHandler(sh)

    return logger


def get_metrics_logger(
    component: str,
    tenant_id: str,
    log_dir: str = "logs/",
    stream: str | None = None,
) -> logging.Logger:
    """Return a dedicated metrics logger in the same plain text style.

    Metrics logs are isolated from operational logs while staying human-readable.
    """
    if not os.path.isabs(log_dir):
        repo_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        log_dir = os.path.join(repo_root, log_dir)

    os.makedirs(log_dir, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
    if component == "streamworker":
        if stream:
            file_stem = f"streamworker_metrics_{stream}"
        else:
            file_stem = "streamworker_metrics"
    else:
        if stream:
            file_stem = f"{component}_metrics_{tenant_id}_{stream}"
        else:
            file_stem = f"{component}_metrics_{tenant_id}"

    log_file = os.path.join(log_dir, f"{file_stem}_{ts}.log")

    if stream:
        logger_name = f"metrics_{component}_{tenant_id}_{stream}"
    else:
        logger_name = f"metrics_{component}_{tenant_id}"
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False

    plain_formatter = _PlainFormatter(component, tenant_id)
    fh = logging.FileHandler(log_file)
    fh.setLevel(logging.INFO)
    fh.setFormatter(plain_formatter)
    logger.addHandler(fh)

    return logger

def get_logger(name: str, tenant: str = None, log_dir: str = None) -> logging.Logger:
    """Standard logger for all mysimbdp components."""
    log_dir = log_dir or os.environ.get("LOG_DIR", "../logs")
    os.makedirs(log_dir, exist_ok=True)

    logger_name = f"{name}.{tenant}" if tenant else name
    logger = logging.getLogger(logger_name)

    if logger.handlers:
        return logger  # avoid duplicate handlers on re-init

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s")

    fh = logging.FileHandler(os.path.join(log_dir, f"{name}.log"))
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def get_pipeline_logger(tenant: str, log_dir: str = None):
    """JSON structured logger for silverpipeline scripts.
    Writes to logs/pipeline_{tenant}.log as newline-delimited JSON.
    """
    log_dir = log_dir or os.environ.get("LOG_DIR", "../logs")
    os.makedirs(log_dir, exist_ok=True)

    logger = logging.getLogger(f"pipeline.{tenant}")

    if logger.handlers:
        return logger  # avoid duplicate handlers on re-init

    logger.setLevel(logging.INFO)

    class JsonFormatter(logging.Formatter):
        def format(self, record):
            entry = {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "level": record.levelname,
                "tenant": tenant,
                "message": record.getMessage(),
            }
            if hasattr(record, "extra"):
                entry.update(record.extra)
            return json.dumps(entry)

    fmt = JsonFormatter()

    fh = logging.FileHandler(os.path.join(log_dir, f"pipeline_{tenant}.log"))
    fh.setFormatter(fmt)

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)

    logger.addHandler(fh)
    logger.addHandler(ch)
    return logger


def safe_json_dumps(payload: dict) -> str:
    return json.dumps(payload, default=str, ensure_ascii=True)


def parse_timestamp(value: str | datetime | None) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if isinstance(value, str) and value:
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            return None
    return None


def kafka_produce_at_least_once(
    producer,
    topic: str,
    value: bytes,
    key: bytes | None = None,
    retries: int = 5,
    retry_backoff_seconds: float = 0.1,
) -> None:
    last_error = None
    for _ in range(retries):
        try:
            producer.produce(topic=topic, key=key, value=value)
            producer.flush(5)
            return
        except BufferError as exc:
            last_error = exc
            producer.poll(0.1)
            time.sleep(retry_backoff_seconds)
        except Exception as exc:
            last_error = exc
            time.sleep(retry_backoff_seconds)

    raise RuntimeError(f"Kafka at-least-once delivery failed for topic={topic}: {last_error}")
