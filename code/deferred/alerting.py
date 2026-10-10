from datetime import datetime, timezone
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysimbdp.utils import kafka_produce_at_least_once, safe_json_dumps


def build_monitoring_event(result: dict) -> dict:
    return {
        "tenant_id": result.get("tenant_id"),
        "metric_name": "window_event_count",
        "metric_value": float(result.get("event_count", 0)),
        "window_start": result.get("window_start"),
        "window_end": result.get("window_end"),
        "processing_latency_ms": result.get("processing_latency_ms"),
        "window_processing_time_ms": result.get("window_processing_time_ms"),
        "emitted_at": datetime.now(timezone.utc).isoformat(),
    }


def publish_json(producer, topic: str, payload: dict, key: str | None = None) -> None:
    kafka_produce_at_least_once(
        producer=producer,
        topic=topic,
        value=safe_json_dumps(payload).encode("utf-8"),
        key=key.encode("utf-8") if key else None,
    )
