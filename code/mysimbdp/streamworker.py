#!/usr/bin/env python3

import argparse
import json
import os
import sys
import threading
import time
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from mysimbdp.alerting import build_monitoring_event, publish_json
from mysimbdp.utils import get_json_logger, get_metrics_logger, safe_json_dumps


TENANT_RULES = {
    "tenant_a": {
        "group_key": "neighbourhood",
        "window_seconds": 5,
        "allowed_lateness_seconds": 3,
    },
}

E2E_SNAPSHOT_INTERVAL_SECONDS = 0.5
KAFKA_CONNECTOR_JAR_NAME = "flink-sql-connector-kafka-3.3.0-1.20.jar"
KAFKA_CONNECTOR_JAR_URL = (
    "https://repo1.maven.org/maven2/org/apache/flink/"
    "flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar"
)

_TOPIC_LOG_LOCK = threading.Lock()
_TOPIC_LOG_FILES: dict[str, str] = {}


def _repo_root_dir() -> str:
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def _topic_log_path(topic: str) -> str:
    # Keep filenames safe while preserving the topic name pattern.
    safe_topic = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "_" for ch in topic)
    return os.path.join(_repo_root_dir(), "logs", f"{safe_topic}.log")


def _output_topics_for_logging() -> list[str]:
    output_topics = [f"{tenant_id}_results" for tenant_id in TENANT_RULES]
    monitoring_topics = [f"{tenant_id}_monitoring" for tenant_id in TENANT_RULES]
    return output_topics + monitoring_topics


def _ensure_output_topic_log_files(logger=None) -> None:
    topic_files: dict[str, str] = {}
    for topic in sorted(set(_output_topics_for_logging())):
        log_path = _topic_log_path(topic)
        os.makedirs(os.path.dirname(log_path), exist_ok=True)
        with open(log_path, "a", encoding="utf-8"):
            pass
        topic_files[topic] = log_path

    with _TOPIC_LOG_LOCK:
        _TOPIC_LOG_FILES.clear()
        _TOPIC_LOG_FILES.update(topic_files)

    if logger is not None:
        logger.info(
            "TOPIC logs ready",
            extra={
                "extra": {
                    "topics": ",".join(sorted(topic_files.keys())),
                }
            },
        )


def _append_topic_log(topic: str, payload: dict, logger=None) -> None:
    try:
        line = safe_json_dumps(payload)
        with _TOPIC_LOG_LOCK:
            log_path = _TOPIC_LOG_FILES.get(topic)
            if not log_path:
                log_path = _topic_log_path(topic)
                os.makedirs(os.path.dirname(log_path), exist_ok=True)
                _TOPIC_LOG_FILES[topic] = log_path
                with open(log_path, "a", encoding="utf-8"):
                    pass

            with open(log_path, "a", encoding="utf-8") as handle:
                handle.write(f"{line}\n")
    except Exception as exc:
        if logger is not None:
            logger.warning(
                "TOPIC log write failed",
                extra={"extra": {"topic": topic, "error": str(exc)}},
            )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Stream worker orchestrator (single entrypoint)")
    parser.add_argument("--broker", default="localhost:9092", help="Kafka broker")
    parser.add_argument(
        "--consumer-group",
        default="streamworker_group",
        help="Kafka consumer group for the worker",
    )
    parser.add_argument(
        "--input-topics",
        default="tenant_a_events",
        help="Comma separated Kafka input topics",
    )
    parser.add_argument(
        "--starting-offsets",
        choices=["latest", "earliest"],
        default="latest",
        help="Kafka starting offsets policy for new runs",
    )
    parser.add_argument("--cassandra-hosts", default="localhost", help="Cassandra hosts list")
    parser.add_argument("--no-cassandra", action="store_true", help="Disable Cassandra writes")
    parser.add_argument(
        "--engine",
        choices=["flink"],
        default="flink",
        help="Execution engine mode (Flink only)",
    )
    return parser.parse_args()


def _parse_timestamp(value) -> datetime:
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc)
    if value:
        try:
            return datetime.fromisoformat(str(value).replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            pass
    return datetime.now(timezone.utc)


def normalize_event(record: dict) -> dict | None:
    tenant_id = str(record.get("tenant_id", "")).strip()
    if tenant_id not in TENANT_RULES:
        return None

    event_key = record.get("event_key") or record.get("id")
    if event_key is None:
        return None

    payload = record.get("payload") if isinstance(record.get("payload"), dict) else record
    rule = TENANT_RULES[tenant_id]

    group_value = (
        payload.get(rule["group_key"])
        or record.get("partition_key")
        or event_key
    )

    produced_at = None
    produced_at_raw = record.get("_produced_at")
    if produced_at_raw:
        try:
            produced_at = datetime.fromisoformat(str(produced_at_raw).replace("Z", "+00:00")).astimezone(timezone.utc)
        except ValueError:
            produced_at = None

    processing_latency_ms = _compute_latency_ms(produced_at, datetime.now(timezone.utc))

    return {
        "tenant_id": tenant_id,
        "event_key": str(event_key),
        "event_time": _parse_timestamp(record.get("event_time") or payload.get("last_review") or record.get("_produced_at")),
        "group_key": rule["group_key"],
        "group_value": str(group_value),
        "price": float(payload.get("price", 0) or 0),
        "processing_latency_ms": round(processing_latency_ms, 3) if processing_latency_ms is not None else None,
    }


def _window_bounds(ts: datetime, window_seconds: int) -> tuple[datetime, datetime]:
    epoch = int(ts.timestamp())
    start_epoch = epoch - (epoch % window_seconds)
    start = datetime.fromtimestamp(start_epoch, tz=timezone.utc)
    end = start + timedelta(seconds=window_seconds)
    return start, end


def _compute_latency_ms(start_time: datetime | None, end_time: datetime | None) -> float | None:
    if start_time is None or end_time is None:
        return None
    return max(0.0, (end_time - start_time).total_seconds() * 1000.0)


def emit_metric(metrics_logger, metric: str, value: float | int | None, **extra_fields) -> None:
    if metrics_logger is None or value is None:
        return

    payload = {
        "metric": metric,
        "value": value,
        "component": "streamworker",
    }
    payload.update(extra_fields)
    metrics_logger.info("METRIC", extra={"extra": payload})


class E2EMetricsTracker:
    """Tracks end-to-end event counters and produces 1-second snapshots."""

    def __init__(self) -> None:
        self._lock = threading.Lock()

        self.consumed_events_total = 0
        self.persisted_events_total = 0
        self.failed_events_total = 0

        self._window_persisted_counts: dict[str, int] = {}
        self._window_seen_counts: dict[str, int] = {}

        self._last_snapshot_ts = time.time()
        self._last_snapshot_consumed_total = 0
        self._last_snapshot_persisted_total = 0
        self._last_snapshot_failed_total = 0

    @staticmethod
    def _to_non_negative_int(value) -> int:
        try:
            return max(0, int(value))
        except (TypeError, ValueError):
            return 0

    @staticmethod
    def _window_key(result: dict) -> str:
        tenant_id = str(result.get("tenant_id", ""))
        group_key = str(result.get("group_key", ""))
        group_value = str(result.get("group_value", ""))
        window_start = str(result.get("window_start", ""))
        window_end = str(result.get("window_end", ""))
        return f"{tenant_id}|{group_key}|{group_value}|{window_start}|{window_end}"

    def record_consumed(self, count: int = 1) -> None:
        count = self._to_non_negative_int(count)
        if count <= 0:
            return
        with self._lock:
            self.consumed_events_total += count

    def record_event_persisted(self, count: int = 1) -> None:
        count = self._to_non_negative_int(count)
        if count <= 0:
            return
        with self._lock:
            self.persisted_events_total += count

    def record_event_failed(self, count: int = 1) -> None:
        count = self._to_non_negative_int(count)
        if count <= 0:
            return
        with self._lock:
            self.failed_events_total += count

    def record_persist_succeeded(self, result: dict) -> None:
        event_count = self._to_non_negative_int(result.get("event_count", 0))
        key = self._window_key(result)

        with self._lock:
            seen_before = self._window_seen_counts.get(key, 0)
            seen_delta = max(0, event_count - seen_before)
            if seen_delta > 0:
                self.consumed_events_total += seen_delta
                self._window_seen_counts[key] = event_count

            persisted_before = self._window_persisted_counts.get(key, 0)
            persisted_delta = max(0, event_count - persisted_before)
            if persisted_delta > 0:
                self.persisted_events_total += persisted_delta
                self._window_persisted_counts[key] = event_count

    def record_persist_failed(self, result: dict) -> None:
        event_count = self._to_non_negative_int(result.get("event_count", 0))
        key = self._window_key(result)

        with self._lock:
            seen_before = self._window_seen_counts.get(key, 0)
            seen_delta = max(0, event_count - seen_before)
            if seen_delta > 0:
                self.consumed_events_total += seen_delta
                self.failed_events_total += seen_delta
                self._window_seen_counts[key] = event_count

    def snapshot(self) -> dict:
        with self._lock:
            now = time.time()
            elapsed = max(0.001, now - self._last_snapshot_ts)
            consumed_delta = self.consumed_events_total - self._last_snapshot_consumed_total
            persisted_delta = self.persisted_events_total - self._last_snapshot_persisted_total
            failed_delta = self.failed_events_total - self._last_snapshot_failed_total
            had_activity = consumed_delta > 0 or persisted_delta > 0 or failed_delta > 0

            throughput_eps = persisted_delta / elapsed
            if self.consumed_events_total > 0:
                drop_rate = (
                    self.consumed_events_total - self.persisted_events_total
                ) / self.consumed_events_total
            else:
                drop_rate = 0.0

            payload = {
                "metric": "e2e_pipeline_snapshot",
                "counter_basis": "window_events",
                "snapshot_interval_seconds": E2E_SNAPSHOT_INTERVAL_SECONDS,
                "window_events_persisted_eps": round(throughput_eps, 3),
                "window_events_drop_rate": round(max(0.0, drop_rate), 6),
                "window_events_seen_total": self.consumed_events_total,
                "window_events_persisted_total": self.persisted_events_total,
                "window_events_failed_total": self.failed_events_total,
                "had_activity": had_activity,
            }

            self._last_snapshot_ts = now
            self._last_snapshot_consumed_total = self.consumed_events_total
            self._last_snapshot_persisted_total = self.persisted_events_total
            self._last_snapshot_failed_total = self.failed_events_total

        return payload


E2E_METRICS = E2EMetricsTracker()


def e2e_record_consumed(count: int = 1) -> None:
    E2E_METRICS.record_consumed(count)


def e2e_record_persisted(count: int = 1) -> None:
    E2E_METRICS.record_event_persisted(count)


def e2e_record_failed(count: int = 1) -> None:
    E2E_METRICS.record_event_failed(count)


def e2e_snapshot() -> dict:
    return E2E_METRICS.snapshot()


def aggregate_windowed_results(events: list[dict]) -> list[dict]:
    per_tenant: dict[str, list[dict]] = defaultdict(list)
    for event in events:
        per_tenant[event["tenant_id"]].append(event)

    results: list[dict] = []

    for tenant_id, tenant_events in per_tenant.items():
        rule = TENANT_RULES[tenant_id]
        max_event_time = max(event["event_time"] for event in tenant_events)
        watermark = max_event_time - timedelta(seconds=rule["allowed_lateness_seconds"])

        buckets: dict[tuple[datetime, datetime, str], list[dict]] = defaultdict(list)
        for event in tenant_events:
            if event["event_time"] < watermark:
                continue
            window_start, window_end = _window_bounds(event["event_time"], rule["window_seconds"])
            bucket_key = (window_start, window_end, event["group_value"])
            buckets[bucket_key].append(event)

        for (window_start, window_end, group_value), rows in buckets.items():
            bucket_compute_start = time.perf_counter()
            avg_price = sum(row["price"] for row in rows) / len(rows)
            row_processing_latencies = [
                float(row["processing_latency_ms"])
                for row in rows
                if row.get("processing_latency_ms") is not None
            ]
            processing_latency_ms = (
                sum(row_processing_latencies) / len(row_processing_latencies)
                if row_processing_latencies
                else None
            )
            window_processing_time_ms = (time.perf_counter() - bucket_compute_start) * 1000.0
            results.append(
                {
                    "tenant_id": tenant_id,
                    "group_key": rule["group_key"],
                    "group_value": group_value,
                    "window_start": window_start.isoformat(),
                    "window_end": window_end.isoformat(),
                    "event_count": len(rows),
                    "avg_price": avg_price,
                    "processing_latency_ms": round(processing_latency_ms, 3) if processing_latency_ms is not None else None,
                    "window_processing_time_ms": round(window_processing_time_ms, 3),
                    "result_type": "window_summary",
                    "emitted_at": datetime.now(timezone.utc).isoformat(),
                }
            )

    results.sort(key=lambda x: (x["tenant_id"], x["window_start"], x["group_value"]))
    return results


def emit_results(
    results: list[dict],
    producer,
    cassandra_writer,
    logger,
    window_metrics_logger=None,
) -> None:
    for result in results:
        tenant_id = result["tenant_id"]
        group_value = result.get("group_value")
        window_start = result.get("window_start")
        window_end = result.get("window_end")
        event_count = result.get("event_count")
        avg_price = result.get("avg_price")
        processing_latency_ms = result.get("processing_latency_ms")
        window_processing_time_ms = result.get("window_processing_time_ms")
        end_to_end_latency_ms = None

        if producer is not None:
            result_topic = f"{tenant_id}_results"
            publish_json(producer, result_topic, result, key=tenant_id)
            _append_topic_log(result_topic, result, logger=logger)
            logger.debug(
                "RESULT emitted",
                extra={
                    "extra": {
                        "tenant_id": tenant_id,
                        "topic": result_topic,
                        "window_start": window_start,
                        "window_end": window_end,
                        "event_count": event_count,
                        "group_value": group_value,
                        "avg_price": avg_price,
                        "processing_latency_ms": processing_latency_ms,
                        "window_processing_time_ms": window_processing_time_ms,
                    }
                },
            )

        if cassandra_writer is not None:
            try:
                persisted_at = cassandra_writer.insert_window_result(result)
                E2E_METRICS.record_persist_succeeded(result)
                logger.debug(
                    "PERSIST succeeded",
                    extra={
                        "extra": {
                            "tenant_id": tenant_id,
                            "table": "window_results",
                            "window_start": window_start,
                            "window_end": window_end,
                            "event_count": event_count,
                            "group_value": group_value,
                            "avg_price": avg_price,
                            "processing_latency_ms": processing_latency_ms,
                            "window_processing_time_ms": window_processing_time_ms,
                        }
                    },
                )

                end_to_end_latency_ms = _compute_latency_ms(_parse_timestamp(window_end), persisted_at)
                if end_to_end_latency_ms is not None:
                    logger.debug(
                        "PERF metric=end_to_end_latency_ms",
                        extra={
                            "extra": {
                                "tenant_id": tenant_id,
                                "group_value": group_value,
                                "window_end": window_end,
                                "persisted_at": persisted_at.isoformat(),
                                "latency_ms": round(end_to_end_latency_ms, 3),
                            }
                        },
                    )
            except Exception as exc:
                E2E_METRICS.record_persist_failed(result)
                logger.warning(
                    "PERSIST failed",
                    extra={
                        "extra": {
                            "error": str(exc),
                            "tenant_id": tenant_id,
                            "table": "window_results",
                            "window_start": window_start,
                            "window_end": window_end,
                            "group_value": group_value,
                        }
                    },
                )
        else:
            # With Cassandra disabled, track output-side throughput from emitted window summaries.
            E2E_METRICS.record_persist_succeeded(result)

        monitoring = build_monitoring_event(result)
        if window_metrics_logger is not None:
            window_metrics_logger.info(
                "METRIC",
                extra={
                    "extra": {
                        "event_age_ms": round(end_to_end_latency_ms, 3) if end_to_end_latency_ms is not None else None,
                        "processing_latency_ms": monitoring.get("processing_latency_ms"),
                        "window_processing_time_ms": monitoring.get("window_processing_time_ms"),
                        "tenant_id": tenant_id,
                        "window": f"{window_start}/{window_end}",
                        "group_value": group_value,
                    }
                },
            )
        if producer is not None:
            monitoring_topic = f"{tenant_id}_monitoring"
            publish_json(producer, monitoring_topic, monitoring, key=tenant_id)
            _append_topic_log(monitoring_topic, monitoring, logger=logger)
            logger.debug(
                f"METRIC tenant={tenant_id} metric={monitoring.get('metric_name')} value={monitoring.get('metric_value')} processing_latency_ms={monitoring.get('processing_latency_ms')} window_processing_time_ms={monitoring.get('window_processing_time_ms')} topic={monitoring_topic}",
                extra={
                    "extra": {
                        "tenant_id": tenant_id,
                        "topic": monitoring_topic,
                        "metric_name": monitoring.get("metric_name"),
                        "metric_value": monitoring.get("metric_value"),
                        "event_count": event_count,
                        "group_value": group_value,
                        "window_start": window_start,
                        "window_end": window_end,
                        "processing_latency_ms": monitoring.get("processing_latency_ms"),
                        "window_processing_time_ms": monitoring.get("window_processing_time_ms"),
                    }
                },
            )


def _ensure_kafka_connector_jar(logger) -> str:
    override_jar = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "").strip()
    if override_jar:
        if not os.path.isfile(override_jar):
            raise RuntimeError(
                f"FLINK_KAFKA_CONNECTOR_JAR is set but file does not exist: {override_jar}"
            )
        return os.path.abspath(override_jar)

    jars_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
    os.makedirs(jars_dir, exist_ok=True)
    jar_path = os.path.join(jars_dir, KAFKA_CONNECTOR_JAR_NAME)

    if os.path.isfile(jar_path):
        return jar_path

    logger.info(
        "KAFKA connector download started",
        extra={"extra": {"url": KAFKA_CONNECTOR_JAR_URL}},
    )

    tmp_path = f"{jar_path}.tmp"
    urllib.request.urlretrieve(KAFKA_CONNECTOR_JAR_URL, tmp_path)
    os.replace(tmp_path, jar_path)
    return jar_path


def _normalize_raw_json(raw_value: str) -> str:
    record = json.loads(raw_value)
    normalized = normalize_event(record)
    if normalized is None:
        return ""

    event_time = normalized.get("event_time")
    if isinstance(event_time, datetime):
        normalized["event_time"] = event_time.isoformat()
    return safe_json_dumps(normalized)


def _result_key(normalized_json: str) -> str:
    event = json.loads(normalized_json)
    return f"{event['tenant_id']}|{event['group_key']}|{event['group_value']}"


def _extract_event_time_millis(normalized_json: str) -> int:
    event = json.loads(normalized_json)
    ts = _parse_timestamp(event.get("event_time"))
    return int(ts.timestamp() * 1000)


def _ensure_kafka_topics(broker: str, topics: list[str], logger) -> None:
    unique_topics = sorted({topic.strip() for topic in topics if topic and topic.strip()})
    if not unique_topics:
        return

    admin = AdminClient({"bootstrap.servers": broker})
    metadata = admin.list_topics(timeout=10)
    existing_topics = set(metadata.topics.keys())

    missing_topics = [topic for topic in unique_topics if topic not in existing_topics]
    if not missing_topics:
        logger.info(
            "TOPIC ensure no-op",
            extra={"extra": {"topics": ",".join(unique_topics)}},
        )
        return

    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=1, replication_factor=1) for topic in missing_topics],
        request_timeout=15,
    )

    created = []
    for topic, future in futures.items():
        try:
            future.result()
            created.append(topic)
        except Exception as exc:
            error_text = str(exc)
            if "TOPIC_ALREADY_EXISTS" in error_text or "already exists" in error_text:
                continue
            raise RuntimeError(f"Failed to create Kafka topic '{topic}': {exc}") from exc

    if created:
        logger.info(
            "TOPIC created",
            extra={"extra": {"topics": ",".join(sorted(created))}},
        )


def _required_runtime_topics(input_topics: list[str]) -> list[str]:
    output_topics = [f"{tenant_id}_results" for tenant_id in TENANT_RULES]
    monitoring_topics = [f"{tenant_id}_monitoring" for tenant_id in TENANT_RULES]
    return input_topics + output_topics + monitoring_topics


def run_flink_worker(args, logger) -> None:
    from mysimbdp.streamworker_flink import run_flink_worker as _run_flink_worker

    _run_flink_worker(args, logger)


def run_worker(args, logger) -> None:
    run_flink_worker(args, logger)


def main() -> None:
    args = parse_args()
    logger = get_json_logger("streamworker", "shared")
    _ensure_output_topic_log_files(logger=logger)
    _ = get_metrics_logger("streamworker", "shared", stream="snapshot")
    run_worker(args, logger)


if __name__ == "__main__":
    main()
