#!/usr/bin/env python3

import argparse
import json
import os
import sys
import threading
import time
import urllib.request
import uuid
from datetime import datetime, timezone

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import Consumer, TopicPartition

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import get_json_logger


DEFAULT_TOPIC = "trading-events"
CONNECTOR_JAR_NAME = "flink-sql-connector-kafka-3.3.0-1.20.jar"
CONNECTOR_JAR_URL = (
    "https://repo1.maven.org/maven2/org/apache/flink/"
    "flink-sql-connector-kafka/3.3.0-1.20/"
    "flink-sql-connector-kafka-3.3.0-1.20.jar"
)
HEARTBEAT_INTERVAL_SECONDS = 15
DEFAULT_PROGRESS_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "logs", "streamworker_progress.jsonl"
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Trading Data Flink transport job")
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument(
        "--flink-broker",
        default=os.getenv("FLINK_BROKER", "localhost:9092"),
        help="Kafka address reachable by the Flink runtime (defaults to localhost:9092)",
    )
    parser.add_argument("--input-topics", default=DEFAULT_TOPIC)
    parser.add_argument("--consumer-group", default="trading-flink-transport")
    parser.add_argument("--starting-offsets", choices=["latest", "earliest"], default="latest")
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--engine", choices=["flink"], default="flink")
    parser.add_argument("--dead-letter-topic", default="trading-events-dead-letter")
    parser.add_argument("--late-topic", default="trading-events-late")
    parser.add_argument("--window-topic", default="trading-events-windows")
    parser.add_argument("--all-window-topic", default="trading-events-windows-all")
    parser.add_argument("--advisory-topic", default="trading-events-advisories")
    parser.add_argument("--subscribed-symbols", default=None)
    parser.add_argument("--subscriptions-file", default=None)
    parser.add_argument("--normalized-topic", default="trading-events-normalized")
    parser.add_argument("--housekeeping-topic", default="trading-events-housekeeping")
    parser.add_argument("--lateness-seconds", type=int, default=300)
    parser.add_argument("--progress-file", default=DEFAULT_PROGRESS_FILE)
    return parser.parse_args()


def ensure_topics(broker: str, topics: list[str], logger) -> None:
    names = sorted({topic.strip() for topic in topics if topic.strip()})
    admin = AdminClient({"bootstrap.servers": broker})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [topic for topic in names if topic not in existing]
    if not missing:
        logger.info("Kafka topics ready", extra={"extra": {"topics": ",".join(names)}})
        return

    futures = admin.create_topics(
        [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in missing],
        request_timeout=15,
    )
    for topic, future in futures.items():
        try:
            future.result()
        except Exception as exc:
            if "already exists" not in str(exc).lower():
                raise RuntimeError(f"Could not create Kafka topic {topic}: {exc}") from exc
    logger.info("Kafka topics created", extra={"extra": {"topics": ",".join(missing)}})


def ensure_connector_jar(logger) -> str:
    override = os.getenv("FLINK_KAFKA_CONNECTOR_JAR", "").strip()
    if override:
        if not os.path.isfile(override):
            raise RuntimeError(f"FLINK_KAFKA_CONNECTOR_JAR does not exist: {override}")
        return os.path.abspath(override)

    jar_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jars")
    os.makedirs(jar_dir, exist_ok=True)
    jar_path = os.path.join(jar_dir, CONNECTOR_JAR_NAME)
    if not os.path.isfile(jar_path):
        logger.info("Downloading Flink Kafka connector", extra={"extra": {"url": CONNECTOR_JAR_URL}})
        temporary_path = f"{jar_path}.tmp"
        urllib.request.urlretrieve(CONNECTOR_JAR_URL, temporary_path)
        os.replace(temporary_path, jar_path)
    return jar_path


def start_heartbeat(logger) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    started_at = time.monotonic()

    def emit_heartbeat() -> None:
        while not stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
            logger.info(
                "Streamworker is still running",
                extra={
                    "extra": {
                        "elapsed_seconds": int(time.monotonic() - started_at),
                    }
                },
            )

    heartbeat = threading.Thread(
        target=emit_heartbeat,
        name="streamworker-heartbeat",
        daemon=True,
    )
    heartbeat.start()
    return stop_event, heartbeat


def start_progress_reporter(
    logger,
    progress_file: str,
    run_id: str,
    broker: str,
    consumer_group: str,
    input_topics: list[str],
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    progress_path = os.path.abspath(progress_file)
    latest = {}

    def emit_progress() -> None:
        last_position = 0
        kafka_consumer = Consumer({
            "bootstrap.servers": broker,
            "group.id": consumer_group,
            "enable.auto.commit": False,
        })

        while not stop_event.wait(HEARTBEAT_INTERVAL_SECONDS):
            snapshots = []
            try:
                with open(progress_path, encoding="utf-8") as file:
                    file.seek(last_position)
                    for line in file:
                        try:
                            snapshot = json.loads(line)
                        except json.JSONDecodeError:
                            continue
                        if snapshot.get("run_id") == run_id:
                            snapshots.append(snapshot)
                    last_position = file.tell()
            except FileNotFoundError:
                snapshots = []

            for snapshot in snapshots:
                latest[(snapshot["stage"], snapshot["subtask"])] = snapshot
            totals = {
                "records_received": 0,
                "records_normalized": 0,
                "records_valid": 0,
                "records_rejected": 0,
                "analytical_records": 0,
                "windows_emitted": 0,
                "late_records": 0,
            }
            for snapshot in latest.values():
                for key in (
                    "records_received",
                    "records_normalized",
                    "records_valid",
                    "records_rejected",
                    "analytical_records",
                    "windows_emitted",
                    "late_records",
                ):
                    totals[key] = totals.get(key, 0) + snapshot.get(key, 0)
            watermarks = [
                snapshot["current_watermark"]
                for snapshot in latest.values()
                if snapshot.get("current_watermark") is not None
            ]
            extra = dict(totals)
            extra["current_watermark"] = min(watermarks) if watermarks else "not_advanced"
            records_per_second = [
                snapshot["records_per_second"]
                for snapshot in latest.values()
                if snapshot.get("records_per_second") is not None
            ]
            windows_per_second = [
                snapshot["windows_per_second"]
                for snapshot in latest.values()
                if snapshot.get("windows_per_second") is not None
            ]
            watermark_lags = [
                snapshot["watermark_lag_ms"]
                for snapshot in latest.values()
                if snapshot.get("watermark_lag_ms") is not None
            ]
            average_latencies = [
                snapshot["average_processing_latency_ms"]
                for snapshot in latest.values()
                if snapshot.get("average_processing_latency_ms") is not None
            ]
            max_latencies = [
                snapshot["max_processing_latency_ms"]
                for snapshot in latest.values()
                if snapshot.get("max_processing_latency_ms") is not None
            ]
            p95_latencies = [
                snapshot["p95_processing_latency_ms"]
                for snapshot in latest.values()
                if snapshot.get("p95_processing_latency_ms") is not None
            ]
            if records_per_second:
                extra["records_per_second"] = round(max(records_per_second), 3)
            if windows_per_second:
                extra["windows_per_second"] = round(max(windows_per_second), 3)
            if watermark_lags:
                extra["watermark_lag_ms"] = max(watermark_lags)
            if average_latencies:
                extra["average_processing_latency_ms"] = round(max(average_latencies), 3)
            if max_latencies:
                extra["max_processing_latency_ms"] = round(max(max_latencies), 3)
            if p95_latencies:
                extra["p95_processing_latency_ms"] = round(max(p95_latencies), 3)
            kafka_lag = 0
            lag_available = False
            try:
                for topic in input_topics:
                    metadata = kafka_consumer.list_topics(topic=topic, timeout=5)
                    topic_metadata = metadata.topics.get(topic)
                    if topic_metadata is None:
                        continue
                    partitions = [
                        TopicPartition(topic, partition)
                        for partition in topic_metadata.partitions
                    ]
                    if not partitions:
                        continue
                    committed = kafka_consumer.committed(partitions, timeout=5)
                    for partition in committed:
                        if partition.offset < 0:
                            low, high = kafka_consumer.get_watermark_offsets(
                                partition, timeout=5
                            )
                            kafka_lag += max(0, high)
                            lag_available = True
                            continue
                        low, high = kafka_consumer.get_watermark_offsets(
                            partition, timeout=5
                        )
                        kafka_lag += max(0, high - partition.offset)
                        lag_available = True
            except Exception as exc:
                extra["kafka_lag_error"] = type(exc).__name__
            if lag_available:
                extra["kafka_consumer_lag"] = kafka_lag
            else:
                extra["kafka_consumer_lag"] = "not_available"
            logger.info("Streamworker progress", extra={"extra": extra})
            lag_log = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "logs",
                "lag_metrics.log",
            )
            os.makedirs(os.path.dirname(lag_log), exist_ok=True)
            lag_snapshot = {
                "kind": "progress_lag",
                "recorded_at": datetime.now(timezone.utc).isoformat(),
                "run_id": run_id,
                **extra,
            }
            with open(lag_log, "a", encoding="utf-8") as file:
                file.write(safe_json_dumps(lag_snapshot) + "\n")
        kafka_consumer.close()

    reporter = threading.Thread(
        target=emit_progress,
        name="streamworker-progress",
        daemon=True,
    )
    reporter.start()
    return stop_event, reporter


def main() -> None:
    args = parse_args()
    if args.parallelism < 1:
        raise SystemExit("--parallelism must be at least 1")
    if args.lateness_seconds < 0:
        raise SystemExit("--lateness-seconds must not be negative")

    logger = get_json_logger("streamworker", "trading")
    run_id = uuid.uuid4().hex
    progress_file = os.path.abspath(args.progress_file)
    os.makedirs(os.path.dirname(progress_file), exist_ok=True)
    with open(progress_file, "w", encoding="utf-8"):
        pass
    topics = [topic.strip() for topic in args.input_topics.split(",") if topic.strip()]
    if not topics:
        raise SystemExit("--input-topics must contain at least one topic")
    from subscriptions import load_subscriptions

    try:
        args.subscribed_symbols = load_subscriptions(
            args.subscribed_symbols, args.subscriptions_file
        )
    except ValueError as exc:
        raise SystemExit(str(exc)) from exc
    ensure_topics(
        args.broker,
        topics + [
            args.dead_letter_topic,
            args.late_topic,
            args.window_topic,
            args.all_window_topic,
            args.advisory_topic,
            args.normalized_topic,
            args.housekeeping_topic,
        ],
        logger,
    )
    connector_jar = ensure_connector_jar(logger)
    from ema import prepare_ema_log, prepare_lag_log, prepare_output_logs

    ema_log = prepare_ema_log()
    subscribed_ema_log, advisory_log = prepare_output_logs()
    lag_log = prepare_lag_log()
    logger.info(
        "Streamworker configuration ready",
        extra={
            "extra": {
                "broker": args.broker,
                "flink_broker": args.flink_broker,
                "input_topics": ",".join(topics),
                "consumer_group": args.consumer_group,
                "starting_offsets": args.starting_offsets,
                "parallelism": args.parallelism,
                "lateness_seconds": args.lateness_seconds,
                "connector_jar": connector_jar,
                "ema_log": ema_log,
                "subscribed_ema_log": subscribed_ema_log,
                "advisory_log": advisory_log,
                "lag_log": lag_log,
                "progress_file": progress_file,
                "subscriptions": (
                    "unfiltered"
                    if args.subscribed_symbols is None
                    else ",".join(sorted(args.subscribed_symbols))
                ),
            }
        },
    )

    from streamworker_flink import run_flink_worker

    logger.info("Starting Flink normalization, windowing, and EMA stages")
    heartbeat_stop, heartbeat = start_heartbeat(logger)
    progress_stop, progress_reporter = start_progress_reporter(
        logger,
        progress_file,
        run_id,
        args.broker,
        args.consumer_group,
        topics,
    )
    try:
        args.run_id = run_id
        run_flink_worker(args, connector_jar, logger, progress_file, run_id)
    finally:
        heartbeat_stop.set()
        progress_stop.set()
        heartbeat.join(timeout=HEARTBEAT_INTERVAL_SECONDS)
        progress_reporter.join(timeout=HEARTBEAT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()