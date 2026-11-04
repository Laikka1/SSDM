#!/usr/bin/env python3

import argparse
import json
import os
import sys
import threading
import time
import urllib.request
import uuid

from confluent_kafka.admin import AdminClient, NewTopic

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
    logger, progress_file: str, run_id: str
) -> tuple[threading.Event, threading.Thread]:
    stop_event = threading.Event()
    progress_path = os.path.abspath(progress_file)
    latest = {}

    def emit_progress() -> None:
        last_position = 0
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
            logger.info("Streamworker progress", extra={"extra": extra})

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
    ensure_topics(
        args.broker,
        topics + [
            args.dead_letter_topic,
            args.late_topic,
            args.window_topic,
            args.normalized_topic,
            args.housekeeping_topic,
        ],
        logger,
    )
    connector_jar = ensure_connector_jar(logger)
    from ema import prepare_ema_log

    ema_log = prepare_ema_log()
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
                "progress_file": progress_file,
            }
        },
    )

    from streamworker_flink import run_flink_worker

    logger.info("Starting Flink normalization, windowing, and EMA stages")
    heartbeat_stop, heartbeat = start_heartbeat(logger)
    progress_stop, progress_reporter = start_progress_reporter(
        logger, progress_file, run_id
    )
    try:
        run_flink_worker(args, connector_jar, logger, progress_file, run_id)
    finally:
        heartbeat_stop.set()
        progress_stop.set()
        heartbeat.join(timeout=HEARTBEAT_INTERVAL_SECONDS)
        progress_reporter.join(timeout=HEARTBEAT_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()