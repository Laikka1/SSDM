#!/usr/bin/env python3

import argparse
import os
import sys
import urllib.request

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


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the Trading Data Flink transport job")
    parser.add_argument("--broker", default="localhost:9092")
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


def main() -> None:
    args = parse_args()
    if args.parallelism < 1:
        raise SystemExit("--parallelism must be at least 1")
    if args.lateness_seconds < 0:
        raise SystemExit("--lateness-seconds must not be negative")

    logger = get_json_logger("streamworker", "trading")
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

    from streamworker_flink import run_flink_worker

    run_flink_worker(args, connector_jar, logger)


if __name__ == "__main__":
    main()