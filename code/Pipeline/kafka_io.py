"""Kafka topics and clients for replay, Flink, and result inspection."""

import argparse
from dataclasses import dataclass
from pathlib import Path
import re
import sys
import time

BROKER = "localhost:9092"
LOG_ROOT = Path(__file__).resolve().parents[1] / "logs"
DEFAULT_PREFIX = "trading-events"
INPUT_PARTITIONS = 8
INPUT_CONFIG = {
    "cleanup.policy": "delete",
    "message.timestamp.type": "CreateTime",
    "message.timestamp.before.max.ms": str(2**63 - 1),
    "retention.ms": "-1",
    "retention.bytes": "-1",
}
OUTPUT_CONFIG = {"message.timestamp.type": "LogAppendTime"}
TOPIC_SETUP_ATTEMPTS = 3


@dataclass(frozen=True)
class Topics:
    input: str
    windows: str
    advisories: str
    diagnostics: str

    @classmethod
    def from_prefix(cls, prefix: str):
        if not re.fullmatch(r"[a-zA-Z0-9_-]{1,200}", prefix):
            raise ValueError("topic prefix must contain 1–200 letters, digits, hyphens or underscores")
        return cls(prefix, f"{prefix}-windows", f"{prefix}-advisories", f"{prefix}-diagnostics")

    @property
    def log_dir(self):
        return LOG_ROOT / self.input

    def partition_counts(self):
        return {self.input: INPUT_PARTITIONS, self.windows: 1, self.advisories: 1, self.diagnostics: 1}


class TopicsNotReady(RuntimeError):
    pass


def provision(topics: Topics):
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import AdminClient, NewTopic

    transient_errors = {
        KafkaError.REQUEST_TIMED_OUT, KafkaError._TIMED_OUT, KafkaError._TRANSPORT,
        KafkaError._ALL_BROKERS_DOWN, KafkaError.NOT_CONTROLLER,
        KafkaError.BROKER_NOT_AVAILABLE, KafkaError.LEADER_NOT_AVAILABLE, KafkaError.UNKNOWN_TOPIC_OR_PART,
    }
    admin = AdminClient({"bootstrap.servers": BROKER})
    requested = [
        NewTopic(name, count, 1, config=INPUT_CONFIG if name == topics.input else OUTPUT_CONFIG)
        for name, count in topics.partition_counts().items()
    ]
    for attempt in range(1, TOPIC_SETUP_ATTEMPTS + 1):
        try:
            futures = admin.create_topics(requested, operation_timeout=15, request_timeout=20)
            for future in futures.values():
                try:
                    future.result()
                except KafkaException as exc:
                    if exc.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                        raise
            _verify_topics(admin, topics)
            return
        except (KafkaException, TopicsNotReady) as exc:
            if isinstance(exc, KafkaException) and exc.args[0].code() not in transient_errors:
                raise
            if attempt == TOPIC_SETUP_ATTEMPTS:
                raise
            print(f"Kafka topic setup attempt {attempt} failed ({exc}). Retrying.", file=sys.stderr)
            time.sleep(attempt)


def verify_topics(topics: Topics):
    from confluent_kafka.admin import AdminClient

    _verify_topics(AdminClient({"bootstrap.servers": BROKER}), topics)


def _verify_topics(admin, topics: Topics):
    from confluent_kafka import KafkaException
    from confluent_kafka.admin import ConfigResource

    metadata = admin.list_topics(timeout=10).topics
    for name, count in topics.partition_counts().items():
        topic = metadata.get(name)
        if topic is None:
            raise TopicsNotReady(f"Kafka topic {name} is missing")
        if topic.error:
            raise KafkaException(topic.error)
        if len(topic.partitions) != count:
            raise RuntimeError(f"Kafka topic {name} must have {count} partitions")
        if any(partition.leader < 0 for partition in topic.partitions.values()):
            raise TopicsNotReady(f"Kafka topic {name} is waiting for partition leaders")

    resources = [ConfigResource("topic", name) for name in topics.partition_counts()]
    for resource, future in admin.describe_configs(resources, request_timeout=10).items():
        actual = future.result()
        expected = INPUT_CONFIG if resource.name == topics.input else OUTPUT_CONFIG
        for key, value in expected.items():
            if key not in actual or actual[key].value != value:
                raise RuntimeError(f"Kafka topic {resource.name} must have {key}={value}")


def create_producer(prefix):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": BROKER,
        "client.id": f"replay-{prefix}",
        "enable.idempotence": True,
        "acks": "all",
        "allow.auto.create.topics": False,
        "delivery.timeout.ms": 15_000,
        "queue.buffering.max.messages": 16_000,
        "queue.buffering.max.kbytes": 16_384,
        "linger.ms": 10,
        "compression.type": "lz4",
    })


def enqueue(producer, delivery, **message):
    while True:
        delivery.check()
        try:
            producer.produce(**message)
            return
        except BufferError:
            producer.poll(0.05)


def replay_source(topics, *, request_timeout_ms=None):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(BROKER)
        .set_topics(topics.input)
        .set_group_id(topics.input)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_property("partition.discovery.interval.ms", "-1")
        .set_property("allow.auto.create.topics", "false")
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if request_timeout_ms is not None:
        builder.set_property("request.timeout.ms", str(request_timeout_ms))
        builder.set_property("default.api.timeout.ms", str(max(60_000, request_timeout_ms + 5000)))
    return builder.build()


def result_sink(topic, *, transactional=False, request_timeout_ms=None):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

    if request_timeout_ms is None:
        request_timeout_ms = 10_000
    delivery_timeout_ms = max(15_000, request_timeout_ms + 5_000)
    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(BROKER)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_property("enable.idempotence", "true")
        .set_property("delivery.timeout.ms", str(delivery_timeout_ms))
        .set_property("request.timeout.ms", str(request_timeout_ms))
        .set_property("linger.ms", "10")
    )
    if transactional:
        from pyflink.datastream.connectors.base import DeliveryGuarantee

        builder.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        builder.set_transactional_id_prefix(f"{topic}-")
        builder.set_property("transaction.timeout.ms", "900000")
    return builder.build()


def result_consumer():
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": BROKER,
        "group.id": "trading-inspector",
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
        "isolation.level": "read_committed",
    })


def main():
    from confluent_kafka import KafkaException

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic-prefix", required=True)
    args = parser.parse_args()
    try:
        topics = Topics.from_prefix(args.topic_prefix)
        provision(topics)
    except (ValueError, RuntimeError, KafkaException) as exc:
        raise SystemExit(f"Kafka topic setup failed for {args.topic_prefix}. {exc}") from None
    print("Kafka topics ready " + ", ".join(topics.partition_counts()))


if __name__ == "__main__":
    main()
