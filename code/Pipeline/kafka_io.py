"""Kafka topics and clients for replay, Flink, and result inspection."""

import argparse
from dataclasses import dataclass
import re

DEFAULT_PREFIX = "trading-events"
INPUT_PARTITIONS = 8
INPUT_CONFIG = {
    "cleanup.policy": "delete",
    "message.timestamp.type": "CreateTime",
    "message.timestamp.before.max.ms": str(2**63 - 1),
    "retention.ms": "-1",
    "retention.bytes": "-1",
}


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

    def outputs(self):
        return self.windows, self.advisories, self.diagnostics

    def partition_counts(self):
        return {self.input: INPUT_PARTITIONS, **dict.fromkeys(self.outputs(), 1)}


def provision(broker: str, topics: Topics):
    from confluent_kafka.admin import AdminClient, NewTopic

    admin = AdminClient({"bootstrap.servers": broker})
    existing = admin.list_topics(timeout=10).topics
    collisions = existing.keys() & topics.partition_counts().keys()
    if collisions:
        raise RuntimeError(f"choose a fresh topic prefix; topics already exist: {sorted(collisions)}")
    futures = admin.create_topics([
        NewTopic(name, count, 1, config=INPUT_CONFIG if name == topics.input else {"message.timestamp.type": "LogAppendTime"})
        for name, count in topics.partition_counts().items()
    ], operation_timeout=15, request_timeout=20)
    for future in futures.values():
        future.result()


def verify_empty_topics(broker: str, topics: Topics):
    from confluent_kafka import Consumer, TopicPartition
    from confluent_kafka.admin import AdminClient, ConfigResource

    config = {"bootstrap.servers": broker}
    admin = AdminClient(config)
    metadata = admin.list_topics(timeout=10).topics
    for name, count in topics.partition_counts().items():
        topic = metadata.get(name)
        if topic is None or topic.error or len(topic.partitions) != count:
            raise RuntimeError(f"provision {name} with {count} partitions before replay")
    resource = ConfigResource("topic", topics.input)
    actual = admin.describe_configs([resource], request_timeout=10)[resource].result()
    for key, expected in INPUT_CONFIG.items():
        if key not in actual or actual[key].value != expected:
            raise RuntimeError(f"{topics.input} must have {key}={expected}")
    consumer = Consumer({**config, "group.id": "replay-topic-check", "enable.auto.commit": False})
    try:
        for name, count in topics.partition_counts().items():
            for partition in range(count):
                _, high = consumer.get_watermark_offsets(TopicPartition(name, partition), timeout=5)
                if high:
                    raise RuntimeError(f"{name} contains prior records; use a fresh topic prefix")
    finally:
        consumer.close()


def create_producer(broker, prefix):
    from confluent_kafka import Producer

    return Producer({
        "bootstrap.servers": broker,
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


def replay_source(broker, topics, *, bounded=False, request_timeout_ms=None):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

    builder = (
        KafkaSource.builder()
        .set_bootstrap_servers(broker)
        .set_topics(topics.input)
        .set_group_id(topics.input)
        .set_starting_offsets(KafkaOffsetsInitializer.earliest())
        .set_property("partition.discovery.interval.ms", "-1")
        .set_property("allow.auto.create.topics", "false")
        .set_value_only_deserializer(SimpleStringSchema())
    )
    if bounded:
        builder.set_bounded(KafkaOffsetsInitializer.latest())
    if request_timeout_ms is not None:
        builder.set_property("request.timeout.ms", str(request_timeout_ms))
        builder.set_property("default.api.timeout.ms", str(max(60_000, request_timeout_ms + 5000)))
    return builder.build()


def result_sink(broker, topic, *, transactional=False, request_timeout_ms=None):
    from pyflink.common.serialization import SimpleStringSchema
    from pyflink.datastream.connectors.kafka import KafkaRecordSerializationSchema, KafkaSink

    builder = (
        KafkaSink.builder()
        .set_bootstrap_servers(broker)
        .set_record_serializer(
            KafkaRecordSerializationSchema.builder()
            .set_topic(topic)
            .set_value_serialization_schema(SimpleStringSchema())
            .build()
        )
        .set_property("enable.idempotence", "true")
        .set_property("delivery.timeout.ms", "15000")
        .set_property("request.timeout.ms", "10000")
        .set_property("linger.ms", "10")
    )
    if transactional:
        from pyflink.datastream.connectors.base import DeliveryGuarantee

        builder.set_delivery_guarantee(DeliveryGuarantee.EXACTLY_ONCE)
        builder.set_transactional_id_prefix(f"{topic}-")
        builder.set_property("transaction.timeout.ms", "900000")
    if request_timeout_ms is not None:
        builder.set_property("request.timeout.ms", str(request_timeout_ms))
        builder.set_property("delivery.timeout.ms", str(max(15_000, request_timeout_ms + 5000)))
    return builder.build()


def result_consumer(broker, group):
    from confluent_kafka import Consumer

    return Consumer({
        "bootstrap.servers": broker,
        "group.id": group,
        "enable.auto.commit": False,
        "allow.auto.create.topics": False,
        "isolation.level": "read_committed",
    })


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--topic-prefix", required=True)
    args = parser.parse_args()
    topics = Topics.from_prefix(args.topic_prefix)
    provision(args.broker, topics)
    print("Created " + ", ".join(topics.partition_counts()))


if __name__ == "__main__":
    main()
