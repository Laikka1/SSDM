#!/usr/bin/env python3

import json
import sys

from utils import safe_json_dumps


def run_flink_worker(args, connector_jar: str, logger) -> None:
    """Consume Trading Data records from Kafka in a transport-only Flink job."""
    try:
        from pyflink.common import Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
        from pyflink.datastream.functions import FilterFunction, MapFunction, SinkFunction
    except Exception as exc:
        logger.error("Flink runtime unavailable", extra={"extra": {"error": str(exc)}})
        raise RuntimeError("Install apache-flink in the Python 3.11 environment") from exc

    class VerifyRecord(MapFunction):
        def open(self, runtime_context):
            self.accepted = 0

        def map(self, value):
            try:
                record = json.loads(value)
                if not isinstance(record, dict) or "payload" not in record:
                    raise ValueError("record must contain a payload")
                verified = safe_json_dumps({
                    "event_key": record.get("event_key"),
                    "symbol": record.get("symbol"),
                    "payload": record.get("payload"),
                    "_source_file": record.get("_source_file"),
                    "_source_row": record.get("_source_row"),
                    "_produced_at": record.get("_produced_at"),
                })
                self.accepted += 1
                if self.accepted <= 5:
                    logger.info(
                        "Verified Trading Data record",
                        extra={
                            "extra": {
                                "event_key": record.get("event_key"),
                                "symbol": record.get("symbol"),
                                "source_file": record.get("_source_file"),
                                "source_row": record.get("_source_row"),
                            }
                        },
                    )
                return verified
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                logger.warning("Rejected Kafka record", extra={"extra": {"error": str(exc)}})
                return ""

    class NonEmpty(FilterFunction):
        def filter(self, value):
            return bool(value)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(max(1, args.parallelism))
    env.set_python_executable(sys.executable)
    env.add_jars(f"file://{connector_jar}")

    offsets = (
        KafkaOffsetsInitializer.earliest()
        if args.starting_offsets == "earliest"
        else KafkaOffsetsInitializer.latest()
    )
    topics = [topic.strip() for topic in args.input_topics.split(",") if topic.strip()]
    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.broker)
        .set_topics(*topics)
        .set_group_id(args.consumer_group)
        .set_starting_offsets(offsets)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    verified = (
        env.from_source(source, WatermarkStrategy.no_watermarks(), "trading-events-source")
        .map(VerifyRecord(), output_type=Types.STRING())
        .filter(NonEmpty())
    )
    verified.add_sink(
        SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink")
    ).name("transport-verification")

    logger.info(
        "Starting Flink transport job",
        extra={
            "extra": {
                "input_topics": ",".join(topics),
                "consumer_group": args.consumer_group,
                "starting_offsets": args.starting_offsets,
                "parallelism": args.parallelism,
                "connector_jar": connector_jar,
            }
        },
    )
    env.execute("trading-data-transport")