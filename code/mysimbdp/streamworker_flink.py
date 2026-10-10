#!/usr/bin/env python3

import json
import sys
import threading
import time
from datetime import datetime, timezone

from mysimbdp.cassandra_insert import CassandraInsert
from mysimbdp.streamworker import (
    E2E_SNAPSHOT_INTERVAL_SECONDS,
    TENANT_RULES,
    _ensure_kafka_connector_jar,
    _ensure_kafka_topics,
    _extract_event_time_millis,
    _normalize_raw_json,
    _required_runtime_topics,
    _result_key,
    e2e_snapshot,
    emit_results,
)
from mysimbdp.utils import get_json_logger, get_metrics_logger, safe_json_dumps


def run_flink_worker(args, logger) -> None:
    try:
        from pyflink.common import Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.common.time import Duration, Time
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.datastream import StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import (
            KafkaOffsetsInitializer,
            KafkaSource,
        )
        from pyflink.datastream.functions import (
            FilterFunction,
            MapFunction,
            ProcessWindowFunction,
            SinkFunction,
        )
        from pyflink.datastream.window import TumblingEventTimeWindows
    except Exception as exc:
        logger.error(
            "ENGINE flink unavailable",
            extra={"extra": {"error": str(exc)}},
        )
        raise RuntimeError(
            "PyFlink runtime not available. Install apache-flink and run with Python 3.11."
        ) from exc

    kafka_connector_jar = _ensure_kafka_connector_jar(logger)

    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars(f"file://{kafka_connector_jar}")
    # Ensure Flink workers use the same interpreter as the launcher process.
    env.set_python_executable(sys.executable)

    class NormalizeMapFunction(MapFunction):
        def __init__(self):
            self.logger = None

        def open(self, runtime_context):
            self.logger = get_json_logger("streamworker", "shared")

        def map(self, value):
            try:
                normalized = _normalize_raw_json(value)
            except Exception as exc:
                self.logger.warning(
                    "NORMALIZE skipped payload",
                    extra={"extra": {"error": str(exc)}},
                )
                return ""

            if not normalized:
                return ""
            return normalized

    class NonEmptyFilter(FilterFunction):
        def filter(self, value):
            return bool(value)

    class JsonTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            return _extract_event_time_millis(value)

    class ResultWindowFunction(ProcessWindowFunction):
        def process(self, key, context, elements):
            window_compute_start = time.perf_counter()
            rows = [json.loads(item) for item in elements]
            if not rows:
                return []

            tenant_id = rows[0]["tenant_id"]
            rule = TENANT_RULES.get(tenant_id)
            if rule is None:
                return []

            avg_price = sum(float(row.get("price", 0.0)) for row in rows) / len(rows)
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
            window_start = datetime.fromtimestamp(context.window().start / 1000, tz=timezone.utc)
            window_end = datetime.fromtimestamp(context.window().end / 1000, tz=timezone.utc)
            window_processing_time_ms = (time.perf_counter() - window_compute_start) * 1000.0

            result = {
                "tenant_id": tenant_id,
                "group_key": rows[0]["group_key"],
                "group_value": rows[0]["group_value"],
                "window_start": window_start.isoformat(),
                "window_end": window_end.isoformat(),
                "event_count": len(rows),
                "avg_price": avg_price,
                "processing_latency_ms": round(processing_latency_ms, 3) if processing_latency_ms is not None else None,
                "window_processing_time_ms": round(window_processing_time_ms, 3),
                "result_type": "window_summary",
                "emitted_at": datetime.now(timezone.utc).isoformat(),
            }
            return [safe_json_dumps(result)]

    class EmitResultsMapFunction(MapFunction):
        def __init__(
            self,
            broker: str,
            cassandra_hosts: str,
            no_cassandra: bool,
            topics: list[str],
            group_id: str,
        ):
            self.broker = broker
            self.cassandra_hosts = cassandra_hosts
            self.no_cassandra = no_cassandra
            self.topics = topics
            self.group_id = group_id
            self.producer = None
            self.cassandra_writer = None
            self.logger = None
            self.window_metrics_logger = None
            self.snapshot_metrics_logger = None
            self.snapshot_stop_event = None
            self.snapshot_thread = None

        def _snapshot_loop(self):
            while not self.snapshot_stop_event.is_set():
                payload = e2e_snapshot()
                had_activity = bool(payload.pop("had_activity", False))
                if had_activity:
                    self.snapshot_metrics_logger.info("METRIC", extra={"extra": payload})
                self.snapshot_stop_event.wait(E2E_SNAPSHOT_INTERVAL_SECONDS)

        def open(self, runtime_context):
            from confluent_kafka import Producer

            self.logger = get_json_logger("streamworker", "shared")
            self.snapshot_metrics_logger = get_metrics_logger(
                "streamworker",
                "shared",
                stream="snapshot",
            )
            self.producer = Producer({"bootstrap.servers": self.broker, "client.id": "streamworker"})
            if not self.no_cassandra:
                self.cassandra_writer = CassandraInsert(hosts=self.cassandra_hosts, logger=self.logger)

            self.snapshot_stop_event = threading.Event()
            self.snapshot_thread = threading.Thread(
                target=self._snapshot_loop,
                name="e2e-snapshot-heartbeat",
                daemon=True,
            )
            self.snapshot_thread.start()

        def map(self, value):
            result = json.loads(value)
            emit_results(
                [result],
                producer=self.producer,
                cassandra_writer=self.cassandra_writer,
                logger=self.logger,
                window_metrics_logger=self.window_metrics_logger,
            )
            return value

        def close(self):
            if self.snapshot_stop_event is not None:
                self.snapshot_stop_event.set()
            if self.snapshot_thread is not None:
                self.snapshot_thread.join(timeout=2)
            if self.producer is not None:
                self.producer.flush(5)
            if self.cassandra_writer is not None:
                self.cassandra_writer.close()

    topics = [topic.strip() for topic in args.input_topics.split(",") if topic.strip()]
    if not topics:
        raise RuntimeError("No Kafka input topics configured. Set --input-topics.")
    _ensure_kafka_topics(args.broker, _required_runtime_topics(topics), logger)

    if args.starting_offsets == "earliest":
        offsets_initializer = KafkaOffsetsInitializer.earliest()
    else:
        offsets_initializer = KafkaOffsetsInitializer.latest()

    source = (
        KafkaSource.builder()
        .set_bootstrap_servers(args.broker)
        .set_topics(*topics)
        .set_group_id(args.consumer_group)
        .set_starting_offsets(offsets_initializer)
        .set_value_only_deserializer(SimpleStringSchema())
        .build()
    )

    raw_stream = env.from_source(source, WatermarkStrategy.no_watermarks(), "kafka-input")
    normalized_stream = (
        raw_stream
        .map(NormalizeMapFunction(), output_type=Types.STRING())
        .filter(NonEmptyFilter())
    )

    if not TENANT_RULES:
        raise RuntimeError("TENANT_RULES is empty. Configure at least one tenant rule.")
    default_rule = next(iter(TENANT_RULES.values()))
    window_seconds = max(1, int(default_rule.get("window_seconds", 60)))
    allowed_lateness_seconds = max(0, int(default_rule.get("allowed_lateness_seconds", 60)))

    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(
        Duration.of_seconds(allowed_lateness_seconds)
    ).with_timestamp_assigner(JsonTimestampAssigner())

    windowed_results = (
        normalized_stream
        .assign_timestamps_and_watermarks(watermark_strategy)
        .key_by(lambda item: _result_key(item), key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.seconds(window_seconds)))
        .allowed_lateness(allowed_lateness_seconds * 1000)
        .process(ResultWindowFunction(), output_type=Types.STRING())
    )

    (
        windowed_results
        .map(
            EmitResultsMapFunction(
                broker=args.broker,
                cassandra_hosts=args.cassandra_hosts,
                no_cassandra=args.no_cassandra,
                topics=topics,
                group_id=args.consumer_group,
            ),
            output_type=Types.STRING(),
        )
        .add_sink(SinkFunction("org.apache.flink.streaming.api.functions.sink.DiscardingSink"))
    )

    logger.info(
        "ENGINE flink start",
        extra={
            "extra": {
                "topics": ",".join(topics),
                "kafka_jar": kafka_connector_jar,
                "python_executable": sys.executable,
                "consumer_group": args.consumer_group,
                "starting_offsets": args.starting_offsets,
                "window_seconds": window_seconds,
                "allowed_lateness_seconds": allowed_lateness_seconds,
                "e2e_snapshot_interval_seconds": E2E_SNAPSHOT_INTERVAL_SECONDS,
            }
        },
    )
    env.execute("streamworker-flink")


if __name__ == "__main__":
    raise SystemExit(
        "Use code/mysimbdp/streamworker.py --engine flink to run the Flink worker task."
    )
