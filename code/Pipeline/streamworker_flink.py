#!/usr/bin/env python3

import json
import sys
from datetime import datetime, timezone

from utils import safe_json_dumps


def run_flink_worker(args, connector_jar: str, logger) -> None:
    """Run normalization, validation, event-time windows, and Kafka boundaries."""
    try:
        from pyflink.common import Duration, Time, Types, WatermarkStrategy
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import OutputTag, StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
        from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.datastream.functions import (
            ProcessFunction,
            ProcessWindowFunction,
            RuntimeContext,
        )
        from pyflink.datastream.window import TumblingEventTimeWindows
    except Exception as exc:
        logger.error("Flink runtime unavailable", extra={"extra": {"error": str(exc)}})
        raise RuntimeError("Install apache-flink in the Python 3.11 environment") from exc

    from normalization import normalize_envelope
    from trading_events import NormalizedEvent, Rejection
    from validation import to_dead_letter, validate_event

    invalid_tag = OutputTag("invalid-events", Types.STRING())
    analytical_tag = OutputTag("analytical-events", Types.STRING())
    housekeeping_tag = OutputTag("housekeeping-events", Types.STRING())

    class NormalizeAndValidate(ProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            group = runtime_context.get_metrics_group().add_group("trading")
            self._metric_group = group
            self.received = group.counter("records_received")
            self.normalized = group.counter("records_normalized")
            self.valid = group.counter("records_valid")
            self.rejected = group.counter("records_rejected")
            self.missing_price = group.counter("records_missing_or_invalid_price")
            self.reason_counters = {}
            self.event_type_counters = {}
            self.exchange_counters = {}

        def _counter(self, counters, group, prefix, value):
            name = f"{prefix}_{value.lower().replace('-', '_')}"
            if name not in counters:
                counters[name] = group.counter(name)
            counters[name].inc()

        def process_element(self, value, ctx):
            self.received.inc()
            try:
                record = json.loads(value)
                normalized = normalize_envelope(record)
                if not isinstance(normalized, NormalizedEvent):
                    self.rejected.inc()
                    self._counter(self.reason_counters, self._metric_group, "records_rejected_reason", normalized.reason_code)
                    if normalized.reason_code in {
                        "invalid_numeric_value",
                        "invalid_price_value",
                        "missing_last_price",
                    }:
                        self.missing_price.inc()
                    yield invalid_tag, safe_json_dumps(
                        to_dead_letter(record, normalized).as_dict()
                    )
                    return
                self.normalized.inc()
                rejection = validate_event(normalized)
                if rejection is not None:
                    self.rejected.inc()
                    self._counter(self.reason_counters, self._metric_group, "records_rejected_reason", rejection.reason_code)
                    if rejection.reason_code == "missing_last_price":
                        self.missing_price.inc()
                    yield invalid_tag, safe_json_dumps(
                        to_dead_letter(record, rejection).as_dict()
                    )
                    return
                self.valid.inc()
                self._counter(self.event_type_counters, self._metric_group, "records_event_type", normalized.event_type)
                self._counter(self.exchange_counters, self._metric_group, "records_exchange", normalized.exchange)
                if normalized.last_price is None:
                    self.missing_price.inc()
                serialized = safe_json_dumps(normalized.as_dict())
                yield serialized
                if normalized.event_type in {"trade", "quote"}:
                    yield analytical_tag, serialized
                else:
                    yield housekeeping_tag, serialized
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self.rejected.inc()
                self._counter(self.reason_counters, self._metric_group, "records_rejected_reason", "malformed_json")
                yield invalid_tag, safe_json_dumps(
                    to_dead_letter(
                        record if "record" in locals() and isinstance(record, dict) else {},
                        Rejection("malformed_json", str(exc)),
                    ).as_dict()
                )

    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            event_time = json.loads(value)["event_time"]
            return int(datetime.fromisoformat(event_time).timestamp() * 1000)

    class WindowResult(ProcessWindowFunction):
        def process(self, key, context, elements):
            from windowing import build_window_result
            records = [json.loads(item) for item in elements]
            typed = [
                NormalizedEvent(
                    event_id=item["event_id"],
                    symbol=item["symbol"],
                    exchange=item["exchange"],
                    security_type=item["security_type"],
                    trading_date=item["trading_date"],
                    event_time=datetime.fromisoformat(item["event_time"]),
                    last_price=item["last_price"],
                    event_type=item["event_type"],
                    available_attributes=tuple(item["available_attributes"]),
                    source_file=item["source_file"],
                    source_row=item["source_row"],
                    produced_at=item["produced_at"],
                    original_payload=item["payload"],
                    original_envelope={},
                )
                for item in records
            ]
            result = build_window_result(typed)
            result["processed_at"] = datetime.now(timezone.utc).isoformat()
            yield safe_json_dumps(result)

    def kafka_sink(topic):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(args.broker)
            .set_record_serializer(
                KafkaRecordSerializationSchema.builder()
                .set_topic(topic)
                .set_value_serialization_schema(SimpleStringSchema())
                .build()
            )
            .build()
        )

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

    parsed = env.from_source(
        source, WatermarkStrategy.no_watermarks(), "trading-events-source"
    )
    classified = parsed.process(
        NormalizeAndValidate(), output_type=Types.STRING()
    )
    invalid = classified.get_side_output(invalid_tag)
    invalid.sink_to(kafka_sink(args.dead_letter_topic)).name("dead-letter-output")
    classified.sink_to(kafka_sink(args.normalized_topic)).name("normalized-output")
    classified.get_side_output(housekeeping_tag).sink_to(
        kafka_sink(args.housekeeping_topic)
    ).name("housekeeping-output")
    analytical = classified.get_side_output(analytical_tag)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(args.lateness_seconds)
        )
        .with_timestamp_assigner(EventTimestampAssigner())
    )
    timed = analytical.assign_timestamps_and_watermarks(watermark_strategy)
    late_tag = OutputTag("late-events", Types.STRING())
    windows = (
        timed.key_by(lambda value: json.loads(value)["symbol"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .allowed_lateness(0)
        .side_output_late_data(late_tag)
        .process(WindowResult(), output_type=Types.STRING())
    )
    windows.sink_to(kafka_sink(args.window_topic)).name("five-minute-windows")
    windows.get_side_output(late_tag).map(
        lambda value: safe_json_dumps({
            "reason_code": "late_event",
            "details": "event arrived after the configured lateness bound",
            "event": json.loads(value),
            "rejected_at": datetime.now(timezone.utc).isoformat(),
        }),
        output_type=Types.STRING(),
    ).sink_to(kafka_sink(args.late_topic)).name("late-event-output")

    logger.info(
        "Starting Flink event preparation job",
        extra={
            "extra": {
                "input_topics": ",".join(topics),
                "consumer_group": args.consumer_group,
                "starting_offsets": args.starting_offsets,
                "parallelism": args.parallelism,
                "connector_jar": connector_jar,
                "dead_letter_topic": args.dead_letter_topic,
                "late_topic": args.late_topic,
                "window_topic": args.window_topic,
                "normalized_topic": args.normalized_topic,
                "housekeeping_topic": args.housekeeping_topic,
                "lateness_seconds": args.lateness_seconds,
            }
        },
    )
    env.execute("trading-event-preparation")