#!/usr/bin/env python3

import json
import os
import sys
import time
from datetime import datetime, timezone

from utils import safe_json_dumps


def run_flink_worker(
    args, connector_jar: str, logger, progress_file: str, run_id: str
) -> None:
    """Run normalization, validation, event-time windows, and Kafka boundaries."""
    try:
        from pyflink.common import Duration, Time, Types, WatermarkStrategy
        from pyflink.common.restart_strategy import RestartStrategies
        from pyflink.common.serialization import SimpleStringSchema
        from pyflink.datastream import OutputTag, StreamExecutionEnvironment
        from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
        from pyflink.datastream.connectors.kafka import KafkaSink, KafkaRecordSerializationSchema
        from pyflink.common.watermark_strategy import TimestampAssigner
        from pyflink.datastream.functions import (
            KeyedProcessFunction,
            ProcessFunction,
            ProcessWindowFunction,
            RuntimeContext,
        )
        from pyflink.datastream.window import TumblingEventTimeWindows
    except Exception as exc:
        logger.error(
            "Flink runtime unavailable",
            extra={"extra": {"error_type": type(exc).__name__, "error": str(exc)}},
        )
        raise RuntimeError("Install apache-flink in the Python 3.11 environment") from exc

    from normalization import normalize_envelope
    from ema import create_ema_calculation
    from trading_events import NormalizedEvent, Rejection
    from validation import to_dead_letter, validate_event

    invalid_tag = OutputTag("invalid-events", Types.STRING())
    analytical_tag = OutputTag("analytical-events", Types.STRING())
    housekeeping_tag = OutputTag("housekeeping-events", Types.STRING())
    advisory_tag = OutputTag("subscribed-advisories", Types.STRING())

    def write_progress(stage, subtask, **values):
        snapshot = {
            "run_id": run_id,
            "stage": stage,
            "subtask": subtask,
            **values,
        }
        with open(progress_file, "a", encoding="utf-8") as file:
            file.write(safe_json_dumps(snapshot) + "\n")
            file.flush()

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
            self.received_count = 0
            self.normalized_count = 0
            self.valid_count = 0
            self.rejected_count = 0
            self.last_progress_log = time.monotonic()
            self.subtask = runtime_context.get_index_of_this_subtask()

        def _counter(self, counters, group, prefix, value):
            name = f"{prefix}_{value.lower().replace('-', '_')}"
            if name not in counters:
                counters[name] = group.counter(name)
            counters[name].inc()

        def process_element(self, value, ctx):
            self.received.inc()
            self.received_count += 1
            try:
                record = json.loads(value)
                normalized = normalize_envelope(record)
                if not isinstance(normalized, NormalizedEvent):
                    self.rejected.inc()
                    self.rejected_count += 1
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
                self.normalized_count += 1
                rejection = validate_event(normalized)
                if rejection is not None:
                    self.rejected.inc()
                    self.rejected_count += 1
                    self._counter(self.reason_counters, self._metric_group, "records_rejected_reason", rejection.reason_code)
                    if rejection.reason_code == "missing_last_price":
                        self.missing_price.inc()
                    yield invalid_tag, safe_json_dumps(
                        to_dead_letter(record, rejection).as_dict()
                    )
                    return
                self.valid.inc()
                self.valid_count += 1
                self._counter(self.event_type_counters, self._metric_group, "records_event_type", normalized.event_type)
                self._counter(self.exchange_counters, self._metric_group, "records_exchange", normalized.exchange)
                if normalized.last_price is None:
                    self.missing_price.inc()
                normalized_payload = normalized.as_dict()
                normalized_payload["normalized_at"] = datetime.now(timezone.utc).isoformat()
                serialized = safe_json_dumps(normalized_payload)
                yield serialized
            except (TypeError, ValueError, json.JSONDecodeError) as exc:
                self.rejected.inc()
                self.rejected_count += 1
                self._counter(self.reason_counters, self._metric_group, "records_rejected_reason", "malformed_json")
                yield invalid_tag, safe_json_dumps(
                    to_dead_letter(
                        record if "record" in locals() and isinstance(record, dict) else {},
                        Rejection("malformed_json", str(exc)),
                    ).as_dict()
                )

            finally:
                if time.monotonic() - self.last_progress_log >= 15:
                    logger.info(
                        "Normalization progress",
                        extra={
                            "extra": {
                                "records_received": self.received_count,
                                "records_normalized": self.normalized_count,
                                "records_valid": self.valid_count,
                                "records_rejected": self.rejected_count,
                            }
                        }
                    )
                    write_progress(
                        "normalization",
                        self.subtask,
                        records_received=self.received_count,
                        records_normalized=self.normalized_count,
                        records_valid=self.valid_count,
                        records_rejected=self.rejected_count,
                    )
                    self.last_progress_log = time.monotonic()

    class RouteValidEvents(KeyedProcessFunction):
        def process_element(self, value, ctx):
            yield value
            event_type = json.loads(value)["event_type"]
            if event_type in {"trade", "quote"}:
                yield analytical_tag, value
            else:
                yield housekeeping_tag, value

    class EventTimestampAssigner(TimestampAssigner):
        def extract_timestamp(self, value, record_timestamp):
            from windowing import window_timestamp

            record = json.loads(value)
            event_time = datetime.fromisoformat(record["event_time"])
            shifted = window_timestamp(event_time, record["exchange"])
            return int(shifted.timestamp() * 1000)

    class WindowResult(ProcessWindowFunction):
        def open(self, runtime_context):
            self.windows_emitted = 0
            self.last_progress_log = time.monotonic()
            self.started_at = time.monotonic()
            self.subtask = runtime_context.get_index_of_this_subtask()
            self.latency_values = []
            log_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "logs",
            )
            os.makedirs(log_dir, exist_ok=True)
            self.lag_log = open(
                os.path.join(log_dir, "lag_metrics.log"),
                "a",
                encoding="utf-8",
            )

        def close(self):
            self.lag_log.close()

        def process(self, key, context, elements):
            from windowing import build_window_result
            window_started_at = datetime.now(timezone.utc)
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
            emitted_at = datetime.now(timezone.utc)
            result["window_started_at"] = window_started_at.isoformat()
            result["processed_at"] = emitted_at.isoformat()
            result["emitted_at"] = emitted_at.isoformat()
            produced_times = [
                item.get("produced_at")
                for item in records
                if item.get("produced_at")
            ]
            normalized_times = [
                item.get("normalized_at")
                for item in records
                if item.get("normalized_at")
            ]
            from ema import (
                calculate_latency_percentiles,
                calculate_processing_latency_ms,
                calculate_stage_latency_ms,
            )
            replay_backlog = calculate_processing_latency_ms(
                max(produced_times, default=None), emitted_at
            )
            result["replay_backlog_latency_ms"] = replay_backlog
            result["processing_latency_ms"] = calculate_stage_latency_ms(
                window_started_at, emitted_at
            )
            result["window_processing_latency_ms"] = result["processing_latency_ms"]
            result["ema_processing_latency_ms"] = None
            result["end_to_end_latency_ms"] = result["processing_latency_ms"]
            self.latency_values.append(result["processing_latency_ms"])
            self.latency_values = self.latency_values[-1000:]
            replay_backlog_values = getattr(self, "replay_backlog_values", [])
            if replay_backlog is not None:
                replay_backlog_values.append(replay_backlog)
            self.replay_backlog_values = replay_backlog_values[-1000:]
            self.windows_emitted += 1
            self.lag_log.write(
                safe_json_dumps(
                    {
                        "kind": "window_latency",
                        "recorded_at": emitted_at.isoformat(),
                        "symbol": result["symbol"],
                        "window_start": result["window_start"],
                        "window_end": result["window_end"],
                        "processing_latency_ms": result["processing_latency_ms"],
                        "replay_backlog_latency_ms": result["replay_backlog_latency_ms"],
                        "window_processing_latency_ms": result["window_processing_latency_ms"],
                        "run_id": getattr(args, "run_id", None),
                        "windows_emitted": self.windows_emitted,
                        "windows_per_second": round(
                            self.windows_emitted / max(time.monotonic() - self.started_at, 0.001),
                            3,
                        ),
                    }
                )
                + "\n"
            )
            self.lag_log.flush()
            if time.monotonic() - self.last_progress_log >= 15:
                latency_summary = calculate_latency_percentiles(self.latency_values)
                replay_summary = calculate_latency_percentiles(
                    getattr(self, "replay_backlog_values", [])
                )
                logger.info(
                    "Window progress",
                    extra={
                        "extra": {
                            "windows_emitted": self.windows_emitted,
                            "windows_per_second": round(
                                self.windows_emitted
                                / max(time.monotonic() - self.started_at, 0.001),
                                3,
                            ),
                            "average_processing_latency_ms": latency_summary["average_ms"],
                            "p95_processing_latency_ms": latency_summary["p95_ms"],
                            "p99_processing_latency_ms": latency_summary["p99_ms"],
                            "max_processing_latency_ms": latency_summary["max_ms"],
                            "average_replay_backlog_latency_ms": replay_summary["average_ms"],
                            "p95_replay_backlog_latency_ms": replay_summary["p95_ms"],
                            "max_replay_backlog_latency_ms": replay_summary["max_ms"],
                        }
                    },
                )
                write_progress(
                    "windows",
                    self.subtask,
                    windows_emitted=self.windows_emitted,
                    windows_per_second=(
                        round(
                            self.windows_emitted
                            / max(time.monotonic() - self.started_at, 0.001),
                            3,
                        )
                    ),
                    average_processing_latency_ms=latency_summary["average_ms"],
                    p95_processing_latency_ms=latency_summary["p95_ms"],
                    p99_processing_latency_ms=latency_summary["p99_ms"],
                    max_processing_latency_ms=latency_summary["max_ms"],
                    average_replay_backlog_latency_ms=replay_summary["average_ms"],
                    p95_replay_backlog_latency_ms=replay_summary["p95_ms"],
                    max_replay_backlog_latency_ms=replay_summary["max_ms"],
                )
                self.last_progress_log = time.monotonic()
            yield safe_json_dumps(result)

    class AnalyticalProgress(ProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            self.processed = 0
            self.started_at = time.monotonic()
            self.latest_event_timestamp = None
            self.last_progress_log = time.monotonic()
            self.subtask = runtime_context.get_index_of_this_subtask()

        def process_element(self, value, ctx):
            self.processed += 1
            event_timestamp = ctx.timestamp()
            if event_timestamp is not None:
                self.latest_event_timestamp = (
                    event_timestamp
                    if self.latest_event_timestamp is None
                    else max(self.latest_event_timestamp, event_timestamp)
                )
            if time.monotonic() - self.last_progress_log >= 15:
                watermark = ctx.timer_service().current_watermark()
                watermark_lag_ms = (
                    max(0, self.latest_event_timestamp - watermark)
                    if self.latest_event_timestamp is not None
                    else None
                )
                logger.info(
                    "Analytical progress",
                    extra={
                        "extra": {
                            "records_processed": self.processed,
                            "records_per_second": round(
                                self.processed
                                / max(time.monotonic() - self.started_at, 0.001),
                                3,
                            ),
                            "current_watermark_ms": watermark,
                            "watermark_lag_ms": watermark_lag_ms,
                            "current_watermark": (
                                datetime.fromtimestamp(
                                    watermark / 1000, timezone.utc
                                ).isoformat()
                                if watermark >= 0
                                else "not_advanced"
                            ),
                        }
                    },
                )
                write_progress(
                    "analytical",
                    self.subtask,
                    analytical_records=self.processed,
                    records_per_second=round(
                        self.processed
                        / max(time.monotonic() - self.started_at, 0.001),
                        3,
                    ),
                    watermark_lag_ms=watermark_lag_ms,
                    current_watermark=(
                        datetime.fromtimestamp(
                            watermark / 1000, timezone.utc
                        ).isoformat()
                        if watermark >= 0
                        else None
                    ),
                )
                self.last_progress_log = time.monotonic()
            yield value

    class LateProgress(ProcessFunction):
        def open(self, runtime_context: RuntimeContext):
            self.processed = 0
            self.last_progress_log = time.monotonic()
            self.subtask = runtime_context.get_index_of_this_subtask()

        def process_element(self, value, ctx):
            self.processed += 1
            if time.monotonic() - self.last_progress_log >= 15:
                write_progress(
                    "late",
                    self.subtask,
                    late_records=self.processed,
                )
                self.last_progress_log = time.monotonic()
            yield value

    def kafka_sink(topic):
        return (
            KafkaSink.builder()
            .set_bootstrap_servers(args.flink_broker)
            .set_property("acks", "all")
            .set_property("retries", "10")
            .set_property("retry.backoff.ms", "1000")
            .set_property("reconnect.backoff.ms", "1000")
            .set_property("request.timeout.ms", "30000")
            .set_property("delivery.timeout.ms", "120000")
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
    env.set_restart_strategy(RestartStrategies.fixed_delay_restart(3, 10_000))
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
        .set_bootstrap_servers(args.flink_broker)
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
    keyed = classified.key_by(
        lambda value: json.loads(value)["symbol"],
        key_type=Types.STRING(),
    )
    routed = keyed.process(RouteValidEvents(), output_type=Types.STRING())
    routed.sink_to(kafka_sink(args.normalized_topic)).name("normalized-output")

    routed.get_side_output(housekeeping_tag).sink_to(
        kafka_sink(args.housekeeping_topic)
    ).name("housekeeping-output")
    analytical = routed.get_side_output(analytical_tag)
    watermark_strategy = (
        WatermarkStrategy.for_bounded_out_of_orderness(
            Duration.of_seconds(args.lateness_seconds)
        )
        .with_timestamp_assigner(EventTimestampAssigner())
    )
    timed = analytical.assign_timestamps_and_watermarks(watermark_strategy)
    timed = timed.process(
        AnalyticalProgress(), output_type=Types.STRING()
    )
    late_tag = OutputTag("late-events", Types.STRING())
    windows = (
        timed.key_by(lambda value: json.loads(value)["symbol"], key_type=Types.STRING())
        .window(TumblingEventTimeWindows.of(Time.minutes(5)))
        .allowed_lateness(0)
        .side_output_late_data(late_tag)
        .process(WindowResult(), output_type=Types.STRING())
    )
    ema_results = (
        windows.key_by(
            lambda value: json.loads(value)["symbol"],
            key_type=Types.STRING(),
        )
        .process(create_ema_calculation(), output_type=Types.STRING())
    )
    ema_results.sink_to(kafka_sink(args.all_window_topic)).name(
        "all-symbol-five-minute-ema-results"
    )

    class SubscriptionOutput(ProcessFunction):
        def open(self, runtime_context):
            log_dir = os.path.join(
                os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                "logs",
            )
            os.makedirs(log_dir, exist_ok=True)
            self.subscribed_log = open(
                os.path.join(log_dir, "EMA_subscribed.log"),
                "a",
                encoding="utf-8",
            )
            self.advisory_log = open(
                os.path.join(log_dir, "advisories.log"),
                "a",
                encoding="utf-8",
            )
            self.lag_log = open(
                os.path.join(log_dir, "lag_metrics.log"),
                "a",
                encoding="utf-8",
            )

        def close(self):
            self.subscribed_log.close()
            self.advisory_log.close()
            self.lag_log.close()

        def process_element(self, value, ctx):
            result = json.loads(value)
            subscribed = (
                args.subscribed_symbols is None
                or result.get("symbol") in args.subscribed_symbols
            )
            if subscribed:
                subscribed_at = datetime.now(timezone.utc)
                stage_latency = sum(
                    value
                    for value in (
                        result.get("window_processing_latency_ms"),
                        result.get("ema_processing_latency_ms"),
                    )
                    if isinstance(value, (int, float))
                )
                self.subscribed_log.write(
                    safe_json_dumps(
                        {
                            "event": "subscribed_ema",
                            "logged_at": subscribed_at.isoformat(),
                            "symbol": result.get("symbol"),
                            "window_start": result.get("window_start"),
                            "window_end": result.get("window_end"),
                            "close": result.get("close"),
                            "ema38": result.get("ema38"),
                            "ema100": result.get("ema100"),
                            "ema_status": result.get("ema_status"),
                            "processing_latency_ms": result.get("processing_latency_ms"),
                            "window_processing_latency_ms": result.get("window_processing_latency_ms"),
                            "ema_processing_latency_ms": result.get("ema_processing_latency_ms"),
                            "replay_backlog_latency_ms": result.get("replay_backlog_latency_ms"),
                            "end_to_end_latency_ms": stage_latency,
                            "run_id": getattr(args, "run_id", None),
                            "emitted_at": subscribed_at.isoformat(),
                        }
                    )
                    + "\n"
                )
                self.subscribed_log.flush()
                yield value
                if result.get("advice") is not None:
                    advisory_at = datetime.now(timezone.utc)
                    self.advisory_log.write(
                        safe_json_dumps(
                            {
                                "event": "crossover_advisory",
                                "logged_at": advisory_at.isoformat(),
                                "symbol": result.get("symbol"),
                                "window_start": result.get("window_start"),
                                "window_end": result.get("window_end"),
                                "advice": result.get("advice"),
                                "reason": result.get("reason"),
                                "close": result.get("close"),
                                "previous_ema38": result.get("previous_ema38"),
                                "previous_ema100": result.get("previous_ema100"),
                                "ema38": result.get("ema38"),
                                "ema100": result.get("ema100"),
                                "processing_latency_ms": result.get("processing_latency_ms"),
                                "window_processing_latency_ms": result.get("window_processing_latency_ms"),
                                "ema_processing_latency_ms": result.get("ema_processing_latency_ms"),
                                "replay_backlog_latency_ms": result.get("replay_backlog_latency_ms"),
                                "end_to_end_latency_ms": stage_latency,
                                "run_id": getattr(args, "run_id", None),
                                "emitted_at": advisory_at.isoformat(),
                            }
                        )
                        + "\n"
                    )
                    self.advisory_log.flush()
                    self.lag_log.write(
                        safe_json_dumps(
                            {
                                "kind": "advisory_latency",
                                "recorded_at": advisory_at.isoformat(),
                                "symbol": result.get("symbol"),
                                "advice": result.get("advice"),
                                "window_start": result.get("window_start"),
                                "window_end": result.get("window_end"),
                                "processing_latency_ms": result.get("processing_latency_ms"),
                                "window_processing_latency_ms": result.get("window_processing_latency_ms"),
                                "ema_processing_latency_ms": result.get("ema_processing_latency_ms"),
                                "replay_backlog_latency_ms": result.get("replay_backlog_latency_ms"),
                                "end_to_end_latency_ms": stage_latency,
                                "run_id": getattr(args, "run_id", None),
                            }
                        )
                        + "\n"
                    )
                    self.lag_log.flush()
                    yield advisory_tag, value

    subscribed_results = ema_results.process(
        SubscriptionOutput(), output_type=Types.STRING()
    )
    subscribed_results.sink_to(kafka_sink(args.window_topic)).name(
        "subscribed-five-minute-ema-results"
    )
    subscribed_results.get_side_output(advisory_tag).sink_to(
        kafka_sink(args.advisory_topic)
    ).name("subscribed-crossover-advisories")
    windows.get_side_output(late_tag).process(
        LateProgress(), output_type=Types.STRING()
    ).map(
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
                "broker": args.flink_broker,
                "starting_offsets": args.starting_offsets,
                "parallelism": args.parallelism,
                "connector_jar": connector_jar,
                "dead_letter_topic": args.dead_letter_topic,
                "late_topic": args.late_topic,
                "window_topic": args.window_topic,
                "all_window_topic": args.all_window_topic,
                "advisory_topic": args.advisory_topic,
                "normalized_topic": args.normalized_topic,
                "housekeeping_topic": args.housekeeping_topic,
                "lateness_seconds": args.lateness_seconds,
            }
        },
    )
    logger.info(
        "Flink stages wired",
        extra={
            "extra": {
                "stages": (
                    "source,normalize,validate,symbol-route,"
                    "event-time-window,ema38-ema100"
                ),
                "outputs": (
                    f"dead-letter={args.dead_letter_topic},"
                    f"normalized={args.normalized_topic},"
                    f"housekeeping={args.housekeeping_topic},"
                    f"late={args.late_topic},"
                    f"all-windows={args.all_window_topic},"
                    f"subscribed-windows={args.window_topic},"
                    f"advisories={args.advisory_topic}"
                ),
            }
        },
    )
    logger.info("Submitting Flink job")
    try:
        env.execute("trading-event-preparation")
    except Exception as exc:
        root = exc
        while root.__cause__ is not None:
            root = root.__cause__
        logger.error(
            "Flink job failed",
            extra={
                "extra": {
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                    "root_error_type": type(root).__name__,
                    "root_error": str(root),
                }
            },
        )
        raise