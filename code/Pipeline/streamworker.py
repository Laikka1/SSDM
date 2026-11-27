#!/usr/bin/env python3
"""Submit and run the two trading queries in PyFlink."""

import argparse
from contextlib import ExitStack
from functools import partial
import json
import math
import os
from pathlib import Path
import signal
import subprocess
import sys
from tempfile import TemporaryDirectory

import pyflink
from pyflink.common import Configuration, Duration, Row, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import CheckpointingMode, DataStream, OutputTag, StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor
from pyflink.java_gateway import get_gateway

from inspect_run import LOGGED_COUNTERS, METRICS_PORT, REST_PORT, WINDOW_OPERATOR, ResultLogs, monitor
from kafka_io import BROKER, DEFAULT_PREFIX, INPUT_PARTITIONS, Topics, replay_source, result_sink
from normalization import (
    HOUSEKEEPING, MARKET_ZONES, PRICE, QUOTE, REJECTED, WINDOW_MS,
    classify_row, parse_subscriptions, window_end_ms,
)

BUNDLED_CONNECTOR = Path(__file__).with_name("jars") / "flink-sql-connector-kafka-3.3.0-1.20.jar"
DEFAULT_LATENESS_SECONDS = 1.0

DIAGNOSTIC_CAP = 20
DIAGNOSTIC_FIELDS = {
    "symbol", "exchange", "sec_type", "last", "ask", "bid", "trade_us", "trade_clock",
    "source_file", "source_index", "source_row", "window_end_ms", "watermark_ms",
    "capture_date", "capture_time", "trading_date", "trading_time",
}

PRICE_TYPE = Types.ROW_NAMED(
    ["symbol", "trade_us", "source_index", "source_row", "close", "source_file"],
    [Types.STRING(), Types.LONG(), Types.INT(), Types.LONG(), Types.DOUBLE(), Types.STRING()],
)
CANDIDATE_TYPE = Types.TUPLE([
    Types.LONG(), Types.INT(), Types.LONG(), Types.DOUBLE(), Types.STRING(),
])
EMA_TYPE = Types.TUPLE([Types.DOUBLE(), Types.DOUBLE()])
DIAGNOSTICS = OutputTag("diagnostics", Types.STRING())
ADVISORIES = OutputTag("advisories", Types.STRING())


class Counters:
    def __init__(self, context, names):
        metrics = context.get_metrics_group()
        group = metrics.add_group("trading")
        self.counts = dict.fromkeys(names, 0)
        counters = {name: group.counter(name) for name in names}
        reported = {}

        def flush_counts():
            for name, total in self.counts.items():
                if total != reported.get(name):
                    counters[name].inc(total - reported.get(name, 0))
                    reported[name] = total
            return 0

        # PyFlink 1.20 evaluates gauge callbacks at bundle finish, inside Beam's
        # metric scope. Publish counter deltas here so timer counts are included
        # and reporting stays independent of gauge timestamps.
        metrics.gauge("trading_counter_flush", flush_counts)

    def inc(self, name):
        self.counts[name] += 1


class DiagnosticSamples:
    def __init__(self):
        self.samples = {}

    def record(self, kind, reason, record):
        samples = self.samples.get(kind, 0)
        if samples >= DIAGNOSTIC_CAP:
            return None
        self.samples[kind] = samples + 1
        sample = {"kind": kind, "reason": reason}
        for key, value in record.items():
            if key not in DIAGNOSTIC_FIELDS:
                continue
            if isinstance(value, str):
                sample[key] = value[:160]
            elif isinstance(value, (int, float)) or value is None:
                sample[key] = value
        return json.dumps(sample)


class DecodeAndClassify(ProcessFunction):
    def open(self, context):
        self.metrics = Counters(context, [
            "raw_received", "missing_trading_date_fallbacks",
            HOUSEKEEPING, QUOTE, REJECTED,
        ])
        self.diagnostics = DiagnosticSamples()

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)
        except (ValueError, TypeError):
            record = {}
            kind, close, reason = REJECTED, 0.0, "malformed_json"
        else:
            if isinstance(record, dict) and record.get("kind") == "progress":
                frontier = record.get("frontier_ms")
                if type(frontier) is not int or frontier != ctx.timestamp():
                    raise RuntimeError("progress marker does not match its Kafka timestamp")
                return
            kind, close, reason = classify_row(record)

        self.metrics.inc("raw_received")
        if kind == PRICE:
            if record["trade_clock"] == "capture_date_fallback":
                self.metrics.inc("missing_trading_date_fallbacks")
            yield Row(record["symbol"], record["trade_us"], record["source_index"],
                      record["source_row"], close, record["source_file"])
            return

        self.metrics.inc(kind)
        sample = self.diagnostics.record(kind, reason, record if isinstance(record, dict) else {})
        if sample is not None:
            yield DIAGNOSTICS, sample


class PriceWindows(KeyedProcessFunction):
    def __init__(self, run_id, subscriptions):
        self.run_id = run_id
        self.subscriptions = subscriptions

    def open(self, context):
        self.open_windows = context.get_map_state(
            MapStateDescriptor("open-price-windows", Types.LONG(), CANDIDATE_TYPE)
        )
        self.ema = context.get_state(ValueStateDescriptor("previous-emas", EMA_TYPE))
        self.metrics = Counters(context, [
            "price_accepted", "price_late", "price_windows_completed",
            *(f"price_accepted_{exchange}" for exchange in MARKET_ZONES),
        ])
        self.diagnostics = DiagnosticSamples()

    def process_element(self, value, ctx):
        symbol, trade_us, source_index, source_row, close, source_file = value
        end = window_end_ms(trade_us)
        timer = ctx.timer_service()
        watermark = timer.current_watermark()
        if watermark >= end:
            self.metrics.inc("price_late")
            sample = self.diagnostics.record("price_late", "window_already_final", {
                "symbol": symbol, "trade_us": trade_us, "source_file": source_file,
                "source_index": source_index, "source_row": source_row, "last": close,
                "window_end_ms": end, "watermark_ms": watermark,
            })
            if sample is not None:
                yield DIAGNOSTICS, sample
            return

        self.metrics.inc("price_accepted")
        self.metrics.inc(f"price_accepted_{symbol.rsplit('.', 1)[1]}")
        current_close = self.open_windows.get(end)
        if current_close is None:
            timer.register_event_time_timer(end)
        else:
            close_trade_us, close_source_index, close_source_row, _, _ = current_close
            # At equal trade times, the later source file and row win.
            if (trade_us, source_index, source_row) <= (close_trade_us, close_source_index, close_source_row):
                return
        self.open_windows.put(end, (trade_us, source_index, source_row, close, source_file))

    def on_timer(self, timestamp, ctx):
        from python_calculations import calculate_ema, detect_crossover, window_iso

        trade_us, source_index, source_row, close, source_file = self.open_windows.get(timestamp)
        self.open_windows.remove(timestamp)
        previous38, previous100 = self.ema.value() or (0.0, 0.0)
        ema38 = calculate_ema(close, previous38, 38)
        ema100 = calculate_ema(close, previous100, 100)
        self.ema.update((ema38, ema100))
        advice = detect_crossover(previous38, previous100, ema38, ema100)
        self.metrics.inc("price_windows_completed")

        symbol = ctx.get_current_key()
        if self.subscriptions is not None and symbol not in self.subscriptions:
            return

        zone = MARKET_ZONES[symbol.rsplit(".", 1)[1]]
        identity = f"{self.run_id}:{symbol}:{timestamp}"
        result = {
            "run_id": self.run_id, "symbol": symbol,
            "window_start": window_iso(timestamp - WINDOW_MS, zone),
            "window_end": window_iso(timestamp, zone),
            "window_end_ms": timestamp, "close": close, "ema38": ema38, "ema100": ema100,
        }
        yield json.dumps({
            **result, "kind": "ema", "result_id": f"{identity}:ema",
            "trade_us": trade_us, "source_file": source_file,
            "source_index": source_index, "source_row": source_row,
        })
        if advice:
            yield ADVISORIES, json.dumps({
                **result, "kind": "advisory", "result_id": f"{identity}:{advice}",
                "advice": advice.upper(), "previous_ema38": previous38, "previous_ema100": previous100,
                "reason": "bullish_crossover" if advice == "buy" else "bearish_crossover",
            })


def build_pipeline(env, args):
    topics = args.topics
    recoverable = args.checkpoint_dir is not None
    source = replay_source(topics, request_timeout_ms=args.kafka_request_timeout_ms)
    sink = partial(result_sink, transactional=recoverable,
                   request_timeout_ms=args.kafka_request_timeout_ms)
    # With no idle timeout, an empty partition participates as soon as its startup
    # marker arrives. Later progress is ordered behind its queued rows and markers.
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(args.delay_ms))
    prices = env.from_source(source, watermarks, "kafka-replay-source").process(
        DecodeAndClassify(), output_type=PRICE_TYPE,
    ).name("decode-classify")
    if not args.jvm_windows:
        results = prices.key_by(lambda row: row[0], key_type=Types.STRING()).process(
            PriceWindows(topics.input, args.subscribed_symbols),
            output_type=Types.STRING(),
        )
    else:
        gateway = get_gateway()
        subscriptions = None
        if args.subscribed_symbols is not None:
            subscriptions = gateway.new_array(gateway.jvm.java.lang.String, len(args.subscribed_symbols))
            for index, symbol in enumerate(args.subscribed_symbols):
                subscriptions[index] = symbol
        results = DataStream(gateway.jvm.ssdm.PriceWindows.apply(
            prices._j_data_stream, topics.input, subscriptions, DIAGNOSTIC_CAP,
        ))
    results.name(WINDOW_OPERATOR)

    results.sink_to(sink(topics.windows)).name("ema-kafka-sink")
    results.get_side_output(ADVISORIES).sink_to(sink(topics.advisories)).name("advisory-kafka-sink")
    prices.get_side_output(DIAGNOSTICS).union(results.get_side_output(DIAGNOSTICS)).sink_to(
        sink(topics.diagnostics)
    ).name("diagnostics-kafka-sink")


def lateness_ms(seconds):
    if not math.isfinite(seconds) or not 0 <= seconds <= 86_400:
        raise ValueError("--lateness-seconds must be between 0 and 86400")
    return math.ceil(seconds * 1000)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--topic-prefix", dest="topics", metavar="PREFIX", type=Topics.from_prefix, default=DEFAULT_PREFIX)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--subscribed-symbols", help="Comma-separated symbols to publish")
    parser.add_argument("--lateness-seconds", type=float, default=DEFAULT_LATENESS_SECONDS)
    parser.add_argument("--checkpoint-dir", type=Path, help="Enable checkpoint recovery and Kafka transactions")
    parser.add_argument("--jvm-windows", action="store_true", help="Build and use the JVM window operator")
    parser.add_argument("--kafka-request-timeout-ms", type=int, help="Override the worker's Kafka request timeout")
    parser.add_argument("--interval", type=float, default=2, help="Seconds between progress reports")
    parser.add_argument("--compare-rest-metrics", action="store_true", help="Also log cached Flink REST offsets for comparison")
    parser.add_argument("--timeout", type=float, default=90, help="Seconds before reporting unavailable progress metrics")
    args = parser.parse_args()
    if not 1 <= args.parallelism <= INPUT_PARTITIONS:
        parser.error(f"parallelism must be 1–{INPUT_PARTITIONS}")
    if args.kafka_request_timeout_ms is not None and not 1 <= args.kafka_request_timeout_ms <= 600_000:
        parser.error("Kafka request timeout must be between 1 and 600000 ms")
    if any(not math.isfinite(value) or value <= 0 for value in (args.interval, args.timeout)):
        parser.error("--interval and --timeout must be positive and finite")
    try:
        args.delay_ms = lateness_ms(args.lateness_seconds)
        args.subscribed_symbols = parse_subscriptions(args.subscribed_symbols)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def build_jvm_window_jar(output: Path):
    flink_libraries = Path(pyflink.__file__).parent / "lib" / "*"
    source = Path(__file__).with_name("jvm") / "PriceWindows.java"
    with TemporaryDirectory(prefix="ssdm-jvm-") as classes:
        subprocess.run(["javac", "-cp", str(flink_libraries), "-d", classes, str(source)], check=True)
        subprocess.run(["jar", "--create", "--file", str(output), "-C", classes, "."], check=True)


def make_reporter_available(stack):
    # PyFlink's embedded MiniCluster discovers reporter factories on the gateway classpath.
    flink_home = Path(os.environ.setdefault("FLINK_HOME", str(Path(pyflink.__file__).parent)))
    libraries = Path(os.environ.get("FLINK_LIB_DIR", flink_home / "lib"))
    plugins = Path(os.environ.get("FLINK_PLUGINS_DIR", flink_home / "plugins"))
    reporters = list((plugins / "metrics-prometheus").glob("flink-metrics-prometheus-*.jar"))
    if len(reporters) != 1:
        raise RuntimeError(f"Expected one Prometheus reporter jar under {plugins / 'metrics-prometheus'}")
    reporter = reporters[0]
    gateway_lib = Path(stack.enter_context(TemporaryDirectory(prefix="ssdm-flink-lib-")))
    for library in libraries.glob("*.jar"):
        if library.name != reporter.name:
            (gateway_lib / library.name).symlink_to(library.resolve())
    (gateway_lib / reporter.name).symlink_to(reporter.resolve())
    os.environ["FLINK_LIB_DIR"] = str(gateway_lib)


def submit(args, stack):
    make_reporter_available(stack)
    config = Configuration()
    config.set_integer("rest.port", REST_PORT)
    config.set_string("rest.bind-port", str(REST_PORT))
    config.set_string("rest.bind-address", "127.0.0.1")
    config.set_string("metrics.fetcher.update-interval", "1 s")
    config.set_string("metrics.reporter.prom.factory.class", "org.apache.flink.metrics.prometheus.PrometheusReporterFactory")
    config.set_string("metrics.reporter.prom.port", str(METRICS_PORT))
    config.set_boolean("metrics.reporter.prom.filterLabelValueCharacters", False)
    reported_metrics = ",".join(sorted(LOGGED_COUNTERS | {"currentOffset", "currentOutputWatermark"}))
    config.set_string("metrics.reporter.prom.filter.includes", f"*:{reported_metrics}")
    config.set_integer("python.fn-execution.bundle.size", 1000)
    config.set_string("python.fn-execution.bundle.time", "100")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(args.parallelism)
    if args.checkpoint_dir is None:
        env.set_restart_strategy(RestartStrategies.no_restart())
    else:
        env.enable_checkpointing(1000, CheckpointingMode.EXACTLY_ONCE)
        env.get_checkpoint_config().set_checkpoint_storage_dir(args.checkpoint_dir.resolve().as_uri())
        env.set_restart_strategy(RestartStrategies.fixed_delay_restart(2, 1000))
    env.set_python_executable(sys.executable)
    env.get_config().set_auto_watermark_interval(100)
    env.add_jars(BUNDLED_CONNECTOR.resolve().as_uri())
    if args.jvm_windows:
        directory = stack.enter_context(TemporaryDirectory(prefix="ssdm-flink-"))
        jar = Path(directory) / "price-windows.jar"
        build_jvm_window_jar(jar)
        env.add_jars(jar.as_uri())
    build_pipeline(env, args)
    return env.execute_async(f"trading-replay:{args.topics.input}")


def stop_job(client, logs):
    try:
        if not client.get_job_execution_result().done():
            client.cancel().result()
    except Exception as exc:
        logs.log("CANCEL_ERROR", error=exc)


def main():
    args = parse_args()
    with ExitStack() as stack:
        logs = ResultLogs(stack, args.topics)
        jvm_logs = Path(os.environ.setdefault("FLINK_LOG_DIR", str(args.topics.log_dir / "flink")))
        jvm_logs.mkdir(parents=True, exist_ok=True)
        os.environ.setdefault("FLINK_IDENT_STRING", f"worker-{os.getpid()}")
        subscriptions = "all symbols" if args.subscribed_symbols is None else ",".join(sorted(args.subscribed_symbols))
        logs.log("SUBMITTING", prefix=args.topics.input, parallelism=args.parallelism, broker=BROKER,
                 operator="JVM" if args.jvm_windows else "Python", delay_ms=args.delay_ms,
                 subscriptions=subscriptions or "no published symbols", jvm_logs=jvm_logs)
        try:
            client = submit(args, stack)
            exit_code = 1
            try:
                exit_code = monitor(args, client, logs)
                return exit_code
            finally:
                if exit_code != 0:
                    stop_job(client, logs)
        except Exception as exc:
            logs.log("FAILED", error=exc)
            raise


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, lambda signum, _: sys.exit(128 + signum))
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
