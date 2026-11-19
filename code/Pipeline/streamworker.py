#!/usr/bin/env python3
"""Submit and run the two trading queries in PyFlink."""

import argparse
import json
import os
from pathlib import Path
import sys

from pyflink.common import Configuration, Duration, Row, Types, WatermarkStrategy
from pyflink.common.restart_strategy import RestartStrategies
from pyflink.datastream import OutputTag, StreamExecutionEnvironment
from pyflink.datastream.functions import KeyedProcessFunction, ProcessFunction
from pyflink.datastream.state import MapStateDescriptor, ValueStateDescriptor

from calculations import calculate_ema, detect_crossover
from kafka_io import DEFAULT_PREFIX, INPUT_PARTITIONS, Topics, replay_source, result_sink
from normalization import (
    HOUSEKEEPING, MARKET_ZONES, PRICE, QUOTE, REJECTED, REJECT_REASONS, classify_row, load_subscriptions,
)
from windowing import DEFAULT_LATENESS_SECONDS, WINDOW_MS, lateness_ms, window_end_ms, window_iso

BUNDLED_CONNECTOR = Path(__file__).with_name("jars") / "flink-sql-connector-kafka-3.3.0-1.20.jar"

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
    def __init__(self, limit, counters):
        self.limit = limit
        self.counters = counters
        self.samples = {}

    def record(self, kind, reason, record):
        samples = self.samples.get(kind, 0)
        if samples >= self.limit:
            self.counters.inc(f"diagnostics_suppressed_{kind}")
            return None
        self.samples[kind] = samples + 1
        self.counters.inc(f"diagnostics_sampled_{kind}")
        return json.dumps({"kind": kind, "reason": reason, **{
            key: value[:160] if isinstance(value, str) else value
            for key, value in record.items()
            if key in {
                "symbol", "exchange", "sec_type", "last", "ask", "bid", "trade_us", "trade_clock",
                "source_file", "source_index", "source_row", "window_end_ms", "watermark_ms",
                "capture_date", "capture_time", "trading_date", "trading_time",
            }
            and (isinstance(value, (str, int, float)) or value is None)
        }})


def diagnostic_counters(*kinds):
    return [f"diagnostics_{action}_{kind}" for kind in kinds for action in ("sampled", "suppressed")]


class DecodeAndClassify(ProcessFunction):
    def __init__(self, diagnostic_cap, delay_ms):
        self.diagnostic_cap = diagnostic_cap
        self.delay_ms = delay_ms

    def open(self, context):
        self.metrics = Counters(context, [
            "raw_received", "progress_markers", "terminal_markers", "missing_trading_date_fallbacks",
            HOUSEKEEPING, QUOTE, REJECTED,
            *(f"reject_{reason}" for reason in REJECT_REASONS),
            *diagnostic_counters(REJECTED, HOUSEKEEPING, QUOTE),
        ])
        self.diagnostics = DiagnosticSamples(self.diagnostic_cap, self.metrics)

    def process_element(self, value, ctx):
        try:
            record = json.loads(value)
        except (ValueError, TypeError):
            record = {}
            kind, close, reason = REJECTED, 0.0, "malformed_json"
        else:
            if isinstance(record, dict) and record.get("kind") == "progress":
                frontier = record.get("frontier_ms")
                terminal = record.get("terminal")
                if (
                    type(frontier) is not int or frontier != ctx.timestamp()
                    or record.get("eligible_watermark_ms") != frontier - self.delay_ms - 1
                    or type(terminal) is not bool
                ):
                    raise RuntimeError("progress marker timestamp or producer/worker watermark delay mismatch")
                self.metrics.inc("progress_markers")
                if terminal:
                    self.metrics.inc("terminal_markers")
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
        if kind == REJECTED:
            self.metrics.inc(f"reject_{reason}")
        sample = self.diagnostics.record(kind, reason, record if isinstance(record, dict) else {})
        if sample is not None:
            yield DIAGNOSTICS, sample


class PriceWindows(KeyedProcessFunction):
    def __init__(self, run_id, subscriptions, diagnostic_cap):
        self.run_id = run_id
        self.subscriptions = subscriptions
        self.diagnostic_cap = diagnostic_cap

    def open(self, context):
        self.open_windows = context.get_map_state(
            MapStateDescriptor("open-price-windows", Types.LONG(), CANDIDATE_TYPE)
        )
        self.ema = context.get_state(ValueStateDescriptor("previous-emas", EMA_TYPE))
        self.metrics = Counters(context, [
            "price_accepted", "price_late", "price_windows_completed", "ema_published",
            "buy_published", "sell_published",
            *(f"price_accepted_{exchange}" for exchange in MARKET_ZONES),
            *diagnostic_counters("price_late"),
        ])
        self.diagnostics = DiagnosticSamples(self.diagnostic_cap, self.metrics)

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
        candidate = self.open_windows.get(end)
        order = (trade_us, source_index, source_row)
        if candidate is None:
            timer.register_event_time_timer(end)
        if candidate is None or order > candidate[:3]:
            self.open_windows.put(end, (*order, close, source_file))

    def on_timer(self, timestamp, ctx):
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
        self.metrics.inc("ema_published")
        yield json.dumps({
            **result, "kind": "ema", "result_id": f"{identity}:ema",
            "trade_us": trade_us, "source_file": source_file,
            "source_index": source_index, "source_row": source_row,
        })
        if advice:
            self.metrics.inc(f"{advice}_published")
            yield ADVISORIES, json.dumps({
                **result, "kind": "advisory", "result_id": f"{identity}:{advice}",
                "advice": advice.upper(), "previous_ema38": previous38, "previous_ema100": previous100,
                "reason": "bullish_crossover" if advice == "buy" else "bearish_crossover",
            })


def build_pipeline(env, args):
    topics = Topics.from_prefix(args.topic_prefix)
    source = replay_source(args.broker, topics)
    # With no idle timeout, an empty partition participates as soon as its startup
    # marker arrives. Later progress is ordered behind its queued rows and markers.
    watermarks = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_millis(args.delay_ms))
    prices = env.from_source(source, watermarks, "kafka-replay-source").process(
        DecodeAndClassify(args.diagnostic_cap, args.delay_ms), output_type=PRICE_TYPE,
    ).name("decode-classify")
    results = prices.key_by(lambda row: row[0], key_type=Types.STRING()).process(
        PriceWindows(args.topic_prefix, args.subscribed_symbols, args.diagnostic_cap),
        output_type=Types.STRING(),
    ).name("price-windows-ema-crossover")

    results.sink_to(result_sink(args.broker, topics.windows)).name("ema-kafka-sink")
    results.get_side_output(ADVISORIES).sink_to(
        result_sink(args.broker, topics.advisories)
    ).name("advisory-kafka-sink")
    prices.get_side_output(DIAGNOSTICS).union(results.get_side_output(DIAGNOSTICS)).sink_to(
        result_sink(args.broker, topics.diagnostics)
    ).name("diagnostics-kafka-sink")


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--broker", "--flink-broker", default="localhost:9092")
    parser.add_argument("--topic-prefix", default=DEFAULT_PREFIX)
    parser.add_argument("--parallelism", type=int, default=1)
    parser.add_argument("--subscribed-symbols")
    parser.add_argument("--subscriptions-file")
    parser.add_argument("--lateness-seconds", type=float, default=DEFAULT_LATENESS_SECONDS)
    parser.add_argument("--diagnostic-cap", type=int, default=20)
    parser.add_argument("--watermark-interval-ms", type=int, default=100)
    parser.add_argument("--bundle-time-ms", type=int, default=100)
    parser.add_argument("--bundle-size", type=int, default=1000)
    parser.add_argument("--rest-port", type=int, help="Expose local Flink job metrics on this loopback port")
    parser.add_argument("--connector-jar", type=Path, default=os.getenv("FLINK_KAFKA_CONNECTOR_JAR"))
    parser.add_argument("--print-plan", action="store_true")
    args = parser.parse_args()
    try:
        Topics.from_prefix(args.topic_prefix)
        args.delay_ms = lateness_ms(args.lateness_seconds)
        args.subscribed_symbols = load_subscriptions(args.subscribed_symbols, args.subscriptions_file)
        if not 1 <= args.parallelism <= INPUT_PARTITIONS or args.diagnostic_cap < 0:
            raise ValueError(f"parallelism must be 1–{INPUT_PARTITIONS} and diagnostic cap non-negative")
        if min(args.bundle_time_ms, args.bundle_size, args.watermark_interval_ms) < 1:
            raise ValueError("bundle size, bundle time and watermark interval must be positive")
        if args.rest_port is not None and not 1 <= args.rest_port <= 65535:
            raise ValueError("--rest-port must be between 1 and 65535")
        if args.connector_jar is not None and not args.connector_jar.is_file():
            raise ValueError(f"Kafka connector jar does not exist: {args.connector_jar}")
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main():
    args = parse_args()
    local = "PYFLINK_GATEWAY_PORT" not in os.environ
    if local and not args.print_plan and "FLINK_LOG_DIR" not in os.environ:
        log_dir = Path(__file__).resolve().parents[1] / "logs" / args.topic_prefix
        log_dir.mkdir(parents=True, exist_ok=True)
        os.environ["FLINK_LOG_DIR"] = str(log_dir)
        os.environ.setdefault("FLINK_IDENT_STRING", f"worker-{os.getpid()}")

    config = Configuration()
    if args.rest_port is not None:
        if not local:
            raise SystemExit("With flink run, use the cluster's configured REST endpoint.")
        config.set_integer("rest.port", args.rest_port)
        config.set_string("rest.bind-port", str(args.rest_port))
        config.set_string("rest.bind-address", "127.0.0.1")
        config.set_string("metrics.fetcher.update-interval", "1 s")
    env = StreamExecutionEnvironment.get_execution_environment(config)
    env.set_parallelism(args.parallelism)
    env.set_restart_strategy(RestartStrategies.no_restart())
    env.set_python_executable(sys.executable)
    env.get_config().set_auto_watermark_interval(args.watermark_interval_ms)
    subscriptions = (
        "all symbols" if args.subscribed_symbols is None
        else ",".join(sorted(args.subscribed_symbols)) or "no published symbols"
    )
    env.get_config().set_global_job_parameters({
        "trading.topic_prefix": args.topic_prefix,
        "trading.subscriptions": subscriptions,
    })
    config.set_integer("python.fn-execution.bundle.size", args.bundle_size)
    config.set_string("python.fn-execution.bundle.time", str(args.bundle_time_ms))
    env.configure(config)
    connector = args.connector_jar or BUNDLED_CONNECTOR
    if local:
        if connector.is_file():
            env.add_jars(connector.resolve().as_uri())
    elif args.connector_jar is not None:
        raise SystemExit("With flink run, install the connector in the Flink lib directory.")

    build_pipeline(env, args)
    if args.print_plan:
        print(env.get_execution_plan())
        return
    print(f"Submitting {args.topic_prefix} to Flink with parallelism {args.parallelism}, "
          f"Kafka {args.broker}, event-time delay {args.delay_ms} ms, subscriptions {subscriptions}",
          flush=True)
    if local and "FLINK_LOG_DIR" in os.environ:
        print(f"Flink JVM logs: {os.environ['FLINK_LOG_DIR']}", flush=True)
    env.execute(f"trading-replay:{args.topic_prefix}")


if __name__ == "__main__":
    main()
