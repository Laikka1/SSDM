import argparse
from collections import Counter
import json
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from urllib.error import HTTPError
from urllib.parse import urlencode

from calculations import calculate_ema, detect_crossover
from inspect_run import METRIC_REQUEST_LIMIT, Flink, FlinkJobGone, drained, progress_fields
from kafka_io import Topics
from normalization import classify_row, load_subscriptions, project_row
from producer import preflight, prepare_order, projected_rows
from reference import batch_reference, compare_output
from streamworker import ADVISORIES, DIAGNOSTICS, DecodeAndClassify, PriceWindows
from windowing import window_end_ms


FIXTURE = Path(__file__).with_name("fixtures") / "mixed.csv"


def selection(**overrides):
    args = dict(files=[FIXTURE], subset=100, max_rows=None, skip_rows=0, ordered_db=None,
                topic_prefix="fixture", subscribed_symbols={"ABC.FR", "SPARSE.NL"})
    return argparse.Namespace(**(args | overrides))


class Metrics:
    def __init__(self):
        self.counts = Counter()
        self.flushers = []

    def add_group(self, name):
        return self

    def gauge(self, name, value):
        self.flushers.append(value)

    def counter(self, name):
        def inc(amount):
            self.counts[name] += amount

        return SimpleNamespace(inc=inc)

    def value(self, name):
        for flush in self.flushers:
            flush()
        return self.counts[name]


class ValueState:
    def __init__(self):
        self.current = None

    def value(self):
        return self.current

    def update(self, value):
        self.current = value


class MapState(dict):
    def put(self, key, value):
        self[key] = value

    def remove(self, key):
        del self[key]


class Context:
    def __init__(self):
        self.metrics = Metrics()
        self.symbol = None
        self.maps = {}
        self.values = {}
        self.watermark = -(2**63)
        self.timers = {}
        self.progress_timestamp = 1

    def get_map_state(self, _):
        return self

    def get(self, key):
        return self.maps.setdefault(self.symbol, MapState()).get(key)

    def put(self, key, value):
        self.maps.setdefault(self.symbol, MapState()).put(key, value)

    def remove(self, key):
        self.maps[self.symbol].remove(key)

    def get_state(self, _):
        return self

    def value(self):
        return self.values.setdefault(self.symbol, ValueState()).value()

    def update(self, value):
        self.values.setdefault(self.symbol, ValueState()).update(value)

    def get_metrics_group(self):
        return self.metrics

    def timer_service(self):
        return self

    def get_current_key(self):
        return self.symbol

    def current_watermark(self):
        return self.watermark

    def register_event_time_timer(self, end):
        self.timers.setdefault(end, set()).add(self.symbol)

    def timestamp(self):
        return self.progress_timestamp


class PipelineSemanticsTests(unittest.TestCase):
    def test_source_identity_fallback_and_boundary(self):
        records = list(projected_rows(selection(skip_rows=2, max_rows=4)))
        self.assertEqual([r["source_row"] for r in records], [3, 4, 5, 6])
        self.assertEqual(records[1]["trade_us"] - records[2]["trade_us"], 400)
        self.assertEqual(window_end_ms(records[3]["trade_us"]) -
                         window_end_ms(records[0]["trade_us"]), 300_000)
        rows = list(projected_rows(selection()))
        self.assertEqual(rows[0]["trade_clock"], "capture_date_fallback")
        self.assertEqual(classify_row(rows[7])[0], "quote_only")
        self.assertEqual(classify_row(rows[9])[0], "housekeeping_or_metadata")
        self.assertEqual(classify_row(rows[-3])[-1], "ambiguous_trading_date")
        self.assertEqual(rows[-1]["trade_clock"], "explicit")
        row = {"ID": "ABC.FR", "SecType": "E", "Date": "08-11-2021",
               "Last": "1", "Trading time": "00:05:00", "Time": ""}
        self.assertEqual(project_row(row, "sample", 0, 1)["trade_clock"], "capture_date_fallback")

    def test_preserves_zero_rule_and_rejects_invalid_quote_attributes(self):
        row = {"ID": " abc.fr ", "SecType": "I", "Last": "0", "Ask": "0", None: [""]}
        envelope = project_row(row, "sample", 0, 1)
        self.assertEqual(envelope["symbol"], "ABC.FR")
        self.assertEqual(classify_row(envelope)[0], "housekeeping_or_metadata")
        row["Bid"] = "-4"
        self.assertEqual(classify_row(project_row(row, "sample", 0, 1))[-1], "invalid_price_value")
        row[None].append("unexpected")
        self.assertEqual(classify_row(project_row(row, "sample", 0, 1))[-1], "invalid_csv_row")

    def test_reference_matches_typed_operator_with_subscriptions(self):
        args = selection()
        topics = Topics.from_prefix(args.topic_prefix)
        expected, summary = batch_reference(args)
        self.assertGreater(summary["sell"], 0)
        self.assertGreater(summary["buy"], 2)
        self.assertEqual(set(summary["eligible_prices_by_exchange"]), {"FR", "NL", "ETR"})

        context = Context()
        classify = DecodeAndClassify(1, 1000)
        analytics = PriceWindows(args.topic_prefix, args.subscribed_symbols, 1)
        classify.open(context)
        analytics.open(context)

        captured = []
        for envelope in projected_rows(args):
            for item in classify.process_element(json.dumps(envelope), context):
                if isinstance(item, tuple) and item[0] == DIAGNOSTICS:
                    captured.append({"topic": topics.diagnostics, "record": json.loads(item[1])})
                else:
                    context.symbol = item[0]
                    self.assertEqual(list(analytics.process_element(item, context)), [])

        for end, symbols in sorted(context.timers.items()):
            context.watermark = end
            for symbol in symbols:
                context.symbol = symbol
                for item in analytics.on_timer(end, context):
                    if isinstance(item, tuple):
                        self.assertEqual(item[0], ADVISORIES)
                        captured.append({"topic": topics.advisories, "record": json.loads(item[1])})
                    else:
                        captured.append({"topic": topics.windows, "record": json.loads(item)})

        self.assertEqual(compare_output(captured, expected, topics)[0], [])
        self.assertEqual(context.metrics.value("raw_received"), 50)
        self.assertEqual(context.metrics.value("price_accepted"), 46)
        self.assertEqual(context.metrics.value("price_windows_completed"), summary["price_windows"])
        self.assertEqual(context.metrics.value("diagnostics_sampled_rejected"), 1)
        self.assertEqual(context.metrics.value("diagnostics_suppressed_rejected"), 1)
        self.assertAlmostEqual(context.values["PRIVATE.ETR"].value()[0],
                               calculate_ema(40, calculate_ema(30, 0, 38), 38))

        # Replaying a price whose window has closed can only emit a late diagnostic.
        first = next(projected_rows(args))
        item, = classify.process_element(json.dumps(first), context)
        context.symbol = item[0]
        late, = analytics.process_element(item, context)
        self.assertEqual(late[0], DIAGNOSTICS)
        self.assertEqual(list(analytics.process_element(item, context)), [])
        self.assertEqual(context.metrics.value("price_late"), 2)
        self.assertEqual(context.metrics.value("diagnostics_suppressed_price_late"), 1)
        self.assertEqual(context.maps["ABC.FR"], {})

    def test_markers_are_counted_separately_and_require_matching_delay(self):
        context = Context()
        classify = DecodeAndClassify(0, 1000)
        classify.open(context)
        marker = {"kind": "progress", "frontier_ms": 1, "eligible_watermark_ms": -1000, "terminal": True}
        self.assertEqual(list(classify.process_element(json.dumps(marker), context)), [])
        self.assertEqual(context.metrics.value("raw_received"), 0)
        self.assertEqual(context.metrics.value("progress_markers"), 1)
        self.assertEqual(context.metrics.value("terminal_markers"), 1)
        marker["eligible_watermark_ms"] = 0
        with self.assertRaisesRegex(RuntimeError, "delay mismatch"):
            list(classify.process_element(json.dumps(marker), context))

    def test_ordered_replay_retains_every_row_and_tie_order(self):
        args = selection()
        original = list(projected_rows(args))
        self.assertEqual(preflight(original)["max_backward_us"], 400)
        with TemporaryDirectory() as path:
            ordered_db = Path(path) / "rows.sqlite"
            args.prepare_order = ordered_db
            self.assertEqual(prepare_order(args)["raw_rows"], len(original))
            args.ordered_db = ordered_db
            ordered = list(projected_rows(args))
        self.assertEqual(preflight(ordered)["max_backward_us"], 0)
        self.assertEqual(sorted(ordered, key=lambda row: row["source_row"]), original)
        tied = [r["source_row"] for r in ordered if r["trading_time"] == "00:04:59.9999"]
        self.assertEqual(tied, [3, 4])

    def test_ema_and_crossover_use_initial_zero(self):
        self.assertAlmostEqual(calculate_ema(100, 0, 38), 200 / 39)
        self.assertAlmostEqual(calculate_ema(100, 0, 100), 200 / 101)
        self.assertEqual(detect_crossover(0.0, 0.0, 2.0, 1.0), "buy")
        self.assertEqual(detect_crossover(3.0, 2.0, 1.0, 2.0), "sell")
        self.assertIsNone(detect_crossover(3.0, 2.0, 4.0, 2.0))

    def test_subscriptions_are_normalized_and_can_be_empty(self):
        self.assertEqual(load_subscriptions("abc.fr,ABC.FR,xyz.nl"), {"ABC.FR", "XYZ.NL"})
        self.assertIsNone(load_subscriptions())
        self.assertEqual(load_subscriptions(""), set())
        with self.assertRaises(ValueError):
            load_subscriptions("ABC.DE")

    def test_known_flink_job_disappeared(self):
        flink = Flink("http://127.0.0.1:8082", "fixture")
        jobs = {"jobs": [{"name": "trading-replay:fixture", "start-time": 1, "jid": "abcd"}]}
        error = HTTPError("http://127.0.0.1:8082/jobs/abcd", 404, "Not Found", None, None)
        with patch.object(flink, "get", side_effect=[jobs, error]):
            with self.assertRaisesRegex(FlinkJobGone, "job abcd"):
                flink.job()
        with patch.object(flink, "get", side_effect=HTTPError("", 500, "Server Error", None, None)):
            with self.assertRaises(HTTPError):
                flink.job()

    def test_parallel_flink_metrics_fit_in_several_requests(self):
        flink = Flink("http://127.0.0.1:8082", "fixture")
        path = "/jobs/job-id/vertices/vertex-id/metrics"
        names = [f"{i % 8}.operator name.trading.metric_{i}" for i in range(120)]

        def get(request_path, **query):
            self.assertEqual(request_path, path)
            self.assertLessEqual(len(path) + len("?") + len(urlencode(query)), METRIC_REQUEST_LIMIT)
            return [{"id": name, "value": "1"} for name in query["get"].split(",")]

        with patch.object(flink, "get", side_effect=get) as request:
            self.assertEqual([sample["id"] for sample in flink.metric_samples(path, names)], names)
            self.assertGreater(request.call_count, 1)

    def test_progress_distinguishes_kafka_lag_from_raw_backlog(self):
        journal = argparse.Namespace(finished=None, latest={"raw_acked": 100})
        offsets = dict.fromkeys(range(8), 10)
        ends = dict.fromkeys(range(8), 15)
        _, sample, _ = progress_fields(offsets, ends, {"raw_received": 75}, None, journal, None, Counter())
        self.assertEqual(sample["lag_records"], 40)
        self.assertEqual(sample["raw_backlog"], 25)
        self.assertEqual(sample["producer_state"], "replaying")
        # Flink's sample may be newer than the producer's log entry.
        _, sample, _ = progress_fields(offsets, ends, {"raw_received": 101}, None, journal, None, Counter())
        self.assertEqual(sample["raw_backlog"], "unavailable")
        journal.finished = 1.0
        _, sample, _ = progress_fields(ends, ends, {"raw_received": 100}, None, journal, None, Counter())
        self.assertEqual(sample["raw_backlog"], 0)
        self.assertEqual(sample["producer_state"], "finished")

    def test_drain_waits_for_final_window_and_kafka_delivery(self):
        journal = argparse.Namespace(
            finished=1.0, terminal={"eligible_watermark_ms": 1000},
            latest={"markers_acked": 16, "raw_acked": 2},
        )
        counters = {
            "terminal_markers": 8, "progress_markers": 16, "raw_received": 2,
            "price_accepted": 1, "housekeeping_or_metadata": 1,
            "price_windows_completed": 1, "ema_published": 1, "buy_published": 1,
        }
        observed = Counter(EMA=1)
        self.assertFalse(drained(journal, 0, 8, counters, 1000, observed))
        observed["BUY"] = 1
        self.assertFalse(drained(journal, 0, 8, counters, 999, observed))
        self.assertFalse(drained(journal, 1, 8, counters, 1000, observed))
        self.assertFalse(drained(journal, None, 7, counters, 1000, observed))
        self.assertTrue(drained(journal, 0, 8, counters, 1000, observed))
        self.assertTrue(drained(journal, None, 8, counters, 1000, observed))


if __name__ == "__main__":
    unittest.main()
