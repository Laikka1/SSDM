import unittest
import os
from datetime import datetime
from zoneinfo import ZoneInfo

from normalization import normalize_envelope
from trading_events import NormalizedEvent, Rejection
from validation import validate_event
from ema import calculate_ema, prepare_output_logs
from crossover import detect_crossover
from subscriptions import load_subscriptions, normalize_symbol
from windowing import build_window_result, window_bounds, window_timestamp


def envelope(**payload):
    return {
        "event_key": "sample.csv:1",
        "symbol": payload.get("ID.[Exchange]", "ABC.FR"),
        "payload": {
            "ID.[Exchange]": payload.get("ID.[Exchange]", "ABC.FR"),
            "SecType": payload.get("SecType", "E"),
            "Date": payload.get("Date", "08-11-2021"),
            "Time": payload.get("Time", "00:05:00.000"),
            **payload,
        },
        "_source_file": "sample.csv",
        "_source_row": 1,
        "_produced_at": "2021-11-08T00:00:00+00:00",
    }


class PipelineSemanticsTests(unittest.TestCase):
    def test_normalizes_exchange_timezone_and_trade(self):
        result = normalize_envelope(envelope(Last="12.5"))
        self.assertIsInstance(result, NormalizedEvent)
        self.assertEqual(result.event_type, "trade")
        self.assertEqual(result.event_time.tzinfo, ZoneInfo("Europe/Paris"))
        self.assertEqual(result.last_price, 12.5)

    def test_classifies_multiple_attributes_as_one_event(self):
        result = normalize_envelope(envelope(Ask="10", Bid="9"))
        self.assertIsInstance(result, NormalizedEvent)
        self.assertEqual(result.event_type, "quote")
        self.assertEqual(result.available_attributes, ("ask", "bid"))

        result = normalize_envelope(envelope(Ask="10", Bid="9", Last="8"))
        self.assertIsInstance(result, NormalizedEvent)
        self.assertEqual(result.event_type, "trade")
        self.assertEqual(result.last_price, 8.0)

    def test_rejects_invalid_price(self):
        result = normalize_envelope(envelope(Last="not-a-price"))
        self.assertIsInstance(result, Rejection)
        self.assertEqual(result.reason_code, "invalid_numeric_value")

        result = normalize_envelope(envelope(Last="0"))
        self.assertIsInstance(result, NormalizedEvent)
        self.assertEqual(result.event_type, "housekeeping")
        self.assertIsNone(result.last_price)

    def test_validation_requires_trade_price(self):
        event = normalize_envelope(envelope())
        self.assertIsInstance(event, NormalizedEvent)
        self.assertIsNone(validate_event(event))

    def test_window_exact_boundary_and_midnight(self):
        midnight = datetime(2021, 11, 8, 0, 0, tzinfo=ZoneInfo("Europe/Paris"))
        self.assertEqual(window_bounds(midnight), (midnight, datetime(2021, 11, 8, 0, 5, tzinfo=midnight.tzinfo)))
        boundary = datetime(2021, 11, 8, 0, 5, tzinfo=ZoneInfo("Europe/Paris"))
        start, end = window_bounds(boundary)
        self.assertEqual(start, boundary)
        self.assertEqual((end - start).total_seconds(), 300)

    def test_window_timestamp_aligns_exchange_midnight_in_winter_and_summer(self):
        winter = datetime(2021, 11, 8, 0, 1, tzinfo=ZoneInfo("Europe/Paris"))
        summer = datetime(2021, 7, 8, 0, 1, tzinfo=ZoneInfo("Europe/Paris"))
        winter_shifted = window_timestamp(winter, "FR")
        summer_shifted = window_timestamp(summer, "FR")
        self.assertEqual(winter_shifted.hour, 0)
        self.assertEqual(summer_shifted.hour, 0)
        self.assertEqual(winter_shifted.minute, 1)
        self.assertEqual(summer_shifted.minute, 1)

    def test_ema_uses_zero_for_first_window(self):
        close = 100.0
        self.assertAlmostEqual(calculate_ema(close, 0.0, 38), 2 * close / 39)
        self.assertAlmostEqual(calculate_ema(close, 0.0, 100), 2 * close / 101)

    def test_ema_uses_previous_value(self):
        first = calculate_ema(100.0, 0.0, 38)
        second = calculate_ema(110.0, first, 38)
        expected = (2 / 39) * 110.0 + (37 / 39) * first
        self.assertAlmostEqual(second, expected)

    def test_crossover_requires_previous_window(self):
        self.assertIsNone(detect_crossover(None, None, 2.0, 1.0))

    def test_crossover_detects_buy_and_sell_once(self):
        self.assertEqual(detect_crossover(1.0, 2.0, 3.0, 2.0), "buy")
        self.assertEqual(detect_crossover(3.0, 2.0, 1.0, 2.0), "sell")
        self.assertIsNone(detect_crossover(3.0, 2.0, 4.0, 2.0))
        self.assertIsNone(detect_crossover(1.0, 2.0, 0.5, 2.0))

    def test_subscriptions_are_normalized_and_deduplicated(self):
        self.assertEqual(
            load_subscriptions("abc.fr,ABC.FR,xyz.nl"), {"ABC.FR", "XYZ.NL"}
        )
        self.assertIsNone(load_subscriptions())
        self.assertEqual(load_subscriptions(""), set())
        with self.assertRaises(ValueError):
            normalize_symbol("ABC.DE")

    def test_output_logs_are_created_at_startup(self):
        subscribed_log, advisory_log = prepare_output_logs()
        self.assertTrue(os.path.isfile(subscribed_log))
        self.assertTrue(os.path.isfile(advisory_log))

    def test_window_result_counts_event_types_and_prices(self):
        first = normalize_envelope(envelope(Last="12.5"))
        second = normalize_envelope(envelope(Time="00:06:00.000"))
        self.assertIsInstance(first, NormalizedEvent)
        self.assertIsInstance(second, NormalizedEvent)
        result = build_window_result([first, second])
        self.assertEqual(result["event_count"], 2)
        self.assertEqual(result["event_type_counts"], {"trade": 1, "housekeeping": 1})
        self.assertEqual(result["valid_price_count"], 1)
        self.assertEqual(result["last_trade_price"], 12.5)
        self.assertIsNotNone(result["last_trade_event_time"])

    def test_window_result_uses_latest_trade_not_latest_event(self):
        early_trade = normalize_envelope(envelope(Time="00:05:01.000", Last="12.5"))
        later_housekeeping = normalize_envelope(envelope(Time="00:05:02.000"))
        self.assertIsInstance(early_trade, NormalizedEvent)
        self.assertIsInstance(later_housekeeping, NormalizedEvent)
        result = build_window_result([early_trade, later_housekeeping])
        self.assertEqual(result["event_count"], 2)
        self.assertEqual(result["last_trade_price"], 12.5)


if __name__ == "__main__":
    unittest.main()
