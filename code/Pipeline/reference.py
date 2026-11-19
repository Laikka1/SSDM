"""Compare captured Kafka query results with an independent batch calculation."""

import argparse
from collections import Counter
from contextlib import closing
from datetime import datetime, timedelta
import json
import math
import re
from zoneinfo import ZoneInfo

from kafka_io import Topics, read_outputs
from normalization import EXCHANGE_TIMEZONES, load_subscriptions
from producer import add_selection_arguments, selected_rows, validate_selection

REL_TOLERANCE = ABS_TOLERANCE = 1e-12
TIME_PATTERN = r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?"


def source_prices(args):
    """Compute analytical eligibility directly from the selected CSV fields."""
    zones = {exchange: ZoneInfo(timezone) for exchange, timezone in EXCHANGE_TIMEZONES.items()}
    for row, file, file_index, number in selected_rows(args):
        if any(row.get(None, ())) or None in row.values():
            continue
        symbol = ((row.get("ID.[Exchange]") or "").strip() or (row.get("ID") or "").strip()).upper()
        instrument, _, exchange = symbol.rpartition(".")
        if not instrument or exchange not in zones or row["SecType"].strip().upper() not in ("E", "I"):
            continue
        try:
            values = [float(row[name].strip() or "0") for name in ("Last", "Ask", "Bid")]
            if values[0] <= 0 or any(value < 0 or not math.isfinite(value) for value in values):
                continue
            day = row["Trading date"].strip()
            clock = row["Trading time"].strip()
            if not re.fullmatch(TIME_PATTERN, clock):
                continue
            format_string = "%H:%M:%S.%f" if "." in clock else "%H:%M:%S"
            trade = datetime.strptime(clock, format_string)
            if not day:
                day = row["Date"].strip()
                capture = row["Time"].strip()
                if re.fullmatch(TIME_PATTERN, capture):
                    try:
                        captured = datetime.strptime(capture, "%H:%M:%S.%f" if "." in capture else "%H:%M:%S")
                    except ValueError:
                        captured = None
                    if captured and abs(trade - captured) > timedelta(hours=12):
                        continue
            trade = datetime.combine(datetime.strptime(day, "%d-%m-%Y").date(), trade.time(), zones[exchange])
        except ValueError:
            continue
        yield symbol, trade, file_index, number, file, values[0]


def batch_reference(args):
    closes = {}
    exchanges = Counter()
    for symbol, trade, file_index, number, file, close in source_prices(args):
        exchanges[symbol.rsplit(".", 1)[-1]] += 1
        start = trade.replace(minute=trade.minute // 5 * 5, second=0, microsecond=0)
        key = (symbol, start)
        candidate = (trade, file_index, number, file, close)
        if key not in closes or candidate[:3] > closes[key][:3]:
            closes[key] = candidate

    emitted = {}
    state = {}
    sides = Counter()
    for (symbol, start), (trade, file_index, number, file, close) in sorted(closes.items()):
        end_seconds = int(start.timestamp()) + 300
        end = datetime.fromtimestamp(end_seconds, start.tzinfo)
        end_ms = end_seconds * 1000
        trade_us = int(trade.replace(microsecond=0).timestamp()) * 1_000_000 + trade.microsecond
        previous38, previous100 = state.get(symbol, (0.0, 0.0))
        fast = (2 / 39) * close + (37 / 39) * previous38
        slow = (2 / 101) * close + (99 / 101) * previous100
        state[symbol] = fast, slow
        if args.subscribed_symbols is not None and symbol not in args.subscribed_symbols:
            continue
        base_id = f"{args.topic_prefix}:{symbol}:{end_ms}"
        base = {
            "run_id": args.topic_prefix, "symbol": symbol, "close": close,
            "window_start": start.isoformat(), "window_end": end.isoformat(), "window_end_ms": end_ms,
            "ema38": fast, "ema100": slow,
        }
        emitted[f"{base_id}:ema"] = dict(
            base, kind="ema", trade_us=trade_us, source_index=file_index, source_row=number, source_file=file,
        )
        side = None
        if previous38 <= previous100 and fast > slow:
            side = "BUY"
        elif previous38 >= previous100 and fast < slow:
            side = "SELL"
        if side:
            sides[side] += 1
            emitted[f"{base_id}:{side.lower()}"] = dict(
                base, kind="advisory", advice=side, previous_ema38=previous38, previous_ema100=previous100,
            )

    return emitted, {
        "eligible_prices_by_exchange": dict(exchanges), "price_windows": len(closes),
        "ema_results": len(emitted) - sum(sides.values()), "buy": sides["BUY"], "sell": sides["SELL"],
    }


def compare_output(lines, expected, topics):
    counts = Counter()
    seen = set()
    errors = []
    for item in lines:
        record = item["record"]
        if item["topic"] == topics.diagnostics:
            counts[record["kind"]] += 1
            continue
        identity = record.get("result_id")
        if identity in seen:
            errors.append(f"duplicate {identity}")
            continue
        seen.add(identity)
        if identity not in expected:
            errors.append(f"unexpected {identity}")
            continue
        wanted = expected[identity]
        expected_topic = topics.windows if wanted["kind"] == "ema" else topics.advisories
        if item["topic"] != expected_topic:
            errors.append(f"{identity}: expected topic {expected_topic}, got {item['topic']}")
        for field, value in wanted.items():
            actual = record.get(field)
            if "ema" in field:
                equal = isinstance(actual, (int, float)) and math.isclose(
                    actual, value, rel_tol=REL_TOLERANCE, abs_tol=ABS_TOLERANCE,
                )
            else:
                equal = actual == value
            if not equal:
                errors.append(f"{identity}: {field} expected {value!r}, got {actual!r}")
    missing = expected.keys() - seen
    if missing:
        errors.append(f"{len(missing)} missing results, for example {sorted(missing)[:5]}")
    return errors, dict(counts)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    add_selection_arguments(parser, required=True)
    parser.add_argument("--topic-prefix", required=True)
    parser.add_argument("--subscribed-symbols")
    parser.add_argument("--subscriptions-file")
    checks = parser.add_mutually_exclusive_group()
    checks.add_argument("--check-kafka", action="store_true", help="Compare with results already in Kafka")
    checks.add_argument("--observed", type=argparse.FileType("r"), help="Compare with saved topic and record JSONL")
    parser.add_argument("--broker", default="localhost:9092")
    args = parser.parse_args()
    validate_selection(args)
    topics = Topics.from_prefix(args.topic_prefix)
    args.subscribed_symbols = load_subscriptions(args.subscribed_symbols, args.subscriptions_file)
    expected, summary = batch_reference(args)
    errors = []
    if args.check_kafka:
        with closing(read_outputs(args.broker, topics)) as records:
            errors, samples = compare_output(records, expected, topics)
    elif args.observed:
        with args.observed as file:
            errors, samples = compare_output((json.loads(line) for line in file), expected, topics)
    if args.check_kafka or args.observed:
        summary.update(match=not errors, mismatches=len(errors), examples=errors[:10], diagnostics=samples)
    print(json.dumps(summary, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
