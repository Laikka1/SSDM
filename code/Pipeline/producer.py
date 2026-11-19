#!/usr/bin/env python3
"""Replay every selected CSV row and advertise partition-ordered event-time progress."""

import argparse
from contextlib import closing
import csv
from itertools import islice
import json
import math
import os
from pathlib import Path
import sqlite3
import sys
from tempfile import NamedTemporaryFile
import time

from kafka_io import DEFAULT_PREFIX, INPUT_PARTITIONS, Topics, create_producer, enqueue, verify_empty_topics
from normalization import PRICE, TERMINAL_CLASSES, classify_row, project_row
from windowing import DEFAULT_LATENESS_SECONDS, lateness_ms, window_end_ms

REQUIRED_FIELDS = {"SecType", "Date", "Time", "Last", "Ask", "Bid", "Trading date", "Trading time"}
INITIAL_PROGRESS_MS = 1  # Kafka clients use a zero timestamp to mean wall-clock time.


def add_selection_arguments(parser, *, required=False):
    parser.add_argument("--files", "-f", nargs="+", type=Path, required=required)
    parser.add_argument("--max-rows", type=int, help="Maximum rows per file after skipping")
    parser.add_argument("--skip-rows", type=int, default=0, help="Initial data rows to skip per file")
    parser.add_argument("--subset", "-s", type=float, default=100.0, help="Percentage of each file")


def validate_selection(args):
    if args.skip_rows < 0 or (args.max_rows is not None and args.max_rows < 1):
        raise ValueError("use --skip-rows >= 0 and --max-rows >= 1")
    if not 0 < args.subset <= 100:
        raise ValueError("--subset must be in (0, 100]")
    for path in args.files or ():
        if not path.is_file():
            raise ValueError(f"input file does not exist: {path}")


def csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(line for line in file if not line.startswith("#"))
        fields = reader.fieldnames
        if fields is None:
            raise ValueError(f"{path}: missing CSV header")
        reader.fieldnames = [name.strip() for name in fields]
        fields = set(reader.fieldnames)
        if not REQUIRED_FIELDS <= fields or not fields.intersection({"ID", "ID.[Exchange]"}):
            raise ValueError(f"{path}: missing CSV fields {sorted(REQUIRED_FIELDS - fields)} or ID")
        yield from reader


def selected_rows(args):
    for source_index, path in enumerate(args.files):
        cap = args.max_rows
        if cap is None and args.subset < 100:
            cap = math.ceil(sum(1 for _ in csv_rows(path)) * args.subset / 100)
        stop = args.skip_rows + cap if cap is not None else None
        with closing(csv_rows(path)) as rows:
            selected = islice(rows, args.skip_rows, stop)
            for source_row, row in enumerate(selected, start=args.skip_rows + 1):
                yield row, path.name, source_index, source_row


def projected_rows(args):
    if args.ordered_db:
        with sqlite3.connect(args.ordered_db.resolve().as_uri() + "?mode=ro", uri=True) as db:
            for payload, in db.execute("SELECT payload FROM replay ORDER BY sort_us, sequence"):
                yield json.loads(payload)
        return
    for row, source_file, source_index, source_row in selected_rows(args):
        yield project_row(row, source_file, source_index, source_row)


def preflight(rows):
    frontier = 0
    backward_us = window_delay_ms = prices = raw_rows = 0
    terminal = dict.fromkeys(TERMINAL_CLASSES, 0)
    fallback_prices = 0
    previous_date = ""
    capture_day_changes = near_midnight_prices = 0
    for record in rows:
        raw_rows += 1
        kind, _, _ = classify_row(record)
        terminal["price_accepted" if kind == PRICE else kind] += 1
        date = record["capture_date"]
        if date and date != previous_date:
            capture_day_changes += bool(previous_date)
            previous_date = date
        if kind != PRICE:
            continue
        timestamp = record["trade_us"]
        prices += 1
        fallback_prices += record["trade_clock"] == "capture_date_fallback"
        near_midnight_prices += record["trading_time"][:2] in ("00", "23")
        backward_us = max(backward_us, frontier - timestamp)
        window_delay_ms = max(window_delay_ms, frontier // 1000 - window_end_ms(timestamp))
        frontier = max(frontier, timestamp)
    return {
        "raw_rows": raw_rows, "eligible_prices": prices, "max_backward_us": backward_us,
        "min_window_safe_delay_ms": window_delay_ms, "latest_trade_us": frontier,
        "missing_trading_date_fallbacks": fallback_prices, "classes": terminal,
        "capture_day_transitions": capture_day_changes, "near_midnight_prices": near_midnight_prices,
    }


def prepare_order(args):
    destination = args.prepare_order
    if destination.exists():
        raise ValueError(f"ordered replay already exists: {destination}")
    started = time.monotonic()
    with NamedTemporaryFile(dir=destination.parent, prefix=".replay-") as temporary:
        with sqlite3.connect(temporary.name) as db:
            db.execute("CREATE TABLE replay (sequence INTEGER PRIMARY KEY, sort_us INTEGER, payload TEXT)")
            db.executemany("INSERT INTO replay VALUES (?, ?, ?)", _ordered_payloads(args))
            db.execute("CREATE INDEX replay_order ON replay(sort_us, sequence)")
            count = db.execute("SELECT count(*) FROM replay").fetchone()[0]
        os.link(temporary.name, destination)
    return {"raw_rows": count, "preparation_s": time.monotonic() - started,
            "ordered_db_bytes": destination.stat().st_size}


def _ordered_payloads(args):
    frontier = 0
    for sequence, record in enumerate(projected_rows(args)):
        kind, _, _ = classify_row(record)
        if kind == PRICE:
            sort_us = record["trade_us"]
            frontier = max(frontier, sort_us)
        else:
            sort_us = frontier
        yield sequence, sort_us, json.dumps(record, separators=(",", ":"))


def report(event, **fields):
    print(json.dumps({
        "event": event, "wall_ns": time.time_ns(), "monotonic_ns": time.monotonic_ns(), **fields,
    }), flush=True)


class Delivery:
    def __init__(self):
        self.raw_queued = self.raw_acked = self.markers_queued = self.markers_acked = 0
        self.failed = 0
        self.last_error = None

    def callback(self, err, msg, marker=None):
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
        elif marker is None:
            self.raw_acked += 1
        else:
            self.markers_acked += 1
            marker["offsets"][msg.partition()] = msg.offset()
            if len(marker["offsets"]) == INPUT_PARTITIONS:
                report("progress_acked", **marker)

    def check(self):
        if self.failed:
            raise RuntimeError(f"{self.failed} delivery failures; last Kafka error: {self.last_error}")

    def counts(self):
        return dict(raw_queued=self.raw_queued, raw_acked=self.raw_acked,
                    markers_queued=self.markers_queued, markers_acked=self.markers_acked,
                    failed=self.failed)


def send_progress(producer, delivery, topic, timestamp_ms, delay_ms, *, terminal=False):
    marker = {
        "frontier_ms": timestamp_ms, "eligible_watermark_ms": timestamp_ms - delay_ms - 1,
        "sent_wall_ns": time.time_ns(), "sent_monotonic_ns": time.monotonic_ns(), "terminal": terminal,
    }
    value = json.dumps({"kind": "progress", **marker}, separators=(",", ":")).encode()
    marker["offsets"] = {}
    for partition in range(INPUT_PARTITIONS):
        enqueue(
            producer, delivery, topic=topic, value=value,
            key=b"", partition=partition, timestamp=timestamp_ms,
            on_delivery=lambda err, msg: delivery.callback(err, msg, marker),
        )
        delivery.markers_queued += 1


def run_replay(args, topics, expected):
    verify_empty_topics(args.broker, topics)
    producer = create_producer(args.broker, args.topic_prefix)
    delivery = Delivery()
    delay = lateness_ms(args.lateness_seconds)
    frontier = 0
    advertised = INITIAL_PROGRESS_MS
    with closing(projected_rows(args)) as records:
        # Open the input and apply --skip-rows before starting the replay clock.
        batch = list(islice(records, args.chunksize))
        started = last_marker = last_report = time.monotonic()
        report("replay_started", topic=topics.input, target_rows_s=args.rate, delay_ms=delay,
               producer_pid=os.getpid(), **delivery.counts())
        try:
            send_progress(producer, delivery, topics.input, advertised, delay)
            while batch:
                if args.rate:
                    deadline = started + (delivery.raw_queued + len(batch)) / args.rate
                    while (remaining := deadline - time.monotonic()) > 0:
                        time.sleep(min(remaining, 0.05))
                        producer.poll(0)
                        delivery.check()
                batch_wall_ns = time.time_ns()
                for record in batch:
                    kind, _, _ = classify_row(record)
                    if kind == PRICE:
                        frontier = max(frontier, record["trade_us"])
                    record["replay_wall_ns"] = batch_wall_ns
                    key = record["symbol"] or f'{record["source_index"]}:{record["source_row"]}'
                    enqueue(
                        producer, delivery, topic=topics.input, key=key.encode(),
                        value=json.dumps(record, separators=(",", ":")).encode(),
                        timestamp=advertised, on_delivery=delivery.callback,
                    )
                    delivery.raw_queued += 1
                now = time.monotonic()
                if now - last_marker >= args.progress_interval and frontier // 1000 > advertised:
                    advertised = frontier // 1000
                    send_progress(producer, delivery, topics.input, advertised, delay)
                    last_marker = now
                producer.poll(0)
                delivery.check()
                if now - last_report >= 1:
                    report("delivery_totals", **delivery.counts())
                    last_report = now
                batch = list(islice(records, args.chunksize))
            if delivery.raw_queued != expected["raw_rows"] or frontier != expected["latest_trade_us"]:
                raise RuntimeError("selected input changed after preflight")
            terminal = window_end_ms(frontier) + delay + 1 if frontier else advertised
            send_progress(producer, delivery, topics.input, terminal, delay, terminal=True)
            pending = producer.flush(20)
            delivery.check()
            if pending or delivery.raw_acked != delivery.raw_queued or delivery.markers_acked != delivery.markers_queued:
                raise RuntimeError(f"replay ended with {pending} pending messages or unverified acknowledgements")
        except BaseException:
            pending = producer.flush(20)
            report("replay_failed", pending=pending, last_error=delivery.last_error, **delivery.counts())
            raise

    elapsed = time.monotonic() - started
    report("replay_finished", elapsed_s=elapsed, acknowledged_rows_s=delivery.raw_acked / elapsed,
           **delivery.counts())


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    add_selection_arguments(parser)
    parser.add_argument("--ordered-db", type=Path, help="Use a replay prepared with --prepare-order")
    parser.add_argument("--prepare-order", type=Path, help="Write a stable time-ordered replay and exit")
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--broker", "-b", default="localhost:9092")
    parser.add_argument("--topic-prefix", "--topic", "-t", default=DEFAULT_PREFIX)
    parser.add_argument("--rate", "-r", type=float, default=200.0, help="Target raw rows/s, or 0 for burst")
    parser.add_argument("--chunksize", "-c", type=int, default=50, help="Number of rows per pacing batch")
    parser.add_argument("--progress-interval", type=float, default=0.2, help="Minimum seconds between markers")
    parser.add_argument("--lateness-seconds", type=float, default=DEFAULT_LATENESS_SECONDS)
    args = parser.parse_args()
    try:
        validate_selection(args)
        lateness_ms(args.lateness_seconds)
        if bool(args.files) == bool(args.ordered_db):
            raise ValueError("choose either --files or --ordered-db")
        if args.ordered_db and (
            not args.ordered_db.is_file() or args.prepare_order
            or args.skip_rows or args.max_rows or args.subset != 100
        ):
            raise ValueError("--ordered-db requires an existing database; select rows during preparation")
        if args.prepare_order and args.preflight_only:
            raise ValueError("choose either --preflight-only or --prepare-order")
        if (
            not math.isfinite(args.rate) or args.rate < 0 or args.chunksize < 1
            or not math.isfinite(args.progress_interval) or args.progress_interval <= 0
        ):
            raise ValueError("require rate >= 0, chunksize >= 1 and progress interval > 0")
        Topics.from_prefix(args.topic_prefix)
    except ValueError as exc:
        parser.error(str(exc))
    return args


def main():
    args = parse_args()
    if args.prepare_order:
        report("order_prepared", **prepare_order(args))
        return

    started = time.monotonic()
    expected = preflight(projected_rows(args))
    report("preflight", elapsed_s=time.monotonic() - started, **expected)
    if args.preflight_only:
        return
    if not expected["raw_rows"]:
        raise SystemExit("selected input contains no data rows")
    measured_delay = (expected["max_backward_us"] + 999) // 1000
    if lateness_ms(args.lateness_seconds) < measured_delay:
        raise SystemExit(
            f"preflight needs {measured_delay} ms for this input order; set the same delay on "
            "producer and worker, or prepare a stable ordered replay with --prepare-order"
        )
    run_replay(args, Topics.from_prefix(args.topic_prefix), expected)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
