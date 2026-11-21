#!/usr/bin/env python3
"""Replay every selected CSV row and advertise partition-ordered event-time progress."""

import argparse
from contextlib import closing
import csv
from itertools import islice
import json
import math
from pathlib import Path
import sys
import time

import psutil

from calculations import WINDOW_MS, window_end_ms
from inspect_run import process_usage, replay_clock_id
from kafka_io import DEFAULT_PREFIX, INPUT_PARTITIONS, Topics, create_producer, enqueue, verify_topics
from normalization import PRICE, classify_row, project_row

REQUIRED_FIELDS = {"SecType", "Date", "Time", "Last", "Ask", "Bid", "Trading date", "Trading time"}
INITIAL_PROGRESS_MS = 1  # Kafka clients use a zero timestamp to mean wall-clock time.


def csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.DictReader(line for line in file if not line.startswith("#"))
        fields = reader.fieldnames
        if fields is None:
            raise ValueError(f"{path}: missing CSV header")
        reader.fieldnames = [name.strip() for name in fields]
        fields = set(reader.fieldnames)
        missing = sorted(REQUIRED_FIELDS - fields)
        if not fields.intersection({"ID", "ID.[Exchange]"}):
            missing.append("either ID or ID.[Exchange]")
        if missing:
            raise ValueError(f"{path}: missing required CSV fields: {', '.join(missing)}")
        yield from reader


def projected_rows(args):
    stop = args.skip_rows + args.max_rows if args.max_rows is not None else None
    for source_index, path in enumerate(args.files):
        with closing(csv_rows(path)) as rows:
            selected = islice(rows, args.skip_rows, stop)
            for source_row, row in enumerate(selected, start=args.skip_rows + 1):
                yield project_row(row, path.name, source_index, source_row)


class Delivery:
    def __init__(self):
        self.raw_queued = self.raw_acked = self.markers_queued = self.markers_acked = 0
        self.failed = 0
        self.last_error = None

    def callback(self, err, _msg, *, progress=False):
        if err is not None:
            self.failed += 1
            self.last_error = str(err)
        elif progress:
            self.markers_acked += 1
        else:
            self.raw_acked += 1

    def check(self):
        if self.failed:
            raise RuntimeError(f"{self.failed} delivery failures; last Kafka error: {self.last_error}")

    def counts(self):
        return dict(raw_queued=self.raw_queued, raw_acked=self.raw_acked,
                    markers_queued=self.markers_queued, markers_acked=self.markers_acked,
                    failed=self.failed)


def send_progress(producer, delivery, topic, timestamp_ms):
    value = json.dumps({"kind": "progress", "frontier_ms": timestamp_ms}, separators=(",", ":")).encode()
    for partition in range(INPUT_PARTITIONS):
        enqueue(
            producer, delivery, topic=topic, value=value,
            key=b"", partition=partition, timestamp=timestamp_ms,
            on_delivery=lambda err, msg: delivery.callback(err, msg, progress=True),
        )
        delivery.markers_queued += 1


def kafka_messages(records):
    with closing(records):
        for record in records:
            kind, _, _ = classify_row(record)
            key = record["symbol"] or f'{record["source_index"]}:{record["source_row"]}'
            yield key.encode(), record["trade_us"] if kind == PRICE else 0, json.dumps(
                record, separators=(",", ":")
            ).encode()


def wait_until_deadline(producer, delivery, deadline_s):
    while (remaining_s := deadline_s - time.monotonic()) > 0:
        time.sleep(min(remaining_s, 0.05))
        producer.poll(0)
        delivery.check()


def run_replay(args, journal):
    def report(event, **fields):
        line = json.dumps({"event": event, **fields})
        print(line, file=journal, flush=True)
        print(line, flush=True)

    def report_totals(event, **fields):
        report(event, **fields, monotonic_ns=time.monotonic_ns(), **process_usage(process), **delivery.counts())

    topics = args.topics
    process = psutil.Process()
    delivery = Delivery()
    latest_trade_us = 0
    next_boundary_us = WINDOW_MS * 1000
    boot = replay_clock_id()
    last_progress_ms = INITIAL_PROGRESS_MS
    with closing(kafka_messages(projected_rows(args))) as records:
        # Open the input and apply --skip-rows before starting the replay clock.
        batch = list(islice(records, args.chunksize))
        if not batch:
            raise SystemExit("selected input contains no data rows")
        verify_topics(topics)
        producer = create_producer(topics.input)
        started_s = last_marker_s = last_report_s = time.monotonic()
        report_totals(
            "replay_started", topic=topics.input, target_rows_s=args.rate,
            files=[str(path) for path in args.files], skip_rows=args.skip_rows, max_rows=args.max_rows,
        )
        try:
            send_progress(producer, delivery, topics.input, last_progress_ms)
            while batch:
                if args.rate:
                    deadline_s = started_s + (delivery.raw_queued + len(batch)) / args.rate
                    wait_until_deadline(producer, delivery, deadline_s)
                for key, trade_us, payload in batch:
                    if trade_us > latest_trade_us:
                        latest_trade_us = trade_us
                        if latest_trade_us >= next_boundary_us:
                            boundary_ms = window_end_ms(latest_trade_us) - WINDOW_MS
                            report("window_frontier", window_end_ms=boundary_ms, monotonic_ns=time.monotonic_ns(),
                                   clock_id=boot)
                            next_boundary_us = (boundary_ms + WINDOW_MS) * 1000
                    # Publish the batch before advertising its event-time progress.
                    enqueue(
                        producer, delivery, topic=topics.input, key=key, value=payload,
                        timestamp=last_progress_ms, on_delivery=delivery.callback,
                    )
                    delivery.raw_queued += 1
                now_s = time.monotonic()
                latest_trade_ms = latest_trade_us // 1000
                if now_s - last_marker_s >= 0.2 and latest_trade_ms > last_progress_ms:
                    last_progress_ms = latest_trade_ms
                    send_progress(producer, delivery, topics.input, last_progress_ms)
                    last_marker_s = now_s
                producer.poll(0)
                delivery.check()
                if now_s - last_report_s >= 1:
                    report_totals("delivery_totals", elapsed_seconds=round(now_s - started_s, 3))
                    last_report_s = now_s
                batch = list(islice(records, args.chunksize))
            if latest_trade_ms > last_progress_ms:
                send_progress(producer, delivery, topics.input, latest_trade_ms)
            pending = producer.flush(20)
            delivery.check()
            if pending or delivery.raw_acked != delivery.raw_queued or delivery.markers_acked != delivery.markers_queued:
                raise RuntimeError(f"replay ended with {pending} pending messages or unverified acknowledgements")
        except BaseException:
            pending = producer.flush(20)
            report_totals("replay_failed", pending=pending, last_error=delivery.last_error)
            raise

    report_totals("replay_finished", elapsed_seconds=round(time.monotonic() - started_s, 3))


def main():
    parser = argparse.ArgumentParser(description=__doc__, allow_abbrev=False)
    parser.add_argument("--files", nargs="+", type=Path, required=True)
    parser.add_argument("--max-rows", type=int, help="Maximum rows per file after skipping")
    parser.add_argument("--skip-rows", type=int, default=0, help="Initial data rows to skip per file")
    parser.add_argument("--topic-prefix", dest="topics", metavar="PREFIX", type=Topics.from_prefix, default=DEFAULT_PREFIX)
    parser.add_argument("--rate", type=float, default=200.0, help="Target raw rows/s, or 0 for burst")
    parser.add_argument("--chunksize", type=int, default=50, help="Rows per pacing batch")
    args = parser.parse_args()
    if args.skip_rows < 0 or (args.max_rows is not None and args.max_rows < 1):
        parser.error("use --skip-rows >= 0 and --max-rows >= 1")
    if not math.isfinite(args.rate) or args.rate < 0 or args.chunksize < 1:
        parser.error("require a finite rate >= 0 and chunksize >= 1")
    for path in args.files:
        if not path.is_file():
            parser.error(f"input file does not exist: {path}")
    args.topics.log_dir.mkdir(parents=True, exist_ok=True)
    with (args.topics.log_dir / "producer.jsonl").open("a", encoding="utf-8") as journal:
        run_replay(args, journal)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
