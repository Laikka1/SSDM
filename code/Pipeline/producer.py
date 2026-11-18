#!/usr/bin/env python3

import argparse
import json
import math
import os
import signal
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils import get_json_logger, safe_json_dumps

_running = True
DEFAULT_TOPIC = "trading-events"


def _handle_sigint(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _handle_sigint)
signal.signal(signal.SIGTERM, _handle_sigint)


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)


def _first_value(record: dict, names: tuple[str, ...]):
    for name in names:
        if name in record and record[name] not in (None, ""):
            return record[name]
    return None


def _symbol_key(record: dict, fallback: str) -> str:
    value = _first_value(record, ("ID.[Exchange]", "ID", "id", "symbol"))
    return str(value).strip() if value not in (None, "") else fallback


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish Trading Data CSV rows to Kafka"
    )
    parser.add_argument("--files", "-f", nargs="+", required=True,
                        help="One or more Trading Data CSV paths")
    parser.add_argument("--topic", "-t", default=DEFAULT_TOPIC, help="Kafka topic name")
    parser.add_argument("--broker", "-b", default="localhost:9092", help="Kafka broker address")
    parser.add_argument(
        "--subset", "-s", type=float, default=100.0,
        help=(
            "Testing convenience: percentage of rows to publish per file "
            "(0 < x <= 100). At 100 (default) no cap is applied and all rows from "
            "the provided files are emitted."
        ),
    )
    parser.add_argument(
        "--max-rows", type=int, default=None,
        help="Maximum rows to publish per file; skips the full-file subset scan",
    )
    parser.add_argument(
        "--skip-rows", type=int, default=0,
        help="Testing only: skip this many data rows at the start of each file",
    )
    parser.add_argument("--chunksize", "-c", type=int, default=500)
    parser.add_argument(
        "--rate", "-r", type=float, default=200.0,
        help=(
            "Target rows to produce per second "
            "(default: 200 for steadier streaming simulation; 0 = burst mode)."
        ),
    )
    return parser.parse_args()


def ensure_topic_exists(broker: str, topic: str, logger) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        return

    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=3, replication_factor=1)],
        request_timeout=15,
    )

    try:
        futures[topic].result()
        logger.info(f"Created Kafka topic {topic}")
    except Exception as exc:
        error_text = str(exc)
        if "TOPIC_ALREADY_EXISTS" in error_text or "already exists" in error_text:
            return
        raise RuntimeError(f"Unable to create Kafka topic '{topic}': {exc}") from exc


def main():
    args = parse_args()

    # Validate inputs
    for f in args.files:
        if not os.path.isfile(f):
            print(f"ERROR: File not found: {f}", file=sys.stderr)
            sys.exit(1)
    if not (0 < args.subset <= 100):
        print(f"ERROR: --subset must be in (0, 100], got {args.subset}", file=sys.stderr)
        sys.exit(1)
    if args.chunksize < 1:
        print(f"ERROR: --chunksize must be >= 1, got {args.chunksize}", file=sys.stderr)
        sys.exit(1)
    if args.max_rows is not None and args.max_rows < 1:
        print(f"ERROR: --max-rows must be >= 1, got {args.max_rows}", file=sys.stderr)
        sys.exit(1)
    if args.skip_rows < 0:
        print(f"ERROR: --skip-rows must be >= 0, got {args.skip_rows}", file=sys.stderr)
        sys.exit(1)

    logger = get_json_logger("producer", "trading")

    max_rows_per_file: dict[str, int] = {}
    if args.max_rows is not None:
        max_rows_per_file = {filepath: args.max_rows for filepath in args.files}
        logger.info(f"Testing mode: max_rows={args.max_rows} per file")
    elif args.subset < 100.0:
        for f in args.files:
            file_total = sum(
                len(chunk)
                for chunk in pd.read_csv(
                    f, chunksize=args.chunksize, usecols=[0], comment="#", index_col=False
                )
            )
            max_rows_per_file[f] = math.ceil(file_total * args.subset / 100)
        logger.info(
            f"Testing mode: subset={args.subset}% → per-file caps: "
            + ", ".join(f"{os.path.basename(f)}={max_rows_per_file[f]}" for f in args.files)
        )
    if args.skip_rows:
        logger.info(
            f"Testing mode: skipping first {args.skip_rows} data rows per file"
        )
    
    logger.info(
        f"Starting Trading Data producer -> {args.topic} ({len(args.files)} file(s))"
    )

    # Create Kafka producer
    prod_conf = {
        "bootstrap.servers": args.broker,
        "client.id": f"trading-producer_{args.topic}",
        "acks": "all",
        "message.send.max.retries": 5,
        "retry.backoff.ms": 50,
        "request.timeout.ms": 30000,
        "delivery.timeout.ms": 120000,
        "queue.buffering.max.messages": 100000,
        "queue.buffering.max.kbytes": 65536,
        "batch.num.messages": 200,
        "linger.ms": 20,
        "compression.type": "snappy",
    }

    logger.info(f"chunksize={args.chunksize} batch={prod_conf['batch.num.messages']} "
                f"linger={prod_conf['linger.ms']}ms")

    ensure_topic_exists(args.broker, args.topic, logger)

    producer = Producer(prod_conf)

    grand_total_sent = 0
    start_time = time.time()

    # Pre-compute per-record sleep duration for rate limiting (0 = disabled)
    row_sleep = (1.0 / args.rate) if args.rate > 0 else 0.0
    if row_sleep:
        logger.info(f"Rate limit: {args.rate:.1f} rows/s → {row_sleep*1000:.1f}ms sleep between records")

    try:
        total_files = len(args.files)

        # ── Process each file sequentially ──
        for file_index, filepath in enumerate(args.files, start=1):
            source_file = os.path.basename(filepath)
            file_rows_sent = 0
            file_max_rows = max_rows_per_file.get(filepath)
            reader = pd.read_csv(
                filepath, chunksize=args.chunksize, comment="#", index_col=False
            )
            rows_to_skip = args.skip_rows

            for chunk_num, chunk in enumerate(reader):
                if rows_to_skip:
                    skipped_from_chunk = min(rows_to_skip, len(chunk))
                    rows_to_skip -= skipped_from_chunk
                    chunk = chunk.iloc[skipped_from_chunk:]
                    if chunk.empty:
                        continue
                # Replace NaN with None — vectorised
                chunk = chunk.astype(object).where(pd.notna(chunk), None)
                records = chunk.to_dict("records")

                cap_reached = False
                for record in records:
                    if not _running:
                        raise KeyboardInterrupt

                    if file_max_rows is not None and file_rows_sent >= file_max_rows:
                        logger.info(
                            f"[file {file_index}/{total_files}] {source_file}: "
                            f"per-file cap reached ({file_max_rows} rows) — moving to next file."
                        )
                        cap_reached = True
                        break

                    produced_at = datetime.now(timezone.utc).isoformat()
                    row_number = file_rows_sent + 1
                    event_key = f"{source_file}:{row_number}"
                    symbol = _symbol_key(record, event_key)

                    envelope = {
                        "event_key": event_key,
                        "symbol": symbol,
                        "payload": record,
                        "_source_file": source_file,
                        "_source_row": row_number,
                        "_produced_at": produced_at,
                    }

                    payload = safe_json_dumps(envelope).encode("utf-8")
                    while _running:
                        try:
                            producer.produce(
                                args.topic,
                                key=symbol.encode("utf-8"),
                                value=payload,
                                callback=delivery_callback,
                            )
                            break
                        except BufferError:
                            producer.poll(0.05)
                    file_rows_sent += 1
                    if row_sleep:
                        time.sleep(row_sleep)

                producer.poll(0)

                logger.info(
                    f"[file {file_index}/{total_files}] {source_file}  chunk {chunk_num + 1}: "
                    f"{file_rows_sent} rows sent so far"
                )

                if cap_reached:
                    break

            producer.flush()
            grand_total_sent += file_rows_sent

    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user")
    except Exception as e:
        logger.error(f"Fatal error: {e}", extra={"extra": {"error": str(e)}})
        sys.exit(1)

    elapsed = time.time() - start_time
    logger.info(
        f"Producer finished: {grand_total_sent} rows in {elapsed:.1f}s  "
        f"[files={len(args.files)}  topic={args.topic}  throughput={grand_total_sent / elapsed:.0f} rows/s]"
    )


if __name__ == "__main__":
    main()