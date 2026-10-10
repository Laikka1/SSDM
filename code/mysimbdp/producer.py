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

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from mysimbdp.utils import get_json_logger

_running = True
SUPPORTED_TENANT = "tenant_a"


def _handle_sigint(signum, frame):
    global _running
    _running = False


signal.signal(signal.SIGINT, _handle_sigint)
signal.signal(signal.SIGTERM, _handle_sigint)


def delivery_callback(err, msg):
    """Called once per message to indicate delivery result."""
    if err is not None:
        print(f"Message delivery failed: {err}", file=sys.stderr)


def derive_record_type(filepath: str) -> str:
    """Derive _record_type from the filename (first token of stem, lowercased)."""
    stem = os.path.basename(filepath).split(".")[0].lower()
    return stem.split("_")[0] if "_" in stem else stem


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Publish CSV rows to a Kafka topic"
    )
    parser.add_argument(
        "--tenant",
        "-n",
        required=True,
        choices=[SUPPORTED_TENANT],
        help="Tenant identifier (tenant_a only)",
    )
    parser.add_argument("--files", "-f", nargs="+", required=True,
                        help="One or more paths to CSV files (.csv.gz or .csv)")
    parser.add_argument("--topic", "-t", required=True, help="Kafka topic name")
    parser.add_argument("--broker", "-b", default="localhost:9092", help="Kafka broker address")
    parser.add_argument(
        "--subset", "-s", type=float, default=100.0,
        help=(
            "Testing convenience: percentage of total rows to use as a row cap "
            "(0 < x <= 100). At 100 (default) no cap is applied and all rows from "
            "the provided files are emitted."
        ),
    )
    parser.add_argument("--chunksize", "-c", type=int, default=500)
    parser.add_argument(
        "--rate", "-r", type=float, default=200.0,
        help=(
            "Target rows to produce per second "
            "(default: 200 for steadier streaming simulation; 0 = burst mode)."
        ),
    )
    parser.add_argument(
        "--event-time-source",
        choices=["produced_at", "last_review", "last_review_or_produced_at"],
        default="produced_at",
        help=(
            "Compatibility option. Event time is always set to produced_at "
            "in the current implementation."
        ),
    )
    return parser.parse_args()


def ensure_topic_exists(broker: str, topic: str, logger) -> None:
    admin = AdminClient({"bootstrap.servers": broker})
    metadata = admin.list_topics(timeout=10)
    if topic in metadata.topics:
        return

    futures = admin.create_topics(
        [NewTopic(topic=topic, num_partitions=1, replication_factor=1)],
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

    logger = get_json_logger("producer", args.tenant)

    max_rows_per_file: dict[str, int] = {}
    if args.subset < 100.0:
        for f in args.files:
            file_total = sum(
                len(chunk)
                for chunk in pd.read_csv(f, chunksize=args.chunksize, usecols=[0])
            )
            max_rows_per_file[f] = math.ceil(file_total * args.subset / 100)
        logger.info(
            f"Testing mode: subset={args.subset}% → per-file caps: "
            + ", ".join(f"{os.path.basename(f)}={max_rows_per_file[f]}" for f in args.files)
        )
    
    logger.info(
        f"Starting producer for {args.tenant} → {args.topic} ({len(args.files)} file(s))"
    )
    if args.event_time_source != "produced_at":
        logger.warning(
            "Ignoring --event-time-source; event_time is fixed to produced_at"
        )
    logger.info("event_time_source=fixed_produced_at")

    # Create Kafka producer
    prod_conf = {
        "bootstrap.servers": args.broker,
        "client.id": f"producer_{args.tenant}_{args.topic}",
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
            record_type = derive_record_type(filepath)

            file_rows_sent = 0
            file_max_rows = max_rows_per_file.get(filepath)
            reader = pd.read_csv(filepath, chunksize=args.chunksize)

            for chunk_num, chunk in enumerate(reader):
                # Replace NaN with None — vectorised
                chunk = chunk.where(chunk.notna(), None)
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
                    event_key = str(record.get("id") or record.get("host_id") or file_rows_sent)
                    partition_key = str(record.get("neighbourhood") or record.get("room_type") or event_key)
                    event_time = produced_at

                    envelope = {
                        "tenant_id": args.tenant,
                        "event_key": event_key,
                        "partition_key": partition_key,
                        "event_time": str(event_time),
                        "payload": record,
                        "_source_file": source_file,
                        "_record_type": record_type,
                        "_produced_at": produced_at,
                    }

                    payload = json.dumps(envelope, default=str).encode("utf-8")
                    while _running:
                        try:
                            producer.produce(
                                args.topic,
                                key=args.tenant.encode("utf-8"),
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
                    f"{file_rows_sent} rows sent so far  [record_type={record_type}]"
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