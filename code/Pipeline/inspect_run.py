#!/usr/bin/env python3
"""Follow a replay's Kafka results and Flink metrics from outside the worker."""

import argparse
from collections import Counter, deque
from contextlib import ExitStack
from datetime import datetime, timezone
import fcntl
import json
import math
from pathlib import Path
import re
import time
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import urlopen

from confluent_kafka import KafkaException, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from kafka_io import INPUT_PARTITIONS, Topics, result_consumer
from normalization import TERMINAL_CLASSES

LOG_ROOT = Path(__file__).resolve().parents[1] / "logs"
FAILED_STATES = {"CANCELED", "CANCELLING", "FAILED", "FAILING", "FINISHED", "RESTARTING", "SUSPENDED"}
OUTPUT_COUNTERS = {"EMA": "ema_published", "BUY": "buy_published", "SELL": "sell_published"}
UNAVAILABLE = "unavailable"
METRIC_REQUEST_LIMIT = 3000


def timestamp():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def clean(value):
    return str(value).replace("\r", r"\r").replace("\n", r"\n")


def fields(data):
    return "  ".join(f"{name}={clean(value)}" for name, value in data.items())


class ResultLogs:
    def __init__(self, stack, directory, topics):
        self.topics = topics
        self.files = {}
        self.last_offset = {}
        self.counts = Counter()
        for topic, filename in zip(topics.outputs(), ("ema.log", "advisories.log", "diagnostics.log")):
            file = stack.enter_context((directory / filename).open("a+", buffering=1, encoding="utf-8"))
            file.seek(0)
            last = deque(file, maxlen=1)
            self.last_offset[topic] = int(last[0].split()[1].split("=", 1)[1]) if last else -1
            self.files[topic] = file

    def record(self, message):
        topic = message.topic()
        row = json.loads(message.value())
        if topic == self.topics.diagnostics:
            self.counts["diagnostics"] += 1
            details = {"type": row["kind"], "reason": row["reason"]}
            details.update({key: value for key, value in row.items() if key not in ("kind", "reason")})
        else:
            result = "EMA" if topic == self.topics.windows else row["advice"]
            self.counts[result] += 1
            details = {
                "type": result, "symbol": row["symbol"], "window_end": row["window_end"],
                "close": row["close"], "EMA38": row["ema38"], "EMA100": row["ema100"],
            }
            if result == "EMA":
                details.update(source_file=row["source_file"], source_row=row["source_row"])
            else:
                details.update(previous_EMA38=row["previous_ema38"], previous_EMA100=row["previous_ema100"],
                               reason=row["reason"])
        offset = message.offset()
        if offset > self.last_offset[topic]:
            print(f"{timestamp()} offset={offset}  {fields(details)}", file=self.files[topic])
            self.last_offset[topic] = offset


class ProducerJournal:
    def __init__(self, path):
        self.path = path
        self.position = 0
        self.latest = None
        self.terminal = None
        self.finished = None
        self.failed = None

    def poll(self):
        if not self.path.is_file():
            return
        with self.path.open(encoding="utf-8") as file:
            file.seek(self.position)
            while line := file.readline():
                if not line.endswith("\n"):
                    break
                self.position = file.tell()
                entry = json.loads(line)
                event = entry["event"]
                if event in {"replay_started", "delivery_totals", "replay_finished"}:
                    self.latest = entry
                if event == "progress_acked" and entry.get("terminal"):
                    self.terminal = entry
                if event == "replay_finished" and self.finished is None:
                    self.finished = time.monotonic()
                if event == "replay_failed":
                    self.failed = entry.get("last_error") or "producer interrupted or exited with an error"


class FlinkJobGone(RuntimeError):
    pass


class Flink:
    def __init__(self, base_url, topic):
        self.base_url = base_url.rstrip("/")
        self.topic = topic
        self.job_id = None
        self.partition_metric = re.compile(rf"\.KafkaSourceReader\.topic\.{re.escape(topic)}\.partition\.(\d+)\.currentOffset$")

    def get(self, path, **params):
        query = "?" + urlencode(params) if params else ""
        with urlopen(self.base_url + path + query, timeout=2) as response:
            return json.load(response)

    def job(self):
        if self.job_id is None:
            jobs = self.get("/jobs/overview")["jobs"]
            matches = [job for job in jobs if job["name"] == f"trading-replay:{self.topic}"]
            if not matches:
                return None
            self.job_id = max(matches, key=lambda job: job["start-time"])["jid"]
        try:
            return self.get(f"/jobs/{self.job_id}")
        except HTTPError as exc:
            if exc.code == 404:
                raise FlinkJobGone(f"Flink REST no longer reports job {self.job_id}") from exc
            raise

    def subscriptions(self):
        config = self.get(f"/jobs/{self.job_id}/config")["execution-config"]
        return config.get("user-config", {}).get("trading.subscriptions", UNAVAILABLE)

    def failure(self):
        data = self.get(f"/jobs/{self.job_id}/exceptions")
        lines = (data.get("root-exception") or "").splitlines()
        if not lines:
            return "Check flink.log or the Flink UI for the worker error."
        return next((line for line in reversed(lines) if line.startswith("Caused by:")), lines[0])

    def metric_samples(self, path, names):
        # Flink's REST server limits the HTTP request line. The vertex endpoint
        # returns a metric name per subtask, so one query can exceed that limit.
        empty_request_size = len(path) + len("?get=")
        request_size = empty_request_size
        batch = []
        for name in names:
            encoded_size = len(urlencode({"get": name})) - len("get=")
            if batch and request_size + len("%2C") + encoded_size > METRIC_REQUEST_LIMIT:
                yield from self.get(path, get=",".join(batch))
                batch.clear()
                request_size = empty_request_size
            request_size += encoded_size + (len("%2C") if batch else 0)
            batch.append(name)
        if batch:
            yield from self.get(path, get=",".join(batch))

    def metrics(self, job):
        offsets, watermarks = {}, {}
        counts = Counter()
        window_parallelism = 0
        for vertex in job["vertices"]:
            name = vertex["name"]
            is_window = name.startswith("price-windows-ema-crossover")
            if is_window:
                window_parallelism = vertex["parallelism"]
            elif "kafka-replay-source" not in name and "decode-classify" not in name:
                continue

            path = f"/jobs/{self.job_id}/vertices/{vertex['id']}/metrics"
            ids = [item["id"] for item in self.get(path)]
            wanted = [
                key for key in ids
                if ".trading." in key or self.partition_metric.search(key)
                or (is_window and key.endswith(".currentOutputWatermark"))
            ]
            for metric in self.metric_samples(path, wanted):
                key = metric["id"]
                try:
                    value = int(metric["value"])
                except (TypeError, ValueError):
                    continue
                partition = self.partition_metric.search(key)
                if partition:
                    offsets[int(partition[1])] = value + 1
                elif ".trading." in key:
                    counts[key.rsplit(".", 1)[1]] += value
                else:
                    subtask = int(key.split(".", 1)[0])
                    if key.count(".") == 1 or ".price-windows-ema-crossover." in key:
                        watermarks[subtask] = max(watermarks.get(subtask, value), value)

        watermark = min(watermarks.values()) if window_parallelism and len(watermarks) == window_parallelism else None
        return offsets, counts, watermark


def progress_fields(offsets, ends, counts, watermark, journal, previous, observed):
    now = time.monotonic()
    acked = journal.latest["raw_acked"] if journal.latest else None
    raw_read = counts.get("raw_received")

    rates = {"producer_rows_s": UNAVAILABLE, "flink_rows_s": UNAVAILABLE}
    if previous:
        elapsed = now - previous[0]
        for key, current, old in (("producer_rows_s", acked, previous[1]), ("flink_rows_s", raw_read, previous[2])):
            if elapsed > 0 and current is not None and old is not None and current >= old:
                rates[key] = f"{(current - old) / elapsed:.1f}"

    lag = {p: ends[p] - offset for p, offset in offsets.items() if p in ends}
    lag_available = len(lag) == INPUT_PARTITIONS and min(lag.values()) >= 0
    total = sum(lag.values()) if lag_available else None
    busiest = min(lag, key=lambda p: (-lag[p], p)) if total else None
    producer_state = "finished" if journal.finished is not None else "replaying" if journal.latest else "waiting"
    raw_backlog = acked - raw_read if acked is not None and raw_read is not None else None
    sample = {
        "producer_state": producer_state,
        "partitions": f"{len(offsets)}/{INPUT_PARTITIONS}",
        "lag_records": total if total is not None else UNAVAILABLE,
        "busiest_partition": (
            f"{busiest} ({lag[busiest]} records)" if busiest is not None
            else "none" if total == 0 else UNAVAILABLE
        ),
        "raw_acked": acked if acked is not None else UNAVAILABLE,
        "raw_read": raw_read if raw_read is not None else UNAVAILABLE,
        "raw_backlog": raw_backlog if raw_backlog is not None and raw_backlog >= 0 else UNAVAILABLE,
        **rates,
        **{kind: counts.get(kind, UNAVAILABLE) for kind in TERMINAL_CLASSES},
        "price_windows": counts.get("price_windows_completed", UNAVAILABLE),
        "ema": observed["EMA"], "buy": observed["BUY"], "sell": observed["SELL"],
        "diagnostics_sampled": observed["diagnostics"],
        "diagnostics_suppressed": sum(value for key, value in counts.items()
                                      if key.startswith("diagnostics_suppressed_")) if counts else UNAVAILABLE,
        "terminal_markers": counts.get("terminal_markers", UNAVAILABLE),
        "window_watermark": (
            datetime.fromtimestamp(watermark / 1000, timezone.utc).isoformat(timespec="milliseconds")
            if watermark is not None and watermark > -(2 ** 63) else UNAVAILABLE
        ),
    }
    return total, sample, (now, acked, raw_read)


def terminal_partitions_read(journal, offsets):
    if journal.terminal is None:
        return 0
    return sum(offsets.get(int(partition), 0) > offset
               for partition, offset in journal.terminal["offsets"].items())


def drained(journal, lag, terminal_reads, counters, watermark, observed):
    if journal.finished is None or terminal_reads != INPUT_PARTITIONS or lag not in (None, 0):
        return False
    final = journal.latest
    return (
        watermark is not None
        and watermark >= journal.terminal["eligible_watermark_ms"]
        and counters.get("terminal_markers") == INPUT_PARTITIONS
        and counters.get("progress_markers") == final["markers_acked"]
        and counters.get("raw_received") == final["raw_acked"]
        and sum(counters.get(kind, 0) for kind in TERMINAL_CLASSES) == final["raw_acked"]
        and (not counters.get("price_accepted") or "price_windows_completed" in counters)
        and all(counters.get(metric, 0) == observed[name] for name, metric in OUTPUT_COUNTERS.items())
        and sum(n for key, n in counters.items() if key.startswith("diagnostics_sampled_")) == observed["diagnostics"]
    )


def monitor(args, topics, results, worker_log):
    def log(event, **data):
        line = f"{timestamp()} {event}  {fields(data)}"
        print(line, flush=True)
        print(line, file=worker_log)

    flink = Flink(args.rest_url, topics.input)
    journal = ProducerJournal(args.directory / "producer.jsonl")
    consumer = result_consumer(args.broker, "trading-inspector")
    admin_config = {"bootstrap.servers": args.broker}
    admin = AdminClient(admin_config)
    requested = {TopicPartition(topics.input, p): OffsetSpec.latest() for p in range(INPUT_PARTITIONS)}
    started = time.monotonic()
    last_job_seen = started
    next_sample = started
    announced = ready = False
    previous = None
    log("WAITING", prefix=topics.input, flink_rest=args.rest_url, broker=args.broker)

    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING) for topic in topics.outputs()])
        while True:
            message = consumer.poll(0.1)
            if message is not None:
                if message.error():
                    log("KAFKA_ERROR", error=message.error())
                else:
                    results.record(message)
            if time.monotonic() < next_sample:
                continue
            next_sample = time.monotonic() + args.interval
            journal.poll()
            if journal.failed:
                log("FAILED", producer_error=journal.failed)
                return 1

            try:
                job = flink.job()
                if job is not None:
                    last_job_seen = time.monotonic()
                if job is None:
                    log("WAITING", reason="Flink job has not registered for this prefix")
                elif job["state"] in FAILED_STATES:
                    log("FAILED", job_id=flink.job_id, state=job["state"], error=flink.failure())
                    return 1
                else:
                    if not announced:
                        log("STARTED", job_id=flink.job_id, state=job["state"],
                            parallelism=job["vertices"][0]["parallelism"], subscriptions=flink.subscriptions())
                        announced = True
                    offsets, counts, watermark = flink.metrics(job)
                    try:
                        ends = {
                            tp.partition: result.result(timeout=3).offset
                            for tp, result in admin.list_offsets(requested, request_timeout=2).items()
                        }
                    except (KafkaException, TimeoutError) as exc:
                        # A new client discovers current leaders after a broker epoch change.
                        admin = AdminClient(admin_config)
                        ends = {}
                        log("KAFKA_OFFSETS_UNAVAILABLE", error=exc)
                    if not ready and job["state"] == "RUNNING" and len(offsets) == len(ends) == INPUT_PARTITIONS:
                        log("READY", job_id=flink.job_id, prefix=topics.input, partitions=INPUT_PARTITIONS)
                        ready = True
                    lag, sample, previous = progress_fields(
                        offsets, ends, counts, watermark, journal, previous, results.counts,
                    )
                    terminal_reads = terminal_partitions_read(journal, offsets)
                    sample["terminal_partitions_read"] = f"{terminal_reads}/{INPUT_PARTITIONS}"
                    sample["elapsed_s"] = f"{time.monotonic() - started:.1f}"
                    if drained(journal, lag, terminal_reads, counts, watermark, results.counts):
                        log("DRAINED", **sample)
                        return 0
                    log("PROGRESS", state=job["state"], **sample)
            except FlinkJobGone as exc:
                log("FAILED", job_id=flink.job_id, error=exc, details="Check the Flink logs for this replay.")
                return 1
            except (URLError, TimeoutError, KafkaException) as exc:
                previous = None
                log("UNAVAILABLE", lag_records=UNAVAILABLE, error=exc)

            now = time.monotonic()
            if not ready and now - started >= args.timeout:
                log("FAILED", reason="worker did not become ready", details="Check flink.log and the topic prefix.")
                return 1
            if ready and now - last_job_seen >= args.timeout:
                log("FAILED", job_id=flink.job_id, reason="worker stopped responding",
                    details="Check the Flink logs for this replay.")
                return 1
            if journal.finished is not None and now - journal.finished >= args.timeout:
                log("FAILED", reason="replay did not drain", details="Check the last progress line and flink.log.")
                return 1
    finally:
        consumer.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--topic-prefix", required=True)
    parser.add_argument("--broker", default="localhost:9092")
    parser.add_argument("--rest-url", default="http://127.0.0.1:8082")
    parser.add_argument("--log-root", type=Path, default=LOG_ROOT)
    parser.add_argument("--interval", type=float, default=2, help="Seconds between progress reports")
    parser.add_argument("--timeout", type=float, default=90,
                        help="Seconds to wait for readiness, worker response, or final drain")
    args = parser.parse_args()
    try:
        topics = Topics.from_prefix(args.topic_prefix)
        if any(not math.isfinite(value) or value <= 0 for value in (args.interval, args.timeout)):
            raise ValueError("--interval and --timeout must be positive and finite")
    except ValueError as exc:
        parser.error(str(exc))
    args.directory = args.log_root / topics.input
    args.directory.mkdir(parents=True, exist_ok=True)
    with ExitStack() as stack:
        worker_log = stack.enter_context((args.directory / "worker.log").open("a", buffering=1, encoding="utf-8"))
        try:
            fcntl.flock(worker_log, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"An inspector is already writing {worker_log.name}.") from None
        results = ResultLogs(stack, args.directory, topics)
        try:
            return monitor(args, topics, results, worker_log)
        except KeyboardInterrupt:
            return 130


if __name__ == "__main__":
    raise SystemExit(main())
