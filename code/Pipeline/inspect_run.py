"""Write Kafka results and Flink progress to the local run directory."""

from bisect import bisect_left
from collections import Counter, deque
from datetime import datetime, timezone
import fcntl
import json
from pathlib import Path
import re
import time
from urllib.error import URLError
from urllib.parse import urlencode
from urllib.request import urlopen

import psutil
from confluent_kafka import KafkaException, OFFSET_BEGINNING, TopicPartition
from confluent_kafka.admin import AdminClient, OffsetSpec

from kafka_io import BROKER, INPUT_PARTITIONS, result_consumer
from normalization import MARKET_ZONES, TERMINAL_CLASSES

REST_PORT = 8082
EXCHANGE_COUNTS = tuple(f"price_accepted_{exchange}" for exchange in MARKET_ZONES)
PROGRESS_COUNTS = (*TERMINAL_CLASSES, *EXCHANGE_COUNTS, "missing_trading_date_fallbacks")
LOGGED_COUNTERS = {*PROGRESS_COUNTS, "raw_received", "price_windows_completed"}
FAILED_STATES = {"CANCELED", "CANCELLING", "FAILED", "SUSPENDED"}
UNAVAILABLE = "unavailable"
METRIC_REQUEST_LIMIT = 3000


def replay_clock_id():
    # Monotonic timestamps can be compared across processes on the same Linux boot.
    try:
        return Path("/proc/sys/kernel/random/boot_id").read_text().strip()
    except OSError:
        return None


def process_usage(process):
    with process.oneshot():
        cpu = process.cpu_times()
        return {
            "pid": process.pid, "name": process.name(), "created": process.create_time(),
            "cpu_seconds": cpu.user + cpu.system, "rss_bytes": process.memory_info().rss,
        }


def timestamp():
    return datetime.now().astimezone().isoformat(timespec="milliseconds")


def clean(value):
    return str(value).replace("\r", r"\r").replace("\n", r"\n")


def fields(data):
    return "  ".join(f"{name}={clean(value)}" for name, value in data.items())


class BacklogAge:
    """Estimate the age of the oldest unread record from sampled partition end offsets."""

    def __init__(self, partitions):
        self.history = [deque() for _ in range(partitions)]

    def sample(self, offsets, ends, now):
        ages = []
        for partition, history in enumerate(self.history):
            end = ends.get(partition)
            if end is None:
                continue
            if history and end < history[-1][1]:
                history.clear()
            # Keep the first and last observation of a plateau to bracket bursts.
            if len(history) >= 2 and history[-2][1] == history[-1][1] == end:
                history.pop()
            history.append((now, end))
            offset = offsets.get(partition)
            if offset is None or offset < 0 or offset > end:
                continue
            while len(history) > 1 and history[1][1] <= offset:
                history.popleft()
            if offset == end:
                ages.append(0.0)
            elif len(history) >= 2 and history[0][1] <= offset:
                (before, low), (after, high) = history[0], history[1]
                arrival = before + (after - before) * (offset - low) / (high - low)
                ages.append(now - arrival)
        return max(ages) if len(ages) == len(self.history) else None


class WindowTimings:
    """Match a live result to the producer's first crossing of its window boundary."""

    def __init__(self, stack, path):
        self.stack = stack
        self.path = path
        self.file = None
        self.boot = replay_clock_id()
        self.started_ns = time.monotonic_ns()
        self.ends = []
        self.times = []

    def read_new_frontiers(self):
        if self.file is None:
            try:
                self.file = self.stack.enter_context(self.path.open(encoding="utf-8"))
            except FileNotFoundError:
                return
        while True:
            position = self.file.tell()
            line = self.file.readline()
            if not line.endswith("\n"):
                self.file.seek(position)
                break
            if '"window_frontier"' not in line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not isinstance(row, dict) or row.get("event") != "window_frontier" or row.get("clock_id") != self.boot:
                continue
            end, stamp = row.get("window_end_ms"), row.get("monotonic_ns")
            if type(end) is int and type(stamp) is int and (not self.ends or self.ends[-1] < end):
                self.ends.append(end)
                self.times.append(stamp)

    def seconds(self, window_end_ms, received_ns):
        if self.boot is None or window_end_ms is None:
            return None
        self.read_new_frontiers()
        # A trade can cross several boundaries. Use the first frontier at or beyond this window.
        index = bisect_left(self.ends, window_end_ms)
        if index < len(self.times) and self.started_ns <= self.times[index] <= received_ns:
            return (received_ns - self.times[index]) / 1e9
        return None


def recover_last_offset(file):
    """Recover the last logged Kafka offset and discard an interrupted final write."""
    file.seek(0)
    last_offset = -1
    while True:
        line_start = file.tell()
        line = file.readline()
        if not line:
            return last_offset
        if not line.endswith("\n"):
            file.truncate(line_start)
            return last_offset
        parts = line.split(maxsplit=2)
        if len(parts) == 3 and parts[1].startswith("offset="):
            offset = parts[1].removeprefix("offset=")
            if offset.isascii() and offset.isdecimal():
                last_offset = int(offset)


class ResultLogs:
    def __init__(self, stack, topics):
        directory = topics.log_dir
        directory.mkdir(parents=True, exist_ok=True)
        self.worker = stack.enter_context((directory / "worker.log").open("a", buffering=1, encoding="utf-8"))
        try:
            fcntl.flock(self.worker, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            raise SystemExit(f"A worker already owns {directory}.") from None
        self.process = psutil.Process()
        self.resources = stack.enter_context(
            (directory / "worker_resources.jsonl").open("a", buffering=1, encoding="utf-8")
        )
        self.topics = topics
        self.files = {}
        self.last_offset = {}
        self.counts = Counter()
        self.timings = WindowTimings(stack, directory / "producer.jsonl")
        for topic, filename in ((topics.windows, "ema.log"), (topics.advisories, "advisories.log")):
            file = stack.enter_context(
                (directory / filename).open("a+", buffering=1, encoding="utf-8", errors="surrogateescape")
            )
            self.last_offset[topic] = recover_last_offset(file)
            self.files[topic] = file

    def log(self, event, **data):
        line = f"{timestamp()} {event}  {fields(data)}"
        print(line, file=self.worker)
        print(line, flush=True)

    def sample_resources(self):
        processes = []
        for process in [self.process, *self.process.children(recursive=True)]:
            try:
                processes.append(process_usage(process))
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        print(json.dumps({"time": timestamp(), "monotonic_ns": time.monotonic_ns(), "processes": processes}),
              file=self.resources)

    def record(self, message):
        topic = message.topic()
        row = json.loads(message.value())
        result = "EMA" if topic == self.topics.windows else row["advice"]
        # Rebuild the observed totals when Kafka replays records already on disk.
        self.counts[result] += 1
        offset = message.offset()
        if offset <= self.last_offset[topic]:
            return
        details = {
            "type": result, "symbol": row["symbol"], "window_end": row["window_end"],
            "close": row["close"], "EMA38": row["ema38"], "EMA100": row["ema100"],
        }
        if result == "EMA":
            details.update(source_file=row["source_file"], source_row=row["source_row"])
        else:
            details.update(previous_EMA38=row["previous_ema38"], previous_EMA100=row["previous_ema100"],
                           reason=row["reason"])
        delay = self.timings.seconds(row.get("window_end_ms"), time.monotonic_ns())
        details["window_to_visible_seconds"] = round(delay, 3) if delay is not None else UNAVAILABLE
        print(f"{timestamp()} offset={offset}  {fields(details)}", file=self.files[topic])
        self.last_offset[topic] = offset


class Flink:
    def __init__(self, job_id, topic):
        self.base_url = f"http://127.0.0.1:{REST_PORT}"
        self.job_id = job_id
        self.partition_metric = re.compile(rf"\.KafkaSourceReader\.topic\.{re.escape(topic)}\.partition\.(\d+)\.currentOffset$")

    def get(self, path, **params):
        query = "?" + urlencode(params) if params else ""
        with urlopen(self.base_url + path + query, timeout=2) as response:
            return json.load(response)

    def failure(self):
        data = self.get(f"/jobs/{self.job_id}/exceptions")
        lines = (data.get("root-exception") or "").splitlines()
        if not lines:
            return "Check the worker terminal or the Flink UI for the error."
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
                if (".trading." in key and key.rsplit(".", 1)[1] in LOGGED_COUNTERS)
                or self.partition_metric.search(key)
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


def progress_fields(offsets, ends, counts, watermark, observed, lag_seconds):
    lag = {p: ends[p] - offset for p, offset in offsets.items() if p in ends}
    lag_available = len(lag) == INPUT_PARTITIONS and min(lag.values()) >= 0
    total = sum(lag.values()) if lag_available else None
    busiest = min(lag, key=lambda p: (-lag[p], p)) if total else None
    return {
        "partitions": f"{len(offsets)}/{INPUT_PARTITIONS}",
        "partition_offsets": ",".join(str(offsets.get(p, UNAVAILABLE)) for p in range(INPUT_PARTITIONS)),
        "lag_records": total if total is not None else UNAVAILABLE,
        "lag_seconds": round(lag_seconds, 3) if total is not None and lag_seconds is not None else UNAVAILABLE,
        "busiest_partition": (
            f"{busiest} ({lag[busiest]} records)" if busiest is not None
            else "none" if total == 0 else UNAVAILABLE
        ),
        "raw_read": counts.get("raw_received", UNAVAILABLE),
        **{kind: counts.get(kind, UNAVAILABLE) for kind in PROGRESS_COUNTS},
        "price_windows": counts.get("price_windows_completed", UNAVAILABLE),
        "ema": observed["EMA"], "buy": observed["BUY"], "sell": observed["SELL"],
        "window_watermark": (
            datetime.fromtimestamp(watermark / 1000, timezone.utc).isoformat(timespec="milliseconds")
            if watermark is not None and watermark > -(2 ** 63) else UNAVAILABLE
        ),
    }


class ProgressSampler:
    def __init__(self, topic):
        self.admin_config = {"bootstrap.servers": BROKER}
        self.admin = AdminClient(self.admin_config)
        self.backlog = BacklogAge(INPUT_PARTITIONS)
        self.requested = {
            TopicPartition(topic, partition): OffsetSpec.latest()
            for partition in range(INPUT_PARTITIONS)
        }

    def sample(self, flink, job, results):
        offsets, counts, watermark = flink.metrics(job)
        try:
            ends = {
                tp.partition: result.result(timeout=3).offset
                for tp, result in self.admin.list_offsets(self.requested, request_timeout=2).items()
            }
        except (KafkaException, TimeoutError) as exc:
            # A new client discovers current leaders after a broker epoch change.
            self.admin = AdminClient(self.admin_config)
            ends = {}
            results.log("KAFKA_OFFSETS_UNAVAILABLE", error=exc)
        lag_seconds = self.backlog.sample(offsets, ends, time.monotonic())
        sample = progress_fields(offsets, ends, counts, watermark, results.counts, lag_seconds)
        complete = len(offsets) == len(ends) == INPUT_PARTITIONS
        return sample, complete


def monitor(args, client, results):
    topics = args.topics
    log = results.log
    flink = Flink(str(client.get_job_id()), topics.input)
    completion = client.get_job_execution_result()
    progress = ProgressSampler(topics.input)
    consumer = result_consumer()
    started = time.monotonic()
    last_complete_sample = started
    timeout_reported = False
    next_sample = started
    ready = False
    log("WAITING", prefix=topics.input, flink_rest=flink.base_url, broker=BROKER,
        monitor_start_ns=round(started * 1e9))

    try:
        consumer.assign([TopicPartition(topic, 0, OFFSET_BEGINNING) for topic in (topics.windows, topics.advisories)])
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
            if completion.done():
                completion.result()  # Surface task failures even after the local REST server exits.
                break
            results.sample_resources()
            try:
                job = flink.get(f"/jobs/{flink.job_id}")
                state = job["state"]
                if state == "FINISHED":
                    break
                if state in FAILED_STATES:
                    log("FAILED", job_id=flink.job_id, state=state, error=flink.failure())
                    return 1
                sample, complete = progress.sample(flink, job, results)
                if state == "RUNNING" and complete:
                    last_complete_sample = time.monotonic()
                    timeout_reported = False
                    if not ready:
                        log("READY", job_id=flink.job_id, prefix=topics.input, partitions=INPUT_PARTITIONS)
                        ready = True
                log("PROGRESS", state=state, **sample, elapsed_seconds=round(time.monotonic() - started, 3))
            except (URLError, TimeoutError, KafkaException) as exc:
                log("UNAVAILABLE", lag_records=UNAVAILABLE, lag_seconds=UNAVAILABLE, error=exc)

            if not timeout_reported and time.monotonic() - last_complete_sample >= args.timeout:
                reason = "progress metrics unavailable" if ready else "waiting for all input partitions"
                log("MONITOR_TIMEOUT", job_id=flink.job_id, reason=reason, retrying=True)
                timeout_reported = True
        log("FINISHED", job_id=flink.job_id)
        return 0
    finally:
        consumer.close()
