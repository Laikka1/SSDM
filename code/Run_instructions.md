# Trading Event Preparation Pipeline

## Prerequisites

- Python 3.11 virtual environment
- Docker with Compose
- Trading Data files in `data/`
- The Flink Kafka connector jar is downloaded automatically when the worker
  starts if it is missing.
- If you run without internet access, set `FLINK_KAFKA_CONNECTOR_JAR` to a
  local jar path.

## Setup

1. Install Python 3.11 and create a dedicated virtual environment.

```bash
sudo apt update
sudo apt install -y software-properties-common
sudo add-apt-repository -y ppa:deadsnakes/ppa
sudo apt update
sudo apt install -y python3.11 python3.11-venv

python3.11 -m venv .venv311
source .venv311/bin/activate
python -m pip install -U pip wheel
python -m pip install -r code/requirements.txt
```

The repository does not need to keep the Kafka connector jars checked in.

## Terminal 1 - Start Kafka

```bash
cd code
docker compose up -d kafka
docker compose ps
```

The current worker uses a local PyFlink MiniCluster, so the Docker Flink
JobManager and TaskManager are not required.

The worker creates these topics automatically:

| Topic | Contents |
|---|---|
| `trading-events` | Producer input events |
| `trading-events-dead-letter` | Malformed, unsupported, or invalid events |
| `trading-events-late` | Valid events arriving after the configured lateness bound |
| `trading-events-windows` | Completed five-minute event-time window results |
| `trading-events-normalized` | All valid normalized events, including housekeeping |
| `trading-events-housekeeping` | Valid housekeeping/initialization events |

The current Python worker runs the Flink job locally, so both Kafka addresses
must use the host-published Kafka port:

```bash
--broker localhost:9092 --flink-broker localhost:9092
```

## Terminal 2 - Start the Flink worker

Run this from the repository root. Keep this terminal running for the full
replay:

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/streamworker.py \
  --engine flink \
  --broker localhost:9092 \
  --flink-broker localhost:9092 \
  --consumer-group trading-flink-preparation-run2 \
  --input-topics trading-events \
  --starting-offsets earliest \
  --parallelism 1 \
  --dead-letter-topic trading-events-dead-letter \
  --late-topic trading-events-late \
  --window-topic trading-events-windows \
  --normalized-topic trading-events-normalized \
  --housekeeping-topic trading-events-housekeeping \
  --lateness-seconds 60 \
  --progress-file code/logs/streamworker_progress.jsonl
```

`--lateness-seconds 300` configures the bounded out-of-orderness watermark.
The event timestamp comes from each row's `Date` and `Time`, interpreted in
the exchange timezone: Paris (`FR`), Amsterdam (`NL`), or Frankfurt (`ETR`).
It is not replaced with processing time. Events that arrive after the
watermark are routed to the late-event topic; the window operator does not
reopen an already emitted window.

The worker writes its startup configuration, a liveness heartbeat every 15
seconds, normalization totals, analytical totals with the current event-time
watermark, completed-window totals, and Flink stage lifecycle to a timestamped
`code/logs/streamworker_*.log` file. It also creates/appends
`code/logs/EMA.log` at startup; completed windows are recorded there with their
symbol, close, EMA38, and EMA100 values.

Progress is exchanged from the local Flink Python operators through
`code/logs/streamworker_progress.jsonl` and aggregated by the host worker.
Each progress line reports received, normalized, valid, rejected, analytical,
late, and completed-window totals plus the current watermark. Kafka sink
retries and a bounded Flink restart strategy handle transient broker
disconnects; unrecoverable failures are logged with their root exception.

Keep the worker terminal running while records are processed. To follow the
worker and progress logs from another terminal:

```bash
tail -f code/logs/streamworker_*.log
tail -f code/logs/streamworker_progress.jsonl
```

## Terminal 3 - Produce a replay

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/producer.py \
  --topic trading-events \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 200000 \
  --rate 1000

# For a larger replay, omit --max-rows or use --subset 1.
# A positive --rate intentionally throttles the replay.
```

For a clean replay, use a new consumer group, for example
`trading-flink-preparation-run2`, or use a new input topic in both commands.
With `--starting-offsets earliest`, the worker reads all records available to
that consumer group from the beginning.

## Terminal 4 - Verify results

Inspect a few input, dead-letter, late-event, and window records:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trading-events --from-beginning --max-messages 3

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trading-events-dead-letter --from-beginning --max-messages 5

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trading-events-late --from-beginning --max-messages 5

docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trading-events-windows --from-beginning --max-messages 5

tail -n 50 code/logs/streamworker_*.log
tail -n 50 code/logs/EMA.log
```

### Inspect Kafka topic counts

Check topic names and partition counts:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list

for topic in trading-events trading-events-normalized \
  trading-events-housekeeping trading-events-dead-letter \
  trading-events-late trading-events-windows; do
  echo "=== $topic ==="
  docker exec kafka /opt/kafka/bin/kafka-get-offsets.sh \
    --bootstrap-server localhost:9092 \
    --topic "$topic"
done
```

The `kafka-get-offsets.sh` output shows the latest offset for each partition.
Sum the partition offsets to estimate the number of records available. The
worker's streamworker log reports analytical records every 1,000 records and
includes the current event-time watermark.

### How to interpret the output

- A normalized valid event has `event_id`, an exchange-qualified `symbol`
  such as `A0HN5C.ETR`, an exchange, security type, an ISO timezone-aware
  `event_time`, an `event_type`, available attributes, and source metadata.
- All structurally valid events are normalized and written to the normalized
  topic. Housekeeping/initialization events are also written to the
  housekeeping topic and counted for observability, but do not advance the
  analytical event-time watermark. Trade and quote events enter the
  symbol-keyed five-minute windows. Only positive finite `Last` trade values
  are used for EMA price selection.
- A dead-letter record has a `reason_code`, `details`, original envelope and
  payload. These records indicate normalization or validation failures and
  should not stop the job.
- A late-event record has `reason_code: "late_event"` and preserves the
  normalized event identity. It indicates that the event was beyond the
  configured event-time lateness bound.
- A window record is emitted after the event-time watermark passes the
  window end. Its `window_start` and `window_end` are five-minute boundaries
  aligned to midnight in the event's exchange timezone. It includes
  `event_count`, `event_type_counts`, `valid_price_count`,
  `last_trade_price`, and source/time metadata. `last_trade_price` is the
  valid `Last` value with the greatest event timestamp in that window; it is
  the price input intended for later EMA calculation.
- The normalized topic is the complete valid-event view. The housekeeping
  topic makes initialization/housekeeping records explicit so their unusual
  timestamps can be inspected without treating them as market-event time.
- A row with a valid `Last` plus Ask/Bid values is one `trade` event; the
  additional attributes are retained and are not treated as duplicate events.
- `Last` values classify as `trade` events and contribute to
  `valid_price_count`. Ask/Bid-only and empty payload rows are retained as
  quote or housekeeping events. Quote events are included in analytical
  windows but do not contribute to EMA prices; housekeeping events are
  retained in the normalized/housekeeping views.
- Zero-valued price fields in this dataset are treated as unavailable
  placeholders: they remain in the original payload but do not qualify as
  analytical prices. Negative, non-numeric, or non-finite price fields are
  rejected with a reason code.

The worker log records the configured topics, parallelism, connector, and
lateness bound. Its consolidated progress lines contain counters for received,
normalized, valid, rejected, analytical, late, and completed-window records,
plus the current event-time watermark. Flink's metric group also contains
counters for event type, exchange, and missing/invalid price records. For a small replay, compare the number and contents of
window records against the CSV rows grouped by exchange-qualified symbol and
five-minute local event-time interval.
