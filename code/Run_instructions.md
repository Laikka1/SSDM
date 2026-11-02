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

## Terminal 1 - Start local infrastructure

```bash
cd code
docker compose up -d kafka flink-jobmanager flink-taskmanager
docker compose ps
```

The worker creates these topics automatically:

| Topic | Contents |
|---|---|
| `trading-events` | Producer input events |
| `trading-events-dead-letter` | Malformed, unsupported, or invalid events |
| `trading-events-late` | Valid events arriving after the configured lateness bound |
| `trading-events-windows` | Completed five-minute event-time window results |
| `trading-events-normalized` | All valid normalized events, including housekeeping |
| `trading-events-housekeeping` | Valid housekeeping/initialization events |

## Terminal 2 - Start the Flink worker

Run this from the repository root:

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/streamworker.py \
  --engine flink \
  --consumer-group trading-flink-preparation \
  --input-topics trading-events \
  --starting-offsets earliest \
  --parallelism 1 \
  --dead-letter-topic trading-events-dead-letter \
  --late-topic trading-events-late \
  --window-topic trading-events-windows \
  --normalized-topic trading-events-normalized \
  --housekeeping-topic trading-events-housekeeping \
  --lateness-seconds 3600
```

`--lateness-seconds 300` configures the bounded out-of-orderness watermark.
The event timestamp comes from each row's `Date` and `Time`, interpreted in
the exchange timezone: Paris (`FR`), Amsterdam (`NL`), or Frankfurt (`ETR`).
It is not replaced with processing time. Events that arrive after the
watermark are routed to the late-event topic; the window operator does not
reopen an already emitted window.

## Terminal 3 - Produce a replay

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/producer.py \
  --topic trading-events \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 10000 \
  --rate 300

# For a larger replay, omit --max-rows or use --subset 1.
# A positive --rate intentionally throttles the replay.
```

For a clean test, use a new input topic in both the worker and producer
commands, or remove/reset the old Kafka topics. With
`--starting-offsets earliest`, the worker reads all records available in the
consumer group from the beginning.

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
```

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
lateness bound. Flink's metric group contains counters for received,
normalized, valid, rejected, late, event-type, exchange, and missing/invalid
price records. For a small replay, compare the number and contents of
window records against the CSV rows grouped by exchange-qualified symbol and
five-minute local event-time interval.
