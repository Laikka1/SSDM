# Sprint Implementation Plan: Trading Event Preparation and Windows

## Scope

This sprint extends the existing CSV → Kafka → Flink pipeline up to validated,
event-time, symbol-keyed five-minute windows:

```text
CSV producer
  -> Kafka trading-events
  -> Flink normalization and validation
  -> dead-letter output for invalid events
  -> event-time watermarks and bounded lateness
  -> keyBy(symbol)
  -> five-minute tumbling windows
```

This sprint does **not** implement EMA calculations, crossover detection,
advisories, subscriptions, Cassandra persistence, dashboards, or other later
query logic.

## Requirements and implementation

### 1. Normalize and classify events

The Flink stream must convert each producer envelope into a stable normalized
event containing at least:

- `event_id`
- exchange-qualified `symbol` from `ID.[Exchange]`
- `exchange`
- `security_type`
- `trading_date`
- timezone-aware `event_time`
- `last_price` when present and numeric
- `event_type`
- source file/row metadata and `produced_at`

The Trading Data date and time fields are interpreted as CEST and converted to
the internal timestamp representation without replacing event time with
processing time. Event type is derived from the populated payload attributes
(for example, ask, bid, last/trade, or housekeeping). The original payload is
retained for traceability.

Normalization must reject unsupported or ambiguous symbol/exchange formats,
invalid security types, invalid dates/times, and invalid numeric values needed
for the identified event type.

### 2. Validate normalized events

Validation runs immediately after normalization. A valid event has a stable
identifier, a valid exchange-qualified symbol, supported security type, valid
CEST-derived event time, and a recognized event type. Price events that
participate in window calculations must have a usable `Last` value.

Validation must return an explicit result:

- valid events continue to the event-time stream;
- invalid events are converted to a dead-letter record;
- no malformed event may stop the Flink job;
- every rejection includes a reason code and enough source metadata for replay
  and diagnosis.

### 3. Dead-letter topic and counters

Invalid events are published to a configurable Kafka dead-letter topic, using
the `event_id` or deterministic source identity as the message key. The
dead-letter payload includes the reason code, validation details, original
envelope/payload, source file and row, and rejection time.

The Flink job must maintain observable counters for at least:

- records received;
- records normalized;
- valid records;
- rejected records;
- rejected records by reason;
- records by event type;
- records by exchange;
- records with missing or invalid prices.

Dead-letter publishing must not silently discard failures. Producer errors are
logged and exposed through the job's failure/metric path.

### 4. Event time, watermarks, and lateness

Valid events use the normalized Trading Data event timestamp. The stream must
assign timestamps and generate bounded-out-of-orderness watermarks before
windowing.

The lateness bound is configurable and recorded in the run configuration. The
initial local value should be conservative and validated with replay data.
Events arriving beyond the configured bound must follow one documented policy:
send them to a late-event/dead-letter output with a reason code and counter, or
route them to a separately observable late-event path. They must not be silently
lost.

The implementation must expose late-event counts and preserve event-time
identity in output records. Processing time may be recorded for latency
measurement but must not determine window membership.

### 5. Symbol-keyed five-minute tumbling windows

Key the valid event stream by the exchange-qualified `symbol`, ensuring that
all events for one instrument are processed together. Use non-overlapping
five-minute event-time tumbling windows aligned to midnight CEST:

```text
window_start = midnight_CEST(date)
             + floor(seconds_since_midnight / 300) * 300 seconds
window_end   = window_start + 300 seconds
```

A window is emitted when its event-time watermark passes the window end. The
window result for this sprint should contain the symbol, exchange,
security type, window bounds, event count, event-type counts, valid-price
count, and source/processing metadata sufficient for verification.

The window operator should establish the state/output boundary that later
analytics can consume. It must not calculate EMA values or emit trading
advisories in this sprint.

## Component relationships

- [producer.py](../code/Pipeline/producer.py) remains responsible for reading
  CSV rows and preserving source fields and transport metadata.
- [streamworker.py](../code/Pipeline/streamworker.py) configures topics,
  consumer settings, lateness, parallelism, and the dead-letter destination.
- [streamworker_flink.py](../code/Pipeline/streamworker_flink.py) owns
  normalization, validation, dead-letter routing, counters, timestamps,
  watermarks, keyed streams, and windows.
- Kafka provides the input and dead-letter boundaries. A separate Python
  consumer is not added to the main processing path.
- The current Cassandra and alerting adapters remain out of scope until later
  query results exist.

## Acceptance criteria

1. A replayed sample produces normalized events with correct CEST-derived
   timestamps, symbols, exchanges, security types, and event types.
2. Invalid records remain observable in the dead-letter topic with reason codes;
   the job continues running.
3. Counters distinguish received, valid, rejected, late, and event-type records.
4. Watermarks and the configured lateness bound are visible in logs or metrics.
5. Valid events are grouped into the correct symbol and five-minute event-time
   windows, including midnight and exact-boundary timestamps.
6. Window output is reproducible against a small reference calculation.
7. The implementation does not contain EMA, crossover, advisory, subscription,
   Cassandra, or dashboard work from later sprints.
