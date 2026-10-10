# Local Implementation Plan: CSV to Flink Infrastructure

## 1. Scope

This plan covers only the infrastructure needed to move the Trading Data CSV
files into a running Flink job locally:

```text
Trading Data CSV files
        |
        v
Python CSV producer
        |
        v
Kafka trading-events topic
        |
        v
Flink Kafka source / consumer
        |
        v
Running Flink job
```

This plan does **not** implement or specify the internal normalization, five-minute
windows, EMA38/EMA100 calculations, crossover detection, subscriptions, or
buy/sell advisories. Those are the next implementation layer after this pipeline
is reliable.

The target deployment is local Docker Compose. The producer runs from the host
Python environment, while Kafka and Flink run in Docker. Cassandra is deferred
and is not required for this first working pipeline.

## 2. Recommendation about Cassandra

### Decision: keep Cassandra optional and defer its setup

Cassandra is useful later for durable historical output, dashboard reads, and
restarting without losing completed window results. It is not needed to prove
that CSV data reaches a Flink job, and adding it now would introduce another
service, schema, connection lifecycle, and failure mode before the streaming
path is working.

The recommended sequence is:

1. First implement and validate CSV -> Kafka -> Flink without Cassandra.
2. Add Cassandra only when the Flink job produces normalized events or query
   results that need durable storage.
3. Keep the future Cassandra writer behind a configuration flag so local tests
   can run with `--no-cassandra`.

The current `cassandra_insert.py` can be retained as a later adapter, but its
current tenant/group schema should not be treated as the final trading schema.
It is not a blocker for this infrastructure milestone.

## 3. Responsibilities by component

### 3.1 CSV producer

The producer is a host-side Python process that reads one or more daily Trading
Data CSV files incrementally and publishes one Kafka message per CSV row.

Responsibilities for this milestone:

- Accept one or more CSV paths on the command line.
- Read with `pandas.read_csv(..., chunksize=...)` so a daily file is not loaded
  entirely into memory.
- Preserve the original row fields instead of applying query logic.
- Add transport metadata needed by downstream Flink:
  - stable event identifier;
  - source file name and source day;
  - original trading date and trading time;
  - producer timestamp for operational latency measurements;
  - a Kafka message key derived from the exchange-qualified instrument id when
    available.
- Publish to one configurable input topic, for example
  `trading-events`.
- Support `--rate 0` for maximum-throughput replay and a positive rate for
  controlled experiments.
- Support `--subset` or a row limit for smoke tests.
- Report rows read, rows published, delivery failures, elapsed time, and
  achieved rows/second.
- Flush outstanding Kafka messages and exit with a non-zero status on fatal
  errors.

The producer must not use Airbnb-specific fields such as `neighbourhood`,
`room_type`, `last_review`, or `price` as required fields. The trading row can
be transported as a payload even before domain normalization is implemented.
The source trading timestamp must remain available; it must not be replaced by
`produced_at`.

### 3.2 Kafka broker and topic

Kafka is the transport boundary between the file replay process and Flink.
Kafka responsibilities for this milestone are limited to receiving, buffering,
and replaying the CSV-derived events.

Recommended local topic configuration:

| Setting | Initial local value | Reason |
|---|---:|---|
| Topic | `trading-events` | Clear domain-specific input name |
| Partitions | 3 | One initial partition per exchange-scale test path; easy to increase |
| Replication factor | 1 | Appropriate for a single local broker |
| Producer delivery | `acks=all` | Detect broker acceptance reliably |
| Compression | `snappy` or `lz4` | Reduce transport volume during full replay |
| Retention | Local default or 1-2 days | Enough for replay/debugging without unbounded growth |

The producer should use the symbol/exchange identifier as the Kafka key when it
can extract one. This establishes the intended partitioning contract for the
future keyed Flink operators. If the field is missing, use a deterministic
fallback event id and record the missing-key count.

The first smoke test can use one partition. The full infrastructure test should
use multiple partitions and verify that Flink consumes all of them.

### 3.3 Flink Kafka source / consumer

A separate Python Kafka consumer is not needed for the main pipeline. The Flink
Kafka source is the Kafka consumer: it belongs inside the Flink job and provides
checkpoint-compatible offsets and parallel consumption.

The Flink source should:

- connect to the broker using the Docker-reachable bootstrap address;
- consume `trading-events` using a named consumer group;
- support `earliest` for deterministic replay tests and `latest` for live tests;
- deserialize each Kafka value as UTF-8 JSON during the initial milestone;
- retain the original payload and transport metadata;
- count malformed messages and route them to a log or future dead-letter path;
- expose source records to a temporary downstream sink so end-to-end delivery
  can be verified before query operators exist.

The consumer group must be configurable. A new group with `earliest` offsets is
used for replay validation; a stable group with `latest` is used for live-style
runs.

### 3.4 Flink job

The first Flink job is a connectivity and transport job, not the final trading
analytics job. It should have the following minimal operator chain:

```text
KafkaSource
  -> JSON deserialization
  -> lightweight transport validation
  -> temporary output sink
```

The temporary output sink can initially be one of:

- stdout/task-manager logs for a small smoke test;
- a Kafka `trading-events-verified` topic for a replayable handoff;
- a simple file sink for local inspection.

The preferred development path is to emit a compact verified record to a second
Kafka topic or log only during small tests. For the full 289-million-event
replay, avoid printing every message because logging becomes the bottleneck.
Use counters and periodic metrics instead.

The Flink environment should initially use a conservative local parallelism,
then make parallelism configurable. The Kafka source parallelism must not exceed
the input topic partition count in the first tests. The job should log the
configured broker, topic, group id, offset mode, parallelism, and connector jar.

Event-time watermarks and query operators are intentionally postponed. The
transport milestone only needs to prove that the original timestamp fields
survive the Kafka and Flink boundaries. Event-time assignment will be added
when normalization and window/query logic are implemented.

### 3.5 Stream worker processes

Do not add an independent Python Kafka consumer for the main data path. It would
compete with the Flink consumer group and duplicate offset, retry, and scaling
responsibilities.

Keep one lightweight launcher/orchestrator if useful. Its responsibilities may
be:

- parse runtime arguments;
- ensure the input and output topics exist;
- download or locate the Flink Kafka connector jar;
- submit/start the Flink job;
- configure the consumer group and offset mode;
- initialize structured logs and metrics.

The existing `streamworker.py` can serve this role. The actual consumption and
stream processing should remain in `streamworker_flink.py` so Flink owns the
runtime, task distribution, and Kafka offsets.

An additional Python worker is only justified later for a separate concern such
as a REST/API service, subscription management, or a non-Flink archival path.
It is not needed for CSV-to-Flink delivery.

## 4. Existing code changes by file

### `code/mysimbdp/producer.py`

Adapt the producer from Airbnb rows to Trading Data rows:

- remove the tenant-only restriction and Airbnb-specific CLI language;
- accept Trading Data CSV files and preserve all source columns in `payload`;
- derive the event id deterministically from row identity plus source file/row
  position when no native id exists;
- derive the Kafka key from `ID.[Exchange]` or the equivalent source column;
- preserve trading date/time fields in the envelope;
- keep `_produced_at` separately for operational metrics;
- keep chunking, rate limiting, delivery callbacks, retries, and flush behavior;
- report source-file row counts and total published rows;
- use a domain-neutral envelope that the future normalization operator can read.

The producer's output contract for this milestone should be documented and
stable. The producer does not need to calculate event type or query values yet.

### `code/mysimbdp/streamworker.py`

Reuse this module as the runtime entrypoint and configuration layer:

- replace `TENANT_RULES` with trading pipeline configuration;
- rename tenant/event-topic arguments to domain-neutral input-topic arguments;
- retain topic creation and connector-jar management;
- retain starting-offset selection;
- retain structured logging and periodic metrics;
- remove or isolate Airbnb-specific normalization and aggregation helpers;
- add a transport-test mode that starts the Flink job without Cassandra;
- make parallelism, input topic, output topic, and consumer group configurable.

This module should remain thin. It should launch/configure the Flink job rather
than become a second stream-processing implementation.

### `code/mysimbdp/streamworker_flink.py`

Reuse the existing Kafka source setup and replace the Airbnb aggregation path
with the minimal transport operator chain:

- retain `KafkaSource`, deserialization, connector-jar setup, and job execution;
- remove the dependency on neighbourhood/grouping and average-price logic for
  the infrastructure milestone;
- preserve the trading payload and transport metadata;
- add a counter/log for received records and malformed JSON;
- add a low-volume verification sink;
- make source parallelism and consumer group configurable;
- leave clear extension points for later event-time assignment and keyed query
  operators.

The existing `TumblingEventTimeWindows` code should not be activated as part of
this milestone because the query/window semantics are intentionally out of
scope here.

### `code/mysimbdp/cassandra_insert.py`

Do not make Cassandra a required dependency of the first pipeline test.
Possible implementation choices:

- retain the module unchanged but do not instantiate it;
- move its use behind a `--with-cassandra` or equivalent feature flag;
- later replace its schema with symbol/window/advisory tables once query output
  exists.

### `code/mysimbdp/alerting.py` and `utils.py`

Reuse these for structured logs, safe JSON serialization, and metrics. Do not
publish buy/sell alerts yet. At this stage, metrics should describe transport:
received, published, deserialization failures, source lag if available, and
throughput.

## 5. Docker Compose setup

The current Compose stack already contains Kafka, Flink JobManager, and Flink
TaskManager, which is sufficient for the first milestone. Cassandra should be
removed from the required startup command or left stopped unless another part of
the repository needs it.

The Compose setup should be checked for the following local concerns:

1. Kafka advertised listeners must be reachable from the host producer and from
   Flink containers. Host and container bootstrap addresses may need separate
   configuration values.
2. Kafka must be healthy before the producer or Flink job starts.
3. Flink JobManager must be healthy before the TaskManager starts.
4. The Flink containers need the Python runtime and dependencies required by the
   PyFlink job, or the launch strategy must clearly run the Python job from the
   host against the local Flink cluster.
5. Connector-jar acquisition must be deterministic and should support a local
   `FLINK_KAFKA_CONNECTOR_JAR` override when internet access is unavailable.
6. Volumes should preserve Kafka data during routine restarts but allow a
   documented clean reset for repeatable tests.

The local startup target is:

```bash
cd code
docker compose up -d kafka flink-jobmanager flink-taskmanager
docker compose ps
```

The exact service names and Python execution mode should be verified before
updating the run instructions. No cloud deployment is part of this plan.

## 6. Implementation phases

### Phase 1: establish a repeatable local stack

- Start Kafka and Flink through Docker Compose.
- Confirm Kafka broker reachability from the host.
- Confirm Flink JobManager and TaskManager are running.
- Confirm the Kafka connector jar and PyFlink versions match.
- Add a short health-check command sequence to `Run_instructions.md`.

**Exit condition:** services start reliably twice in a row and the Flink UI is
reachable on the configured local port.

### Phase 2: adapt the producer transport contract

- Replace Airbnb field assumptions in `producer.py`.
- Add Trading Data file validation and stable event metadata.
- Publish a small subset to `trading-events`.
- Inspect several Kafka messages and verify that source timestamps and symbol
  identifiers are present.

**Exit condition:** a 100-1,000-row test publishes successfully with zero
unexpected delivery failures and correct source metadata.

### Phase 3: run the minimal Flink consumer job

- Replace the current average/window path with source, deserialization, and
  verification sink operators.
- Run with `earliest` offsets and a fresh consumer group.
- Verify Flink receives the same number of test messages that Kafka accepted.
- Test malformed-message behavior without stopping the job.

**Exit condition:** the Flink job remains running, consumes the test topic, and
reports received and rejected counts.

### Phase 4: test replay and throughput

- Run a larger file slice with rate limiting.
- Run burst mode with `--rate 0`.
- Test one and multiple Kafka partitions.
- Vary Flink parallelism within the local machine's capacity.
- Record producer rate, Flink receive rate, consumer lag, CPU, memory, and
  failures.

**Exit condition:** the pipeline can consume a substantial slice without
unbounded lag or memory growth, and the bottleneck is identified.

### Phase 5: prepare extension points for analytics

- Freeze the transport envelope and topic contract.
- Add event-time extraction only after the contract is verified.
- Add keyed normalization and query operators in the next plan/implementation
  stage.
- Decide whether completed results need Cassandra based on the dashboard and
  historical-query requirements.

## 7. Verification checklist

### Connectivity

- Kafka accepts messages from the host producer.
- Flink connects using the configured bootstrap server.
- The Flink Kafka connector version matches the Flink runtime.
- Flink TaskManager registers with JobManager.

### Data transport

- Every test message has a stable event id.
- Source file, trading date, trading time, and symbol identifier survive the
  producer-to-Flink boundary.
- Producer timestamp is separate from source trading timestamp.
- Kafka keys are present for keyed records.
- Malformed records are counted and do not crash the job.

### Operational behavior

- `earliest` replays a test topic deterministically.
- `latest` consumes newly produced records without replaying old data.
- A producer restart does not silently lose buffered messages after flush.
- A Flink job restart with a new group behaves as documented.
- The full replay does not print one log line per event.
- Cassandra is not required for the transport pipeline to start.

## 8. Final recommendation

Implement the first milestone as a deliberately small pipeline:

```text
producer.py -> Kafka trading-events -> streamworker.py ->
streamworker_flink.py -> verification sink/logs
```

Reuse the existing Kafka publishing, topic creation, connector setup, Flink
source, logging, and metrics patterns. Remove Airbnb-specific assumptions from
the transport boundary, but postpone Cassandra and all trading analytics until
this path is demonstrably correct. This keeps the core infrastructure useful,
keeps local setup simple, and leaves a clean place to add normalization and
Query 1/Query 2 afterward.
