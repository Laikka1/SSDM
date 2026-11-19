# Run the trading pipeline

Use Bash in Ubuntu WSL with Python 3.11, JDK 17, and Docker Compose.

## One-time setup

```bash
cd /home/jerry/SSDM
python3.11 -m venv .venv311
source .venv311/bin/activate
python \
  -m pip install \
  --requirement code/requirements.txt
```

Run one setup at a time. Start at Terminal 1 in the setup you choose. Wait
for `READY` in Terminal 3 before starting the producer. After `DRAINED`, run
the reference check in Terminal 4, then stop the Flink worker in Terminal 2
with Ctrl+C. Kafka can stay running between tests.

## Default run · 100,000 rows from November 8 at 1,000 rows/s

Replay the first 100,000 records from the November 8 tick file. The
worker publishes results for `INCAC.FR` and `INEUN.NL`.

### Terminal 1 · Start Kafka and prepare a fresh replay

This creates a new prefix and saves it for the other terminals. Continue
when the command prints `Replay prefix`.

```bash
(
  set -e
  cd /home/jerry/SSDM
  source .venv311/bin/activate
  PREFIX="trading-events-$(date +%Y%m%d-%H%M%S-%N)"
  DELAY=1
  mkdir -p "code/logs/$PREFIX"
  docker compose \
    --file code/docker-compose.yml \
    up \
    --detach \
    --wait \
    kafka
  python code/Pipeline/kafka_io.py \
    --topic-prefix "$PREFIX"
  printf 'export PREFIX=%q\nexport DELAY=%q\n' "$PREFIX" "$DELAY" > code/logs/current-run.env
  printf 'Replay prefix %s\n' "$PREFIX"
)
```

### Terminal 2 · Start the Flink consumer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --rest-port 8082 \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  2>&1 | tee "code/logs/$PREFIX/flink.log"
```

### Terminal 3 · Follow the live logs

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/inspect_run.py \
  --topic-prefix "$PREFIX"
```

### Terminal 4 · Run the producer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 100000 \
  --rate 1000 \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  | tee "code/logs/$PREFIX/producer.jsonl"
```

After `DRAINED`, run this in Terminal 4.

```bash
python code/Pipeline/reference.py \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 100000 \
  --topic-prefix "$PREFIX" \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  --check-kafka
```

The checker should report two EMA results and two BUYs.

## Test 1 · 50-row correctness fixture at 40 rows/s

Replay the fixture with `ABC.FR` and `SPARSE.NL` subscribed. Small batches produce
progress markers throughout the replay.

### Terminal 1 · Start Kafka and prepare a fresh replay

This creates a new prefix and saves it for the other terminals. Continue
when the command prints `Replay prefix`.

```bash
(
  set -e
  cd /home/jerry/SSDM
  source .venv311/bin/activate
  PREFIX="trading-events-$(date +%Y%m%d-%H%M%S-%N)"
  DELAY=1
  mkdir -p "code/logs/$PREFIX"
  docker compose \
    --file code/docker-compose.yml \
    up \
    --detach \
    --wait \
    kafka
  python code/Pipeline/kafka_io.py \
    --topic-prefix "$PREFIX"
  printf 'export PREFIX=%q\nexport DELAY=%q\n' "$PREFIX" "$DELAY" > code/logs/current-run.env
  printf 'Replay prefix %s\n' "$PREFIX"
)
```

### Terminal 2 · Start the Flink consumer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --rest-port 8082 \
  --subscribed-symbols ABC.FR,SPARSE.NL \
  2>&1 | tee "code/logs/$PREFIX/flink.log"
```

### Terminal 3 · Follow the live logs

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/inspect_run.py \
  --topic-prefix "$PREFIX"
```

### Terminal 4 · Run the producer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
python code/Pipeline/producer.py \
  --files code/Pipeline/fixtures/mixed.csv \
  --rate 40 \
  --chunksize 2 \
  --progress-interval 0.05 \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  | tee "code/logs/$PREFIX/producer.jsonl"
```

After `DRAINED`, run this in Terminal 4.

```bash
python code/Pipeline/reference.py \
  --files code/Pipeline/fixtures/mixed.csv \
  --topic-prefix "$PREFIX" \
  --subscribed-symbols ABC.FR,SPARSE.NL \
  --check-kafka
```

The checker should report 40 EMA results, three BUYs, and one SELL.
The assignment initializes the previous EMAs to zero. The first eligible
positive close makes EMA38 exceed EMA100 and emits a BUY when the window closes.

## Test 2 · Two real files at 5,000 rows/s with JVM windows

Replay 500,000 rows from each of the November 8 and November 9 tick
files. The skip and row limit apply per file, giving one million rows
in total. The worker publishes results for `INCAC.FR` and `INEUN.NL`.
This configuration uses four Flink subtasks and two input partitions per
reader. See the Kafka timeout settings below if the WSL clock jumps.

### Terminal 1 · Start Kafka and prepare a fresh replay

This creates a new prefix and saves it for the other terminals. Continue
when the command prints `Replay prefix`.

```bash
(
  set -e
  cd /home/jerry/SSDM
  source .venv311/bin/activate
  PREFIX="trading-events-$(date +%Y%m%d-%H%M%S-%N)"
  DELAY=1
  mkdir -p "code/logs/$PREFIX"
  docker compose \
    --file code/docker-compose.yml \
    up \
    --detach \
    --wait \
    kafka
  python code/Pipeline/kafka_io.py \
    --topic-prefix "$PREFIX"
  printf 'export PREFIX=%q\nexport DELAY=%q\n' "$PREFIX" "$DELAY" > code/logs/current-run.env
  printf 'Replay prefix %s\n' "$PREFIX"
)
```

### Terminal 2 · Start the Flink consumer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
python code/Pipeline/build_jvm_window.py "code/logs/$PREFIX/price-windows.jar" &&
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --rest-port 8082 \
  --parallelism 4 \
  --jvm-window-jar "code/logs/$PREFIX/price-windows.jar" \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  2>&1 | tee "code/logs/$PREFIX/flink.log"
```

### Terminal 3 · Follow the live logs

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/inspect_run.py \
  --topic-prefix "$PREFIX"
```

### Terminal 4 · Run the producer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
set -o pipefail
python code/Pipeline/producer.py \
  --files \
    data/debs2022-gc-trading-day-08-11-21.csv \
    data/debs2022-gc-trading-day-09-11-21.csv \
  --skip-rows 190000 \
  --max-rows 500000 \
  --rate 5000 \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  | tee "code/logs/$PREFIX/producer.jsonl"
```

After `DRAINED`, run this in Terminal 4.

```bash
python code/Pipeline/reference.py \
  --files \
    data/debs2022-gc-trading-day-08-11-21.csv \
    data/debs2022-gc-trading-day-09-11-21.csv \
  --skip-rows 190000 \
  --max-rows 500000 \
  --topic-prefix "$PREFIX" \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  --check-kafka
```

The checker should report `"match": true` with zero mismatches.

## JVM window operator

The JVM option keeps CSV normalization and classification in Python and
runs the partitioning by symbol, window state, timers, EMAs and crossovers
in Flink's JVM. Build its JAR with JDK 17 and the same PyFlink environment
used to launch the worker.

```bash
source .venv311/bin/activate
python code/Pipeline/build_jvm_window.py /tmp/ssdm-price-windows.jar
```

Test 2 already builds and selects the JVM operator. For the original
PyFlink baseline, use `--parallelism 3` and omit `--jvm-window-jar`.
Keep the data, subscriptions and lateness settings the same when comparing
runs.

On the WSL host where roughly 200-second wall-clock steps were observed,
the replay was tested with a 240-second Kafka broker session timeout and
worker request timeout. To reproduce those settings on that host, set
`KAFKA_BROKER_SESSION_TIMEOUT_MS=240000` on the Kafka Compose startup command
and add `--kafka-request-timeout-ms 240000` to the worker command. Longer
timeouts also delay detection of genuine connection failures. Flink REST
snapshots can still lag after a clock step.

## Checkpoint recovery test

Use a fresh prefix and run the producer to completion before starting the
worker. The following mode snapshots the current Kafka end offsets,
processes those inputs as a finite replay, and finishes after closing
the last windows. It uses automatic Flink task recovery with checkpoints
and transactional Kafka sinks.

```bash
source .venv311/bin/activate
source code/logs/current-run.env
_python_worker_execution_mode=process PYTHONPATH="$PWD/code/Pipeline" \
  python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --rest-port 8082 \
  --parallelism 4 \
  --lateness-seconds "$DELAY" \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  --jvm-window-jar /tmp/ssdm-price-windows.jar \
  --recoverable-replay "$PWD/code/logs/$PREFIX/checkpoints"
```

After the process exits successfully, run the reference checker for the
staged CSV selection. For a recovery drill, stop one Python task process
after a completed checkpoint appears in the Flink UI. Keep the Flink
job and Kafka running, let Flink restore the task, and repeat the reference
comparison. The bounded job uses its successful exit as the completion
signal. The live inspector's `DRAINED` check is for the paced streaming
benchmark. The environment variable runs Python task executors in separate
processes so the drill can stop one executor while leaving Flink running.

## Read the logs

Compare several `PROGRESS` lines with `producer_state=replaying`.
`raw_backlog` compares producer acknowledgements with Flink's decode counter.
Downstream backpressure can slow the source and decode stage. Flink REST
metrics can temporarily stay frozen after a wall-clock jump. `lag_records`
also includes event-time progress markers.
Use `window_watermark` to follow progress through the window stage.
`DRAINED` confirms that the replay and its observed outputs reconcile.

Each replay writes to `code/logs/$PREFIX/`.

| File | What it shows |
| --- | --- |
| `ema.log` | Symbol, window end, close, EMA38, EMA100, and the source file and row |
| `advisories.log` | BUY or SELL, window end, previous and current EMAs, and the crossover reason |
| `worker.log` | Readiness, Kafka lag, raw backlog, result counts, and drain |
| `flink.log` | Flink startup output and errors |
| `flink-worker-*-python-*.log` | Flink JVM task and Kafka connector errors |
| `producer.jsonl` | Acknowledgement totals and final partition offsets used by the inspector |

The diagnostics topic retains samples of housekeeping, quotes, late prices,
and rejected rows.

Paste this in a spare terminal to watch the result and worker logs.

```bash
cd /home/jerry/SSDM
source code/logs/current-run.env
tail -F "code/logs/$PREFIX/"{ema,advisories,worker}.log
```

If a run fails, check the first task exception in the Flink JVM log.
An `IllegalArgumentException` at `SubTaskInitializationMetrics` line 49
means the clock moved backward during Flink startup. If it keeps
happening under WSL, finish any work in your WSL distributions and run
`wsl --shutdown` from PowerShell. This stops all running distributions.
Reopen Ubuntu and start again at Terminal 1 with a fresh prefix.
