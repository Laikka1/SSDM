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
for `READY` in Terminal 3 before starting the producer. After `DRAINED`,
stop the Flink worker in Terminal 2 with Ctrl+C. Kafka can stay running
between tests.

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
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --rest-port 8082 \
  --subscribed-symbols INCAC.FR,INEUN.NL
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

The final `DRAINED` line should report two EMA results and two BUYs.

## High-rate replay · 5,000 rows/s with JVM windows

Replay 500,000 rows from each of the November 8 and November 9 tick
files. The skip and row limit apply per file, giving one million rows
in total. The worker publishes results for `INCAC.FR` and `INEUN.NL`.
This configuration uses four Flink subtasks and two input partitions per
reader. It includes the Kafka timeout settings tested with WSL clock jumps.

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
  KAFKA_BROKER_SESSION_TIMEOUT_MS=240000 docker compose \
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
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --rest-port 8082 \
  --parallelism 4 \
  --jvm-windows \
  --kafka-request-timeout-ms 240000 \
  --subscribed-symbols INCAC.FR,INEUN.NL
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

The final `DRAINED` line should report one million raw rows read, 72 EMA
results, and two BUYs.

## JVM window operator

The JVM option keeps CSV normalization and classification in Python and
runs the partitioning by symbol, window state, timers, EMAs and crossovers
in Flink's JVM. `--jvm-windows` builds the operator with JDK 17 into a temporary
JAR and cleans it up on normal exit or Ctrl+C. For the PyFlink baseline, use
`--parallelism 3` and omit `--jvm-windows`.

The 240-second Kafka timeouts in the high-rate replay tolerate the roughly 200-second
wall-clock jumps observed on the WSL host. Longer timeouts also delay
detection of genuine connection failures. Flink REST snapshots can still
lag after a clock step.

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
  --jvm-windows \
  --recoverable-replay "$PWD/code/logs/$PREFIX/checkpoints"
```

For a recovery drill, stop one Python task process after a completed
checkpoint appears in the Flink UI. Keep the Flink job and Kafka running
and let Flink restore the task. The bounded job uses its successful exit
as the completion signal. The live inspector's `DRAINED` check is for the
paced replay. The environment variable runs Python task executors in
separate processes so the drill can stop one executor while leaving Flink running.

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
| `producer.jsonl` | Acknowledgement totals and final partition offsets used by the inspector |

The diagnostics topic retains samples of housekeeping, quotes, late prices,
and rejected rows.

Paste this in a spare terminal to watch the result and worker logs.

```bash
cd /home/jerry/SSDM
source code/logs/current-run.env
tail -F "code/logs/$PREFIX/"{ema,advisories,worker}.log
```

If a run fails, check the worker terminal or the Flink UI. The worker prints
the location of its temporary JVM logs. Set `FLINK_LOG_DIR` explicitly if you
need to keep those logs after the worker exits.
An `IllegalArgumentException` at `SubTaskInitializationMetrics` line 49
means the clock moved backward during Flink startup. If it keeps
happening under WSL, finish any work in your WSL distributions and run
`wsl --shutdown` from PowerShell. This stops all running distributions.
Reopen Ubuntu and start again at Terminal 1 with a fresh prefix.
