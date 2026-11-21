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
  mkdir -p code/logs
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

### Optional · Open the live visualization

After Kafka is ready and the replay prefix is saved, run the viewer in a spare
terminal. Use this same step for any replay below.

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/visualization/viewer_server.py --run-dir "code/logs/$PREFIX"
```

Open [the local viewer](http://127.0.0.1:8765/) in your browser. Start the worker
in Terminal 2, wait for `READY`, then start the producer in Terminal 3. The
viewer shows **Waiting for results** until the first five-minute window is
finalized. Use the **Symbol** menu to switch between published symbols. Click
an EMA point to see both EMA values and the close for that window. Click a BUY
or SELL marker to see the EMA values before and after the crossover.
The x-axis shows finalized windows at equal spacing, skipping empty intervals.
A dashed vertical line marks each market date change.

The viewer checks for updates once a second. For each new replay, stop the
previous viewer with Ctrl+C and rerun the command above so it picks up the new
prefix. For a saved run, pass its directory to `--run-dir`.
If WSL reports `Address already in use`, stop the previous viewer or use
`--port 8766` to keep both viewers open.
If the browser shows a 404 while the viewer is running in WSL, a Windows
service may be listening on the same port. Restart the viewer with
`--port 8766` and open the URL it prints.

### Terminal 2 · Start the Flink consumer

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --lateness-seconds "$DELAY" \
  --subscribed-symbols INCAC.FR,INEUN.NL
```

### Terminal 3 · Run the producer after the worker prints `READY`

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 100000 \
  --rate 1000 \
  --topic-prefix "$PREFIX"
```

## High-rate replay · 20,000 rows/s with JVM windows

Skip 150,000 rows and replay the next million rows from each November 8–10
file (three million rows total). The subscriptions are `INCAC.FR`, `INEUN.NL`,
`METEX.FR`, and `ALINN.FR`.
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
  mkdir -p code/logs
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
  --parallelism 4 \
  --jvm-windows \
  --kafka-request-timeout-ms 240000 \
  --subscribed-symbols INCAC.FR,INEUN.NL,METEX.FR,ALINN.FR
```

### Terminal 3 · Run the producer after the worker prints `READY`

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/producer.py \
  --files \
    data/debs2022-gc-trading-day-08-11-21.csv \
    data/debs2022-gc-trading-day-09-11-21.csv \
    data/debs2022-gc-trading-day-10-11-21.csv \
  --skip-rows 150000 \
  --max-rows 1000000 \
  --rate 20000 \
  --topic-prefix "$PREFIX"
```

## More replay test cases

For each case, repeat Terminal 1 from the high-rate replay with the listed
`DELAY`. In Terminal 2, start the JVM worker with the listed subscriptions.
When the worker prints `READY`, run the corresponding producer command in Terminal 3.

| Case | `DELAY` | `--subscribed-symbols` | Rows published |
| --- | ---: | --- | --- |
| BUY and SELL fixture | 1 | `DEMO.FR` | 50 |
| Three exchanges | 1.2 | `IEBBB.FR,INTDT.NL,A0C4CA.ETR` | 590,000 |
| All seven files | 1.2 | `IEBBB.FR,INTDT.NL,A0C4CA.ETR` | 6,809,511 |

### BUY and SELL fixture

This creates a temporary CSV with one trade per five-minute window. A
positive first close triggers a BUY at 08:05 CET. The price then drops
and stays low until the EMAs cross, triggering a SELL at 10:35 CET.

```bash
(
  set -e
  cd /home/jerry/SSDM
  source .venv311/bin/activate
  source code/logs/current-run.env
  crossover_csv=$(mktemp --suffix=.csv)
  trap 'rm -f -- "$crossover_csv"' EXIT
  python - "$crossover_csv" <<'PY'
import csv
from datetime import datetime, timedelta
import sys

start = datetime(2021, 11, 8, 8)
with open(sys.argv[1], "w", newline="", encoding="utf-8") as fixture:
    writer = csv.writer(fixture)
    writer.writerow(["ID.[Exchange]", "SecType", "Date", "Time", "Last",
                     "Ask", "Bid", "Trading date", "Trading time"])
    for index in range(50):
        tick = start + timedelta(minutes=5 * index)
        day = tick.strftime("%d-%m-%Y")
        clock = tick.strftime("%H:%M:%S")
        close = "100" if index == 0 else "0.01"
        writer.writerow(["DEMO.FR", "E", day, clock, close, "", "", day, clock])
PY
  python code/Pipeline/producer.py \
    --files "$crossover_csv" \
    --rate 40 \
    --chunksize 2 \
    --topic-prefix "$PREFIX"
)
```

### Three exchanges in about two minutes

The first 590,000 rows of November 10 include trades for the selected
French, Dutch, and German symbols. Expect one BUY per symbol.

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-10-11-21.csv \
  --max-rows 590000 \
  --rate 5000 \
  --topic-prefix "$PREFIX"
```

### All seven files at 5,000 rows/s

`--max-rows 1350000` caps each file individually. The five weekdays reach
that cap, and the two weekend files contribute all 59,511 of their rows.
At 5,000 rows/s, publishing takes about 22 minutes 42 seconds.
The weekend rows are housekeeping or metadata. The three
subscribed symbols each produce one BUY from the weekday data.

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-{08..14}-11-21.csv \
  --max-rows 1350000 \
  --rate 5000 \
  --topic-prefix "$PREFIX"
```

## Weekday slices at 20,000 rows/s with 14 crossover symbols

This stress run skips the first two million rows in each file and caps each
file at five million more. Both weekend files fall entirely within the skip.

### Terminal 1 · Start Kafka and create the replay topics

Topic setup retries temporary Kafka errors. If it still times out, run the
topic-creation and `printf` lines again with the same `PREFIX` and `DELAY`.
Continue when the command prints `Kafka topics ready`.

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
PREFIX="trading-events-$(date +%Y%m%d-%H%M%S-%N)"
DELAY=10
mkdir -p code/logs &&
  KAFKA_BROKER_SESSION_TIMEOUT_MS=240000 docker compose \
    --file code/docker-compose.yml up --detach --wait kafka &&
  python code/Pipeline/kafka_io.py --topic-prefix "$PREFIX" &&
  printf 'export PREFIX=%q\nexport DELAY=%q\n' "$PREFIX" "$DELAY" > code/logs/current-run.env
```

### Terminal 2 · Start the Flink worker

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
PYTHONPATH="$PWD/code/Pipeline" python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" --lateness-seconds "$DELAY" \
  --parallelism 4 --jvm-windows --interval 5 --timeout 600 \
  --kafka-request-timeout-ms 240000 \
  --subscribed-symbols I3PLU.FR,IAMDS.FR,ITSL3.NL,ALE15.FR,A2QP7J.ETR,I3SHP.NL,ALCYB.FR,IAMZS.FR,I2STS.NL,A2PLUG.ETR,GV.FR,ALGAU.FR,A14U78.ETR,620200.ETR
```

### Terminal 3 · Run the producer after the worker prints `READY`

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-{08..14}-11-21.csv \
  --rate 20000 \
  --topic-prefix "$PREFIX" \
  --skip-rows 2000000 \
  --max-rows 5000000
```

### Full-week endurance (evaluation Test 7)

Repeat Terminals 1 and 2 with a fresh prefix. After `READY`, replay every row of
all seven files. Set `RATE` to the sustained rate measured in Test 5.

```bash
cd /home/jerry/SSDM
source .venv311/bin/activate
source code/logs/current-run.env
RATE=5000  # Replace with the measured sustainable rate.
python code/Pipeline/producer.py \
  --files data/debs2022-gc-trading-day-{08..14}-11-21.csv \
  --rate "$RATE" --topic-prefix "$PREFIX"
```

## Publish additional batches

Leave the worker running after the producer exits so it can log through drain.
Run the producer again with the next tick file or row slice and the same
`PREFIX`. The worker retains its holdback setting. Open windows wait for later ticks to advance event
time. Use a fresh prefix to replay older data with fresh EMA state and a
fresh watermark. Stop the worker with Ctrl+C when you are done.

## JVM window operator

The JVM option keeps CSV normalization and classification in Python and
runs the partitioning by symbol, window state, timers, EMAs and crossovers
in Flink's JVM. `--jvm-windows` builds the operator with JDK 17 into a temporary
JAR and cleans it up on normal exit or Ctrl+C. For the PyFlink comparison in Test 8, omit
`--jvm-windows` and use the same parallelism and CPU allocation in both runs.

The 240-second Kafka timeouts in the high-rate replay tolerate the roughly 200-second
wall-clock jumps observed on the WSL host. Longer timeouts also delay
detection of genuine connection failures. Flink REST snapshots can still
lag after a clock step.

## Checkpoint recovery test

Prepare a fresh prefix and start the worker below. Run the producer after `READY`. This mode
uses automatic Flink task recovery with checkpoints and transactional Kafka
sinks and keeps consuming as more batches arrive.

```bash
source .venv311/bin/activate
source code/logs/current-run.env
_python_worker_execution_mode=process PYTHONPATH="$PWD/code/Pipeline" \
  python code/Pipeline/streamworker.py \
  --topic-prefix "$PREFIX" \
  --parallelism 4 \
  --lateness-seconds "$DELAY" \
  --subscribed-symbols INCAC.FR,INEUN.NL \
  --jvm-windows \
  --checkpoint-dir "$PWD/code/logs/$PREFIX/checkpoints"
```

For a recovery drill, stop one Python task process after a completed
checkpoint appears in the Flink UI. Keep the Flink job and Kafka running
and let Flink restore the task. The environment variable runs Python task
executors in separate processes so the drill can stop one executor without
stopping the job.

## Read the logs

cd /home/jerry/SSDM
source code/logs/current-run.env
tail -F "code/logs/$PREFIX/"{ema,advisories,worker}.log
```

For Test 6, subtract successive `partition_offsets` to measure records read on
each partition, including progress markers. `busiest_partition` identifies the
largest backlog at that instant. For CPU utilization, divide the change in CPU
seconds by the elapsed monotonic seconds and multiply by 100 (one core is 100%).
Match process samples by PID and creation time. RSS includes shared pages in
each process. For Test 4, prefix the worker command with e.g. `taskset -c 0,1`
and set `--parallelism 2` for a two-core run. Choose CPU IDs available to WSL
and save the allocation for each run.

For evaluation runs, sample Kafka alongside the worker and producer. Start this
in Terminal 1 after Kafka is up and the worker has created the run directory.
Stop with Ctrl+C after drain.

```bash
cd /home/jerry/SSDM
source code/logs/current-run.env
KAFKA_ID=$(docker compose --file code/docker-compose.yml ps -q kafka)
while sample=$(docker stats --no-stream --format '{{.CPUPerc}} {{.MemUsage}}' "$KAFKA_ID"); do
  printf '%s %s\n' "$(date --iso-8601=seconds)" "$sample"
  sleep 2
done >> "code/logs/$PREFIX/kafka-resources.log"
```

If a run fails, check `worker.log`, the JVM logs, and the Flink UI.
An `IllegalArgumentException` at `SubTaskInitializationMetrics` line 49
means the clock moved backward during Flink startup. If it keeps
happening under WSL, finish any work in your WSL distributions and run
`wsl --shutdown` from PowerShell. This stops all running distributions.
Reopen Ubuntu and start again at Terminal 1 with a fresh prefix.
