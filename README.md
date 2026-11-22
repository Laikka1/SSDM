# Overview of the components

**producer**

- Replays market-data CSV rows into Kafka at a configured rate.
- [Producer](code/Pipeline/producer.py)

**Kafka**

- Carries replay events into Flink and delivers EMA results and trading advisories. The Kafka I/O module manages topics and connections.
- [Broker configuration](code/docker-compose.yml)
- [Kafka I/O](code/Pipeline/kafka_io.py)
- [Flink connector](code/Pipeline/jars/)

**Flink worker**

- Classifies incoming events, computes five minute closing prices, EMA38, EMA100, and crossovers and publishes results for subscribed symbols.
- [Flink worker](code/Pipeline/streamworker.py)
- [JVM window operator](code/Pipeline/jvm/PriceWindows.java)

**Pipeline helpers**

- [Normalization](code/Pipeline/normalization.py) handles symbols, timestamps, event classification, and shared window boundaries.
- [Python calculations](code/Pipeline/python_calculations.py) formats window timestamps and computes EMAs and crossovers only when `--jvm-windows` is omitted.

**Run monitoring**

- Reads Kafka results and records worker progress, timing, and resource usage.
- [Run inspector](code/Pipeline/inspect_run.py)

**Result viewer**

- Displays closing prices, EMAs, and BUY/SELL advisories from live or saved run logs.
- [Viewer server and browser files](code/visualization/)

## Running and logs

- [Run instructions](code/Run_instructions.md)
- [Python dependencies](code/requirements.txt)
- Each run writes to `code/logs/<prefix>/`. Look in `producer.jsonl` for replay progress, `worker.log` for worker status, `worker_resources.jsonl` for resource samples, and `ema.log` and `advisories.log` for results. Flink JVM logs go in `flink/` by default.
