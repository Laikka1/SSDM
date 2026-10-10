# Stream Analytics Run Instructions

## Prerequisites

- Python 3.11 virtual environment
- Docker with Compose
- Trading Data files in `data/`
- The Flink Kafka connector jar is downloaded automatically when the worker starts if it is missing.
- If you run without internet access, set FLINK_KAFKA_CONNECTOR_JAR to a local jar path.


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

## Terminal 1 - Start Local Infrastructure

```bash
cd code
docker compose up -d kafka flink-jobmanager flink-taskmanager
docker compose ps
```

## Terminal 2 - Start Stream Worker

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/streamworker.py \
  --engine flink \
  --consumer-group trading-flink-transport \
  --input-topics trading-events \
  --starting-offsets earliest \
  --parallelism 1
```

## Terminal 3 - Produce Events

```bash
source .venv311/bin/activate

PYTHONPATH=code/Pipeline python code/Pipeline/producer.py \
  --topic trading-events \
  --files data/debs2022-gc-trading-day-08-11-21.csv \
  --max-rows 1000 \
  --rate 0

# For a larger replay, use --subset 1. It scans the full file first and is
# intentionally slow at a positive --rate.
```

## Terminal 4 - Verify Kafka and Flink Logs

Kafka keeps records from previous runs. For a clean smoke test, use a new
topic name consistently in the producer and stream worker commands, or delete
the old topic before restarting the test. The existing `trading-events` topic
may contain records produced before the CSV parser was corrected.

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic trading-events --from-beginning --max-messages 5
tail -n 30 code/logs/streamworker_*.log
```

## Optional - Check Output Logs

```bash
ls -lah code/logs
```
