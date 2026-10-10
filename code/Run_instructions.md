# Stream Analytics Run Instructions

## Prerequisites

- Python 3.11 virtual environment
- Docker with Compose
- Data files in data/airbnb_milan
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

## Terminal 1 - Start Infrastructure

```bash
cd code
docker compose up -d
docker compose ps
```

## Terminal 2 - Start Stream Worker

```bash
source .venv311/bin/activate

PYTHONPATH=code python code/mysimbdp/streamworker.py \
  --engine flink \
  --cassandra-hosts localhost \
  --consumer-group streamworker_group \
  --input-topics tenant_a_events \
  --starting-offsets latest
```

## Terminal 3 - Produce Events

```bash
source .venv311/bin/activate

PYTHONPATH=code python code/mysimbdp/producer.py \
  --tenant tenant_a \
  --topic tenant_a_events \
  --files data/airbnb_milan/listings.csv \
  --subset 100 \
  --rate 200 \
  --event-time-source produced_at
```

## Terminal 4 - Verify Cassandra Outputs

```bash
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM stream_analytics.raw_events;"
docker exec -it cassandra cqlsh -e "SELECT COUNT(*) FROM stream_analytics.window_results;"
docker exec -it cassandra cqlsh -e "SELECT tenant_id, window_start, window_end, group_key, group_value, event_count, avg_price FROM stream_analytics.window_results LIMIT 20;"
docker exec -it cassandra cqlsh -e "SELECT tenant_id, event_time, event_key, group_key, group_value, price FROM stream_analytics.raw_events LIMIT 20;"
docker exec -it cassandra cqlsh -e "SELECT SUM(event_count) FROM stream_analytics.window_results;"
```

## Optional - Check Output Logs

```bash
ls -lah logs
tail -n 30 logs/tenant_a_results.log
tail -n 30 logs/tenant_a_monitoring.log
```
