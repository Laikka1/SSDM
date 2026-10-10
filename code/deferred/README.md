# Deferred adapters

These files are preserved for later milestones and are not imported by the current CSV-to-Kafka-to-Flink transport pipeline.

- `cassandra_insert.py`: durable storage after window/query results exist.
- `alerting.py`: advisory and monitoring publication after crossover logic exists.

The current milestone intentionally runs without Cassandra and without advisory outputs.
