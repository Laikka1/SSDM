# Deferred adapters

These files are preserved for later milestones and are not imported by the current CSV-to-Kafka-to-Flink transport pipeline.

- `cassandra_insert.py`: durable storage after window/query results exist.
- `alerting.py`: legacy advisory and monitoring helpers; the active Flink job
  now publishes crossover advisories directly.

Cassandra persistence remains deferred. Advisory outputs are implemented in the
active Flink pipeline and are published to the configured advisory topic.
