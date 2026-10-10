from datetime import datetime, timezone

try:
    from cassandra.cluster import Cluster
except ImportError:  # pragma: no cover
    Cluster = None


KEYSPACE = "stream_analytics"
WINDOW_RESULTS_TABLE = "window_results"
RAW_EVENTS_TABLE = "raw_events"


def _to_datetime(value):
    if isinstance(value, datetime):
        return value
    if value is None:
        return datetime.now(timezone.utc)
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))


def _to_optional_float(value):
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


class CassandraInsert:
    def __init__(self, hosts: str = "localhost", logger=None):
        self.logger = logger
        if Cluster is None:
            raise RuntimeError("cassandra-driver is not installed")
        host_list = [h.strip() for h in hosts.split(",") if h.strip()]
        self.cluster = Cluster(host_list)
        self.session = self.cluster.connect()
        self._bootstrap_schema()

    def _bootstrap_schema(self) -> None:
        self.session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )
        self.session.set_keyspace(KEYSPACE)

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {WINDOW_RESULTS_TABLE} (
                tenant_id text,
                window_start timestamp,
                window_end timestamp,
                group_key text,
                group_value text,
                event_count int,
                avg_price double,
                result_type text,
                persisted_at timestamp,
                PRIMARY KEY ((tenant_id), window_start, window_end, group_key, group_value)
            )
            """
        )

        self.session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {RAW_EVENTS_TABLE} (
                tenant_id text,
                event_time timestamp,
                event_key text,
                group_key text,
                group_value text,
                price double,
                processing_latency_ms double,
                consumed_at timestamp,
                PRIMARY KEY ((tenant_id), event_time, event_key)
            )
            """
        )

    def insert_window_result(self, result: dict) -> datetime:
        persisted_at = datetime.now(timezone.utc)
        self.session.execute(
            f"""
            INSERT INTO {WINDOW_RESULTS_TABLE}
            (tenant_id, window_start, window_end, group_key, group_value, event_count, avg_price, result_type, persisted_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                result.get("tenant_id"),
                _to_datetime(result.get("window_start")),
                _to_datetime(result.get("window_end")),
                result.get("group_key"),
                result.get("group_value"),
                int(result.get("event_count", 0)),
                float(result.get("avg_price", 0.0)),
                result.get("result_type", "window_summary"),
                persisted_at,
            ),
        )
        return persisted_at

    def insert_raw_event(self, event: dict) -> datetime:
        consumed_at = datetime.now(timezone.utc)
        self.session.execute(
            f"""
            INSERT INTO {RAW_EVENTS_TABLE}
            (tenant_id, event_time, event_key, group_key, group_value, price, processing_latency_ms, consumed_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                event.get("tenant_id"),
                _to_datetime(event.get("event_time")),
                str(event.get("event_key") or ""),
                event.get("group_key"),
                event.get("group_value"),
                _to_optional_float(event.get("price")),
                _to_optional_float(event.get("processing_latency_ms")),
                consumed_at,
            ),
        )
        return consumed_at

    def close(self) -> None:
        self.cluster.shutdown()
