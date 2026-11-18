"""Per-symbol EMA calculations for completed trading windows."""

import json
import os
from datetime import datetime, timezone

from utils import safe_json_dumps
from crossover import detect_crossover


def ema_log_path() -> str:
    return os.getenv(
        "EMA_LOG_PATH",
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
            "logs",
            "EMA.log",
        ),
    )


def prepare_ema_log() -> str:
    """Create the shared EMA inspection log and return its path."""
    log_path = ema_log_path()
    os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
    with open(log_path, "a", encoding="utf-8") as log_file:
        log_file.write(
            f"{datetime.now(timezone.utc).isoformat()} "
            "event=ema_worker_started\n"
        )
    return log_path


def prepare_output_logs() -> tuple[str, str]:
    """Create the subscribed EMA and advisory logs before processing starts."""
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs",
    )
    os.makedirs(log_dir, exist_ok=True)
    subscribed_log = os.path.join(log_dir, "EMA_subscribed.log")
    advisory_log = os.path.join(log_dir, "advisories.log")
    for log_path in (subscribed_log, advisory_log):
        with open(log_path, "a", encoding="utf-8"):
            pass
    return subscribed_log, advisory_log


def prepare_lag_log() -> str:
    """Create the lag and latency inspection log before processing starts."""
    log_dir = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        "logs",
    )
    os.makedirs(log_dir, exist_ok=True)
    lag_log = os.path.join(log_dir, "lag_metrics.log")
    with open(lag_log, "a", encoding="utf-8"):
        pass
    return lag_log


def calculate_processing_latency_ms(
    produced_at: str | None, emitted_at: datetime
) -> float | None:
    if not produced_at:
        return None
    try:
        produced = datetime.fromisoformat(produced_at)
        if produced.tzinfo is None:
            produced = produced.replace(tzinfo=timezone.utc)
        return max(0.0, (emitted_at - produced.astimezone(timezone.utc)).total_seconds() * 1000)
    except (TypeError, ValueError):
        return None


def calculate_stage_latency_ms(started_at: datetime, emitted_at: datetime) -> float:
    """Calculate wall-clock latency within one pipeline stage."""
    return max(
        0.0,
        (emitted_at - started_at.astimezone(timezone.utc)).total_seconds() * 1000,
    )


def calculate_latency_percentiles(values: list[float]) -> dict[str, float | None]:
    """Calculate small rolling latency summaries without external dependencies."""
    if not values:
        return {
            "average_ms": None,
            "p50_ms": None,
            "p95_ms": None,
            "p99_ms": None,
            "max_ms": None,
        }
    ordered = sorted(values)

    def percentile(percent: float) -> float:
        index = min(len(ordered) - 1, round((percent / 100) * (len(ordered) - 1)))
        return ordered[index]

    return {
        "average_ms": sum(ordered) / len(ordered),
        "p50_ms": percentile(50),
        "p95_ms": percentile(95),
        "p99_ms": percentile(99),
        "max_ms": ordered[-1],
    }


def calculate_ema(close: float, previous: float, period: int) -> float:
    """Calculate one EMA from a window close and the previous EMA."""
    alpha = 2 / (period + 1)
    return alpha * close + (1 - alpha) * previous


def create_ema_calculation():
    """Create the keyed Flink operator that calculates and logs EMA values."""
    from pyflink.common import Types
    from pyflink.datastream.functions import KeyedProcessFunction
    from pyflink.datastream.state import ValueStateDescriptor

    class EmaCalculation(KeyedProcessFunction):
        def open(self, runtime_context):
            self.state = runtime_context.get_state(
                ValueStateDescriptor("ema-state", Types.STRING())
            )
            log_path = ema_log_path()
            os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)
            self.log_file = open(log_path, "a", encoding="utf-8")

        def close(self):
            self.log_file.close()

        def process_element(self, value, ctx):
            ema_started_at = datetime.now(timezone.utc)
            result = json.loads(value)
            close = result.get("last_trade_price")
            if close is None:
                result["ema_status"] = "no_valid_close"
                result["ema_emitted_at"] = datetime.now(timezone.utc).isoformat()
                result["ema_processing_latency_ms"] = calculate_stage_latency_ms(
                    ema_started_at, datetime.now(timezone.utc)
                )
                self.log_file.write(
                    f"{datetime.now(timezone.utc).isoformat()} "
                    f"symbol={result['symbol']} "
                    f"window_start={result['window_start']} "
                    f"window_end={result['window_end']} "
                    "event=window_processed_no_valid_close "
                    "ema_status=no_valid_close\n"
                )
                self.log_file.flush()
                yield safe_json_dumps(result)
                return

            previous = json.loads(
                self.state.value()
                or '{"ema38": null, "ema100": null}'
            )
            previous_ema38 = previous.get("ema38")
            previous_ema100 = previous.get("ema100")
            ema38 = calculate_ema(float(close), previous_ema38 or 0.0, 38)
            ema100 = calculate_ema(float(close), previous_ema100 or 0.0, 100)
            result["close"] = close
            result["ema38"] = ema38
            result["ema100"] = ema100
            result["ema_status"] = "calculated"
            ema_emitted_at = datetime.now(timezone.utc)
            result["ema_emitted_at"] = ema_emitted_at.isoformat()
            result["ema_processing_latency_ms"] = calculate_stage_latency_ms(
                ema_started_at, ema_emitted_at
            )
            advice = detect_crossover(
                previous_ema38, previous_ema100, ema38, ema100
            )
            if advice is not None:
                result.update(
                    {
                        "advice": advice,
                        "reason": (
                            "bullish_crossover"
                            if advice == "buy"
                            else "bearish_crossover"
                        ),
                        "previous_ema38": previous_ema38,
                        "previous_ema100": previous_ema100,
                    }
                )
            self.state.update(safe_json_dumps({"ema38": ema38, "ema100": ema100}))
            self.log_file.write(
                f"{datetime.now(timezone.utc).isoformat()} "
                f"symbol={result['symbol']} "
                f"window_start={result['window_start']} "
                f"window_end={result['window_end']} "
                f"close={close} ema38={ema38:.10f} ema100={ema100:.10f}\n"
            )
            self.log_file.flush()
            yield safe_json_dumps(result)

    return EmaCalculation()
