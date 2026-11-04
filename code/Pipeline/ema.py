"""Per-symbol EMA calculations for completed trading windows."""

import json
import os
from datetime import datetime, timezone

from utils import safe_json_dumps


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
            result = json.loads(value)
            close = result.get("last_trade_price")
            if close is None:
                result["ema_status"] = "no_valid_close"
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
                self.state.value() or '{"ema38": 0.0, "ema100": 0.0}'
            )
            ema38 = calculate_ema(float(close), previous["ema38"], 38)
            ema100 = calculate_ema(float(close), previous["ema100"], 100)
            result["close"] = close
            result["ema38"] = ema38
            result["ema100"] = ema100
            result["ema_status"] = "calculated"
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
