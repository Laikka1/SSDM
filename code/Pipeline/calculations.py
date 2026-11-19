"""Trading window boundaries, EMA calculations, and crossover rules."""

from datetime import datetime
import math

WINDOW_MS = 5 * 60 * 1000
DEFAULT_LATENESS_SECONDS = 1.0


def window_end_ms(trade_us: int) -> int:
    # FR, NL and ETR market offsets are whole multiples of five minutes for
    # the dataset. Their local midnight is on this UTC epoch grid in CET/CEST.
    return (trade_us // 1000 // WINDOW_MS + 1) * WINDOW_MS


def window_iso(timestamp_ms, zone):
    return datetime.fromtimestamp(timestamp_ms // 1000, zone).isoformat()


def lateness_ms(seconds):
    if not math.isfinite(seconds) or not 0 <= seconds <= 86_400:
        raise ValueError("--lateness-seconds must be between 0 and 86400")
    return math.ceil(seconds * 1000)


def calculate_ema(close: float, previous: float, period: int) -> float:
    alpha = 2 / (period + 1)
    return alpha * close + (1 - alpha) * previous


def detect_crossover(
    previous_ema38: float,
    previous_ema100: float,
    current_ema38: float,
    current_ema100: float,
) -> str | None:
    if previous_ema38 <= previous_ema100 and current_ema38 > current_ema100:
        return "buy"
    if previous_ema38 >= previous_ema100 and current_ema38 < current_ema100:
        return "sell"
    return None
