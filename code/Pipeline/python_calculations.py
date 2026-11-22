"""Window formatting, EMAs, and crossovers used only when --jvm-windows is omitted."""

from datetime import datetime


def window_iso(timestamp_ms, zone):
    return datetime.fromtimestamp(timestamp_ms // 1000, zone).isoformat()


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
