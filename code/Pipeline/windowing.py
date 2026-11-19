"""Five-minute market clock windows and deterministic close ordering."""

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
