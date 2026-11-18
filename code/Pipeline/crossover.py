"""Pure crossover detection for the two EMA query."""


def detect_crossover(
    previous_ema38: float | None,
    previous_ema100: float | None,
    current_ema38: float,
    current_ema100: float,
) -> str | None:
    """Return buy or sell when the EMA ordering changes."""
    if previous_ema38 is None or previous_ema100 is None:
        return None
    if previous_ema38 <= previous_ema100 and current_ema38 > current_ema100:
        return "buy"
    if previous_ema38 >= previous_ema100 and current_ema38 < current_ema100:
        return "sell"
    return None
