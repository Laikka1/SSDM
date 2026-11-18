"""Validation and loading of optional user symbol subscriptions."""

from pathlib import Path

from trading_events import EXCHANGE_TIMEZONES


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    parts = symbol.rsplit(".", 1)
    if len(parts) != 2 or not parts[0] or parts[1] not in EXCHANGE_TIMEZONES:
        raise ValueError(
            f"invalid subscription symbol {value!r}; expected INSTRUMENT.FR, "
            "INSTRUMENT.NL, or INSTRUMENT.ETR"
        )
    return symbol


def load_subscriptions(
    symbols: str | None = None, file_path: str | None = None
) -> set[str] | None:
    """Return configured symbols, or None when output should remain unfiltered."""
    if symbols is None and file_path is None:
        return None

    values: list[str] = []
    if symbols is not None:
        values.extend(symbols.split(","))
    if file_path is not None:
        try:
            values.extend(
                line.split("#", 1)[0].strip()
                for line in Path(file_path).read_text(encoding="utf-8").splitlines()
            )
        except OSError as exc:
            raise ValueError(f"could not read subscriptions file {file_path!r}: {exc}") from exc

    return {normalize_symbol(value) for value in values if value.strip()}
