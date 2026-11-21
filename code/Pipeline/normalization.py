"""Trading record normalization, classification, and symbol subscriptions."""

from datetime import datetime, time
from functools import lru_cache
from math import isfinite
import re
from zoneinfo import ZoneInfo

SUPPORTED_SECURITY_TYPES = {"E", "I"}
EXCHANGE_TIMEZONES = {
    "FR": "Europe/Paris",
    "NL": "Europe/Amsterdam",
    "ETR": "Europe/Berlin",
}

PRICE = "price"
QUOTE = "quote_only"
HOUSEKEEPING = "housekeeping_or_metadata"
REJECTED = "rejected"
TERMINAL_CLASSES = ("price_accepted", "price_late", QUOTE, HOUSEKEEPING, REJECTED)
CLOCK_RE = re.compile(r"[0-9]{2}:[0-9]{2}:[0-9]{2}(?:\.[0-9]{1,6})?")
MARKET_ZONES = {exchange: ZoneInfo(zone) for exchange, zone in EXCHANGE_TIMEZONES.items()}


def normalize_symbol(value: str) -> str:
    symbol = value.strip().upper()
    parts = symbol.rsplit(".", 1)
    if len(parts) != 2 or not parts[0] or parts[1] not in EXCHANGE_TIMEZONES:
        raise ValueError(
            f"invalid subscription symbol {value!r}; expected INSTRUMENT.FR, "
            "INSTRUMENT.NL, or INSTRUMENT.ETR"
        )
    return symbol


def parse_subscriptions(symbols: str | None) -> set[str] | None:
    if symbols is None:
        return None
    return {normalize_symbol(value) for value in symbols.split(",") if value.strip()}


@lru_cache(maxsize=64)
def _parse_date(value):
    return datetime.strptime(value, "%d-%m-%Y").date()


def _parse_clock(value):
    if not CLOCK_RE.fullmatch(value):
        raise ValueError("invalid clock")
    return time.fromisoformat(value)


def _clock_offset_us(clock):
    return ((clock.hour * 60 + clock.minute) * 60 + clock.second) * 1_000_000 + clock.microsecond


def _trade_clock(trading_date, trading_time, capture_date, capture_time, exchange):
    try:
        clock = _parse_clock(trading_time)
        if not trading_date:
            try:
                capture_clock = _parse_clock(capture_time)
            except ValueError:
                capture_clock = None
            # A blank date near a midnight rollover cannot establish the trade's day.
            if capture_clock and abs(_clock_offset_us(clock) - _clock_offset_us(capture_clock)) > 43_200_000_000:
                return None, "ambiguous_trading_date"
        local = datetime.combine(
            _parse_date(trading_date or capture_date), clock, tzinfo=MARKET_ZONES[exchange]
        )
        epoch_second = int(local.replace(microsecond=0).timestamp())
        timestamp_us = epoch_second * 1_000_000 + clock.microsecond
        if timestamp_us < 2000:
            return None, "invalid_trade_time"
        return timestamp_us, "explicit" if trading_date else "capture_date_fallback"
    except (ValueError, OverflowError):
        return None, "invalid_trade_time"


def _text(row, name):
    return (row.get(name) or "").strip()


def project_row(row, source_file, source_index, source_row):
    """Represent one raw row with its query fields, source identity and capture clock."""
    raw_id = _text(row, "ID.[Exchange]") or _text(row, "ID")
    symbol = raw_id.upper()
    exchange = symbol.rsplit(".", 1)[-1] if "." in symbol else ""
    last = _text(row, "Last")
    capture_date, capture_time = _text(row, "Date"), _text(row, "Time")
    trading_date, trading_time = _text(row, "Trading date"), _text(row, "Trading time")
    trade_us, trade_clock = None, "unused"
    if last and exchange in MARKET_ZONES:
        try:
            positive = 0 < float(last) < float("inf")
        except ValueError:
            positive = False
        if positive:
            trade_us, trade_clock = _trade_clock(
                trading_date, trading_time, capture_date, capture_time, exchange
            )
    record = {
        "kind": "row",
        "symbol": symbol,
        "exchange": exchange if exchange in MARKET_ZONES else "",
        "sec_type": _text(row, "SecType").upper(),
        "last": last,
        "ask": _text(row, "Ask"),
        "bid": _text(row, "Bid"),
        "trade_us": trade_us,
        "trade_clock": trade_clock,
        "capture_date": capture_date,
        "capture_time": capture_time,
        "trading_date": trading_date,
        "trading_time": trading_time,
        "source_file": source_file,
        "source_index": source_index,
        "source_row": source_row,
    }
    if any(row.get(None, ())) or None in row.values():
        record["csv_error"] = True
    return record


def _parse_price(value):
    if not isinstance(value, str):
        raise ValueError("invalid_envelope")
    if not value:
        return 0.0
    try:
        price = float(value)
    except ValueError:
        raise ValueError("invalid_numeric_value") from None
    if not isfinite(price) or price < 0:
        raise ValueError("invalid_price_value")
    return price


def classify_row(record):
    """Return (class, price, reason); the keyed operator distinguishes accepted from late."""
    if not isinstance(record, dict) or record.get("kind") != "row":
        return REJECTED, 0.0, "invalid_envelope"
    if (
        type(record.get("source_index")) is not int or record["source_index"] < 0
        or type(record.get("source_row")) is not int or record["source_row"] < 1
        or not isinstance(record.get("source_file"), str) or not record["source_file"]
    ):
        return REJECTED, 0.0, "invalid_envelope"
    if record.get("csv_error"):
        return REJECTED, 0.0, "invalid_csv_row"
    symbol = record.get("symbol")
    if not isinstance(symbol, str) or not symbol:
        return REJECTED, 0.0, "missing_symbol"
    try:
        normalized = normalize_symbol(symbol)
    except ValueError:
        return REJECTED, 0.0, "invalid_symbol"
    if normalized != symbol or record.get("exchange") != symbol.rsplit(".", 1)[1]:
        return REJECTED, 0.0, "invalid_symbol"
    sec_type = record.get("sec_type")
    if not isinstance(sec_type, str) or sec_type not in SUPPORTED_SECURITY_TYPES:
        return REJECTED, 0.0, "invalid_security_type"
    try:
        last = _parse_price(record.get("last"))
        ask = _parse_price(record.get("ask"))
        bid = _parse_price(record.get("bid"))
    except ValueError as exc:
        return REJECTED, 0.0, str(exc)
    if last:
        clock = record.get("trade_clock")
        if clock == "ambiguous_trading_date":
            return REJECTED, 0.0, clock
        trade_us = record.get("trade_us")
        if clock not in ("explicit", "capture_date_fallback") or type(trade_us) is not int:
            return REJECTED, 0.0, "invalid_trade_time"
        if not 2000 <= trade_us <= 253_402_300_799_999_999:
            return REJECTED, 0.0, "invalid_trade_time"
        return PRICE, last, None
    if ask or bid:
        return QUOTE, 0.0, "quote_without_trade"
    return HOUSEKEEPING, 0.0, "no_positive_price"
