"""Normalization of producer envelopes into stable trading events."""

from datetime import datetime
from math import isfinite
from typing import Any
from zoneinfo import ZoneInfo

from trading_events import EXCHANGE_TIMEZONES, NormalizedEvent, Rejection, SUPPORTED_SECURITY_TYPES


_EVENT_FIELDS = (("ask", "Ask"), ("bid", "Bid"), ("trade", "Last"))


def _value(payload: dict[str, Any], name: str) -> Any:
    value = payload.get(name)
    if value is None:
        value = payload.get(name.strip())
    return value


def _parse_symbol(raw_symbol: Any) -> tuple[str, str] | Rejection:
    if not isinstance(raw_symbol, str) or not raw_symbol.strip():
        return Rejection("missing_symbol", "ID.[Exchange] is missing")
    symbol = raw_symbol.strip().upper()
    parts = symbol.rsplit(".", 1)
    if len(parts) != 2 or not parts[0] or parts[1] not in EXCHANGE_TIMEZONES:
        return Rejection("invalid_symbol", "symbol must be INSTRUMENT.FR, INSTRUMENT.NL, or INSTRUMENT.ETR")
    return symbol, parts[1]


def _parse_price(value: Any, field: str) -> float | None | Rejection:
    try:
        price = float(str(value).strip())
    except (TypeError, ValueError):
        return Rejection("invalid_numeric_value", f"{field} is not numeric")
    if not isfinite(price) or price < 0:
        return Rejection("invalid_price_value", f"{field} must be finite and non-negative")
    if price == 0:
        return None
    return price


def _parse_event_time(payload: dict[str, Any], exchange: str) -> tuple[str, datetime] | Rejection:
    date_value = _value(payload, "Date")
    time_value = _value(payload, "Time")
    if not isinstance(date_value, str) or not isinstance(time_value, str):
        return Rejection("invalid_event_time", "Date and Time are required")
    try:
        local = datetime.strptime(
            f"{date_value.strip()} {time_value.strip()}",
            "%d-%m-%Y %H:%M:%S.%f",
        ).replace(tzinfo=ZoneInfo(EXCHANGE_TIMEZONES[exchange]))
    except ValueError as exc:
        return Rejection("invalid_event_time", str(exc))
    return date_value.strip(), local


def normalize_envelope(envelope: dict[str, Any]) -> NormalizedEvent | Rejection:
    """Normalize one producer envelope, returning a typed rejection on failure."""
    if not isinstance(envelope, dict):
        return Rejection("invalid_envelope", "record must be an object")
    payload = envelope.get("payload")
    if not isinstance(payload, dict):
        return Rejection("missing_payload", "record payload must be an object")

    event_id = envelope.get("event_key")
    if not isinstance(event_id, str) or not event_id.strip():
        return Rejection("missing_event_id", "event_key is required")
        
    symbol_result = _parse_symbol(payload.get("ID.[Exchange]", envelope.get("symbol")))
    if isinstance(symbol_result, Rejection):
        return symbol_result
    symbol, exchange = symbol_result

    security_type = _value(payload, "SecType")
    if security_type not in SUPPORTED_SECURITY_TYPES:
        return Rejection("invalid_security_type", "SecType must be E or I")
        
    time_result = _parse_event_time(payload, exchange)
    if isinstance(time_result, Rejection):
        return time_result
    trading_date, event_time = time_result

    present = []
    for event_type, field in _EVENT_FIELDS:
        value = _value(payload, field)
        if value not in (None, ""):
            present.append((event_type, field, value))
    parsed_prices = {}
    available_attributes = tuple(field.lower() for _, field, _ in present)
    for event_type, field, raw_price in present:
        price_result = _parse_price(raw_price, field)
        if isinstance(price_result, Rejection):
            return price_result
        if price_result is not None:
            parsed_prices[event_type] = price_result
    if parsed_prices:
        event_type = "trade" if "trade" in parsed_prices else "quote"
        last_price = parsed_prices.get("trade")
    else:
        event_type, last_price = "housekeeping", None

    source_row = envelope.get("_source_row")
    if source_row is not None and not isinstance(source_row, int):
        return Rejection("invalid_source_metadata", "_source_row must be an integer")

    return NormalizedEvent(
        event_id=event_id.strip(),
        symbol=symbol,
        exchange=exchange,
        security_type=SUPPORTED_SECURITY_TYPES[security_type],
        trading_date=trading_date,
        event_time=event_time,
        last_price=last_price,
        event_type=event_type,
        available_attributes=available_attributes,
        source_file=envelope.get("_source_file"),
        source_row=source_row,
        produced_at=envelope.get("_produced_at"),
        original_payload=payload,
        original_envelope=envelope,
    )
