"""Event-time five-minute window calculations."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from trading_events import NormalizedEvent


WINDOW_SECONDS = 300


def window_bounds(event_time: datetime) -> tuple[datetime, datetime]:
    """Return midnight-aligned five-minute bounds in the event's local zone."""
    if event_time.tzinfo is None:
        raise ValueError("event_time must be timezone-aware")
    local = event_time.astimezone(event_time.tzinfo)
    midnight = local.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed = int((local - midnight).total_seconds())
    start = midnight + timedelta(seconds=(elapsed // WINDOW_SECONDS) * WINDOW_SECONDS)
    return start, start + timedelta(seconds=WINDOW_SECONDS)


def build_window_result(events: list[NormalizedEvent]) -> dict:
    if not events:
        raise ValueError("cannot build a window result without events")
    start, end = window_bounds(events[0].event_time)
    event_type_counts: dict[str, int] = {}
    exchanges = {event.exchange for event in events}
    symbols = {event.symbol for event in events}
    for event in events:
        event_type_counts[event.event_type] = event_type_counts.get(event.event_type, 0) + 1
    trade_events = [
        event for event in events
        if event.event_type == "trade" and event.last_price is not None
    ]
    latest_trade = max(trade_events, key=lambda event: event.event_time, default=None)
    return {
        "symbol": events[0].symbol,
        "exchange": events[0].exchange,
        "security_type": events[0].security_type,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "event_count": len(events),
        "event_type_counts": event_type_counts,
        "valid_price_count": sum(event.last_price is not None for event in events),
        "last_trade_price": latest_trade.last_price if latest_trade else None,
        "last_trade_event_time": (
            latest_trade.event_time.isoformat() if latest_trade else None
        ),
        "source_files": sorted({event.source_file for event in events if event.source_file}),
        "first_event_time": min(event.event_time for event in events).isoformat(),
        "last_event_time": max(event.event_time for event in events).isoformat(),
        "symbols": sorted(symbols),
        "exchanges": sorted(exchanges),
    }
