"""Validation and dead-letter conversion for normalized events."""

from datetime import datetime, timezone
from typing import Any

from trading_events import DeadLetter, NormalizedEvent, Rejection


def validate_event(event: NormalizedEvent) -> Rejection | None:
    if not event.event_id or not event.symbol or "." not in event.symbol:
        return Rejection("invalid_normalized_identity", "event identity is incomplete")
    if event.event_time.tzinfo is None:
        return Rejection("invalid_event_time", "event time must be timezone-aware")
    if event.event_type == "trade" and event.last_price is None:
        return Rejection("missing_last_price", "trade events require a usable Last value")
    return None


def to_dead_letter(
    envelope: dict[str, Any],
    rejection: Rejection,
    rejected_at: datetime | None = None,
) -> DeadLetter:
    payload = envelope.get("payload") if isinstance(envelope, dict) else None
    event_id = envelope.get("event_key", envelope.get("_source_file", "unknown")) if isinstance(envelope, dict) else "unknown"
    return DeadLetter(
        event_id=str(event_id),
        reason_code=rejection.reason_code,
        details=rejection.details,
        source_file=envelope.get("_source_file") if isinstance(envelope, dict) else None,
        source_row=envelope.get("_source_row") if isinstance(envelope, dict) else None,
        rejected_at=(rejected_at or datetime.now(timezone.utc)).isoformat(),
        envelope=envelope if isinstance(envelope, dict) else {"raw": envelope},
        payload=payload if isinstance(payload, dict) else None,
    )
