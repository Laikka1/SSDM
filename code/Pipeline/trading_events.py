"""Domain types and constants for the trading event preparation pipeline."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any


SUPPORTED_SECURITY_TYPES = {"E": "equity", "I": "index"}
EXCHANGE_TIMEZONES = {
    "FR": "Europe/Paris",
    "NL": "Europe/Amsterdam",
    "ETR": "Europe/Berlin",
}


@dataclass(frozen=True)
class NormalizedEvent:
    event_id: str
    symbol: str
    exchange: str
    security_type: str
    trading_date: str
    event_time: datetime
    last_price: float | None
    event_type: str
    available_attributes: tuple[str, ...]
    source_file: str | None
    source_row: int | None
    produced_at: str | None
    original_payload: dict[str, Any]
    original_envelope: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "symbol": self.symbol,
            "exchange": self.exchange,
            "security_type": self.security_type,
            "trading_date": self.trading_date,
            "event_time": self.event_time.isoformat(),
            "last_price": self.last_price,
            "event_type": self.event_type,
            "available_attributes": list(self.available_attributes),
            "source_file": self.source_file,
            "source_row": self.source_row,
            "produced_at": self.produced_at,
            "payload": self.original_payload,
        }


@dataclass(frozen=True)
class Rejection:
    reason_code: str
    details: str


@dataclass(frozen=True)
class DeadLetter:
    event_id: str
    reason_code: str
    details: str
    source_file: str | None
    source_row: int | None
    rejected_at: str
    envelope: dict[str, Any]
    payload: dict[str, Any] | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "event_id": self.event_id,
            "reason_code": self.reason_code,
            "details": self.details,
            "source_file": self.source_file,
            "source_row": self.source_row,
            "rejected_at": self.rejected_at,
            "envelope": self.envelope,
            "payload": self.payload,
        }
