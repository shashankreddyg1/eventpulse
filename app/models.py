"""Event model for EventPulse.

Events are the unit of the pipeline. Every event carries an idempotency id,
a type, a partition key (usually a user or order id), and a JSON payload.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from pydantic import BaseModel, Field


EVENT_TYPES = (
    "order_created",
    "payment_succeeded",
    "payment_failed",
    "inventory_reserved",
    "inventory_failed",
)

TOPIC_FOR_TYPE = {
    "order_created": "orders",
    "payment_succeeded": "payments",
    "payment_failed": "payments",
    "inventory_reserved": "inventory",
    "inventory_failed": "inventory",
}


class Event(BaseModel):
    id: str = Field(..., description="Client-supplied idempotency id")
    type: str = Field(..., description=f"One of {', '.join(EVENT_TYPES)}")
    key: str = Field(..., description="Partition key, e.g. user_7 or order_123")
    payload: Dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))

    def topic(self) -> str:
        return TOPIC_FOR_TYPE.get(self.type, "events")


class DLQEntry(BaseModel):
    event: Event
    error: str
    attempts: int = 1
    failed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class FlaggedOrder(BaseModel):
    event_id: str
    key: str
    reason: str
    amount: Optional[float] = None
    flagged_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
