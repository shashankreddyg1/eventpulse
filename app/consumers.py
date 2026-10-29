"""Stream processors: fraud screening and analytics.

Both processors run as bus subscribers, so they execute identically on the
in-memory bus and on real Kafka. A poison event (payload
``fail_processing: true``) raises in the pipeline and lands in the DLQ
instead of killing the consumer.
"""
from __future__ import annotations

from datetime import datetime, timezone

from .models import Event, FlaggedOrder
from .store import Store

HIGH_VALUE_THRESHOLD = 5000.0
VELOCITY_WINDOW_SECONDS = 60
VELOCITY_MAX_ORDERS = 3


def _recent(store: Store, key: str, event_type: str, seconds: int) -> int:
    now = datetime.now(timezone.utc)
    count = 0
    for event in store.recent(500):
        if event.key != key or event.type != event_type:
            continue
        age = (now - event.timestamp).total_seconds()
        if 0 <= age <= seconds:
            count += 1
    return count


def fraud_check(store: Store, event: Event) -> str | None:
    """Return a flag reason, or None if the event looks clean."""
    amount = event.payload.get("amount")
    if isinstance(amount, (int, float)) and amount > HIGH_VALUE_THRESHOLD:
        return f"high_value: amount {amount} exceeds {HIGH_VALUE_THRESHOLD}"
    if event.type == "order_created":
        recent = _recent(store, event.key, "order_created", VELOCITY_WINDOW_SECONDS)
        if recent >= VELOCITY_MAX_ORDERS:
            return f"velocity: {recent} orders in {VELOCITY_WINDOW_SECONDS}s"
    if event.type == "payment_failed":
        recent = _recent(store, event.key, "payment_failed", VELOCITY_WINDOW_SECONDS)
        if recent >= 2:
            return f"failed_burst: {recent} failed payments in {VELOCITY_WINDOW_SECONDS}s"
    return None


async def process_event(store: Store, event: Event) -> None:
    """The shared pipeline: poison check -> log -> fraud screen."""
    if event.payload.get("fail_processing") is True:
        raise ValueError("simulated processor failure (poison event)")
    recorded = store.record(event)
    if not recorded:
        return  # idempotent replay: already processed
    reason = fraud_check(store, event)
    if reason is not None:
        amount = event.payload.get("amount")
        store.flag(
            FlaggedOrder(
                event_id=event.id,
                key=event.key,
                reason=reason,
                amount=amount if isinstance(amount, (int, float)) else None,
            )
        )


async def pipeline_handler(store: Store, event: Event) -> None:
    """Wraps process_event with DLQ routing on failure."""
    try:
        await process_event(store, event)
    except Exception as exc:  # noqa: BLE001 - DLQ is the point
        store.to_dlq(event, str(exc))
