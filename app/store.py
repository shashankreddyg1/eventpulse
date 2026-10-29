"""In-memory read models.

The store is deliberately boring: a ring buffer of recent events, running
aggregates, the fraud-flag list, and the dead-letter queue. It is the same
read model whether events arrived via real Kafka or the in-memory bus, so
the dashboard never cares which transport is underneath.
"""
from __future__ import annotations

import threading
from collections import deque
from datetime import datetime, timezone
from typing import Dict, List, Optional

from .models import DLQEntry, Event, FlaggedOrder


class Store:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.events: deque = deque(maxlen=500)
        self.seen_ids: set = set()
        self.counts: Dict[str, int] = {}
        self.revenue: float = 0.0
        self.flagged: List[FlaggedOrder] = []
        self.dlq: List[DLQEntry] = []
        self.started_at = datetime.now(timezone.utc)
        self.mode = "demo"
        self.broker = None
        self.topic_depths: Dict[str, int] = {}

    def record(self, event: Event) -> bool:
        """Append an event to the log. Returns False if already seen."""
        with self._lock:
            if event.id in self.seen_ids:
                return False
            self.seen_ids.add(event.id)
            self.events.append(event)
            self.counts[event.type] = self.counts.get(event.type, 0) + 1
            if event.type == "payment_succeeded":
                amount = event.payload.get("amount")
                if isinstance(amount, (int, float)):
                    self.revenue += float(amount)
            return True

    def flag(self, entry: FlaggedOrder) -> None:
        with self._lock:
            self.flagged.append(entry)
            self.flagged = self.flagged[-100:]

    def to_dlq(self, event: Event, error: str) -> None:
        with self._lock:
            for entry in self.dlq:
                if entry.event.id == event.id:
                    entry.attempts += 1
                    return
            self.dlq.append(DLQEntry(event=event, error=error))

    def replay_from_dlq(self, event_id: str) -> Optional[Event]:
        with self._lock:
            for i, entry in enumerate(self.dlq):
                if entry.event.id == event_id:
                    return self.dlq.pop(i).event
            return None

    def recent(self, limit: int = 100) -> List[Event]:
        with self._lock:
            return list(self.events)[-limit:]

    def stats(self) -> dict:
        with self._lock:
            return {
                "mode": self.mode,
                "broker": self.broker,
                "uptime_seconds": round(
                    (datetime.now(timezone.utc) - self.started_at).total_seconds(), 1
                ),
                "total_events": len(self.seen_ids),
                "counts": dict(self.counts),
                "revenue": round(self.revenue, 2),
                "flagged": len(self.flagged),
                "dlq_depth": len(self.dlq),
                "topic_depths": dict(self.topic_depths),
            }
