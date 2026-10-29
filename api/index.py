"""EventPulse demo-data API for Vercel serverless.

The full stateful pipeline (FastAPI + Kafka via ``docker compose up``) lives
in ``app/`` and runs locally. Serverless functions cannot hold a background
generator or shared state, so this demo API synthesizes the same event shapes
deterministically from the wall clock instead:

- Events are generated per minute-bucket with a seeded RNG, so every instance
  returns the same recent window and the feed always looks live.
- ``/stats``, ``/flagged`` and ``/dlq`` are computed from that same window.
- ``POST /dlq/{id}/replay`` acknowledges the replay; because serverless
  state is not shared, the dashboard filters replayed ids client-side.

This is demo data by design — it is labelled as such on the dashboard.
"""
from __future__ import annotations

import hashlib
import random
from datetime import datetime, timedelta, timezone

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="EventPulse Demo API", version="0.1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

TYPES = (
    "order_created",
    "payment_succeeded",
    "payment_failed",
    "inventory_reserved",
    "inventory_failed",
)
WEIGHTS = (35, 30, 10, 20, 5)
PRODUCTS = ["headphones", "keyboard", "monitor", "laptop", "mouse", "desk"]
WINDOW_MINUTES = 60
EVENTS_PER_MINUTE = 6  # one every ~10s, matching the local demo generator


def _rng_for(bucket: str) -> random.Random:
    seed = int(hashlib.sha256(f"eventpulse-{bucket}".encode()).hexdigest()[:8], 16)
    return random.Random(seed)


def _synthesized_events() -> list:
    """Events for the last WINDOW_MINUTES minutes, oldest first."""
    now = datetime.now(timezone.utc).replace(second=0, microsecond=0)
    events = []
    for back in range(WINDOW_MINUTES, 0, -1):
        minute = now - timedelta(minutes=back)
        rng = _rng_for(minute.isoformat())
        for i in range(EVENTS_PER_MINUTE):
            event_type = rng.choices(TYPES, weights=WEIGHTS, k=1)[0]
            amount = round(rng.uniform(15, 900), 2)
            payload: dict = {
                "product": rng.choice(PRODUCTS),
                "amount": amount,
            }
            if event_type in ("payment_succeeded", "payment_failed"):
                payload["method"] = rng.choice(["card", "wallet", "bank"])
            poison = rng.random() < 0.04
            if poison:
                payload["fail_processing"] = True
            elif rng.random() < 0.03:
                payload["amount"] = round(rng.uniform(5200, 12000), 2)
            events.append(
                {
                    "id": f"demo-{minute.strftime('%Y%m%d%H%M')}-{i}",
                    "type": event_type,
                    "key": f"user_{rng.randint(1, 20)}",
                    "payload": payload,
                    "timestamp": (minute + timedelta(seconds=i * 10)).isoformat(),
                }
            )
    return events


@app.get("/health")
async def health():
    return {"status": "ok", "mode": "demo-api", "broker": None}


@app.get("/events")
async def list_events(limit: int = 100):
    return _synthesized_events()[-limit:]


@app.get("/stats")
async def stats():
    events = _synthesized_events()
    counts: dict = {}
    revenue = 0.0
    for event in events:
        counts[event["type"]] = counts.get(event["type"], 0) + 1
        if event["type"] == "payment_succeeded":
            revenue += float(event["payload"]["amount"])
    flagged = sum(
        1
        for e in events
        if isinstance(e["payload"].get("amount"), (int, float))
        and e["payload"]["amount"] > 5000
    )
    dlq = sum(1 for e in events if e["payload"].get("fail_processing") is True)
    return {
        "mode": "demo-api",
        "broker": None,
        "uptime_seconds": 0,
        "total_events": len(events),
        "counts": counts,
        "revenue": round(revenue, 2),
        "flagged": flagged,
        "dlq_depth": dlq,
        "topic_depths": {},
    }


@app.get("/flagged")
async def flagged():
    out = []
    for event in _synthesized_events():
        amount = event["payload"].get("amount")
        if isinstance(amount, (int, float)) and amount > 5000:
            out.append(
                {
                    "event_id": event["id"],
                    "key": event["key"],
                    "reason": f"high_value: amount {amount} exceeds 5000.0",
                    "amount": amount,
                    "flagged_at": event["timestamp"],
                }
            )
    return out


@app.get("/dlq")
async def dlq():
    return [
        {
            "event": event,
            "error": "simulated processor failure (poison event)",
            "attempts": 1,
            "failed_at": event["timestamp"],
        }
        for event in _synthesized_events()
        if event["payload"].get("fail_processing") is True
    ]


@app.post("/dlq/{event_id}/replay")
async def replay(event_id: str):
    # Stateless: the dashboard filters this id client-side after a replay.
    return {"replayed": True, "id": event_id}
