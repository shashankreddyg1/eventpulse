"""EventPulse API.

Run locally (demo mode, no broker needed)::

    DEMO_MODE=true uvicorn app.main:app --reload

Run with real Kafka (see docker-compose.yml)::

    KAFKA_BROKERS=redpanda:9092 DEMO_MODE=false uvicorn app.main:app

If ``KAFKA_BROKERS`` is set but unreachable, the app falls back to the
in-memory bus and reports ``mode: demo (broker unreachable)`` in /health
instead of crashing. The dashboard works in both modes.
"""
from __future__ import annotations

import asyncio
import os
import random
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, StreamingResponse

from .bus import KafkaBus, MemoryBus
from .consumers import pipeline_handler
from .models import EVENT_TYPES, Event
from .store import Store

TOPICS = ("orders", "payments", "inventory")
store = Store()
bus = None

DEMO_CUSTOMERS = [f"user_{i}" for i in range(1, 21)]
DEMO_PRODUCTS = ["headphones", "keyboard", "monitor", "laptop", "mouse", "desk"]


def _demo_event() -> Event:
    """Generate one realistic event. ~4% are poison (DLQ demo), ~3% are
    high value (fraud demo)."""
    event_type = random.choices(
        EVENT_TYPES, weights=[35, 30, 10, 20, 5], k=1
    )[0]
    key = random.choice(DEMO_CUSTOMERS)
    payload: dict = {
        "product": random.choice(DEMO_PRODUCTS),
        "amount": round(random.uniform(15, 900), 2),
    }
    if event_type in ("payment_succeeded", "payment_failed"):
        payload["method"] = random.choice(["card", "wallet", "bank"])
    if random.random() < 0.04:
        payload["fail_processing"] = True
    elif random.random() < 0.03:
        payload["amount"] = round(random.uniform(5200, 12000), 2)
    return Event(
        id=f"evt-{uuid.uuid4().hex[:12]}",
        type=event_type,
        key=key,
        payload=payload,
    )


async def _demo_generator() -> None:
    while True:
        await asyncio.sleep(1.2)
        event = _demo_event()
        try:
            await bus.publish(event.topic(), event)
        except Exception:  # noqa: BLE001 - demo must not kill the app
            pass


async def _sse_feed():
    seen_ids = {e.id for e in store.recent(500)}
    while True:
        current = store.recent(500)
        fresh = [e for e in current if e.id not in seen_ids]
        for event in fresh:
            seen_ids.add(event.id)
            yield f"data: {event.model_dump_json()}\n\n"
        await asyncio.sleep(0.5)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global bus
    brokers = os.environ.get("KAFKA_BROKERS")
    demo_mode = os.environ.get("DEMO_MODE", "true").lower() == "true"
    if brokers and not demo_mode:
        candidate = KafkaBus(brokers, store)
        try:
            await asyncio.wait_for(candidate.start(), timeout=5)
            bus = candidate
            store.mode = "kafka"
            store.broker = brokers
        except Exception as exc:  # noqa: BLE001 - fall back, don't crash
            print(f"[eventpulse] broker unreachable ({exc}); using in-memory bus")
            bus = MemoryBus(store)
            store.mode = "demo (broker unreachable)"
    else:
        bus = MemoryBus(store)
        store.mode = "demo"
    for topic in TOPICS:
        bus.subscribe(topic, "pipeline", lambda e: pipeline_handler(store, e))
    generator_task = None
    if demo_mode:
        generator_task = asyncio.create_task(_demo_generator())
    yield
    if generator_task is not None:
        generator_task.cancel()
    await bus.close()


app = FastAPI(title="EventPulse", version="0.1.0", lifespan=lifespan)


@app.get("/", include_in_schema=False)
async def dashboard():
    index = Path(__file__).resolve().parent.parent / "docs" / "index.html"
    return FileResponse(index)


@app.get("/health")
async def health():
    stats = store.stats()
    return {"status": "ok", "mode": stats["mode"], "broker": stats["broker"]}


@app.post("/events", status_code=202)
async def ingest(event: Event):
    if event.type not in EVENT_TYPES:
        raise HTTPException(status_code=422, detail=f"unknown type: {event.type}")
    await bus.publish(event.topic(), event)
    await asyncio.sleep(0.05)  # let the consumer pipeline pick it up
    return {"accepted": True, "id": event.id, "topic": event.topic()}


@app.get("/events")
async def list_events(limit: int = 100):
    return [e.model_dump(mode="json") for e in store.recent(limit)]


@app.get("/stats")
async def stats():
    return store.stats()


@app.get("/flagged")
async def flagged():
    return [f.model_dump(mode="json") for f in store.flagged]


@app.get("/dlq")
async def dlq():
    return [d.model_dump(mode="json") for d in store.dlq]


@app.post("/dlq/{event_id}/replay")
async def replay(event_id: str):
    event = store.replay_from_dlq(event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="not in DLQ")
    event.payload.pop("fail_processing", None)
    await bus.publish(event.topic(), event)
    await asyncio.sleep(0.05)
    return {"replayed": True, "id": event.id}


@app.get("/stream", include_in_schema=False)
async def stream():
    return StreamingResponse(_sse_feed(), media_type="text/event-stream")
