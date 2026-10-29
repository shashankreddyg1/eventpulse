# EventPulse

A real-time event streaming platform built on Apache Kafka: producers publish
orders, payments, and inventory events; stream processors screen for fraud,
aggregate revenue, and route poison messages to a dead-letter queue with replay.

**Live demo:** https://shashankreddyg1.github.io/eventpulse/

> Demo honesty: the GitHub Pages demo simulates the event stream in your
> browser (no backend on a static host). The full pipeline — FastAPI API,
> Kafka consumers, DLQ + replay — runs locally with `docker compose up`
> (Redpanda, Kafka-compatible) or in demo mode with `DEMO_MODE=true`, no
> broker required. The dashboard served by the API uses the real SSE stream.

## What it demonstrates

- **Kafka producers/consumers** — topics `orders`, `payments`, `inventory`,
  consumer group `eventpulse-pipeline`, keyed partitioning.
- **Transport abstraction** — the same pipeline code runs on a real Kafka
  broker (`KafkaBus`, aiokafka) or an in-memory bus (`MemoryBus`) for demo
  mode and tests. If a configured broker is unreachable, the app falls back
  to the in-memory bus instead of crashing.
- **Stream processing** — fraud rules (high-value threshold, order velocity,
  failed-payment bursts), per-type counts, revenue aggregation.
- **Idempotent ingestion** — client-supplied event ids are deduplicated.
- **Dead-letter queue + replay** — poison events (simulated processor
  failures) are quarantined, inspectable, and replayable after the fault
  clears.
- **Live ops dashboard** — SSE event feed, aggregates, DLQ browser with
  one-click replay, fraud flags.

## Quickstart

Real Kafka (Redpanda + API):

```bash
docker compose up
open http://localhost:8000
```

Demo mode, no broker:

```bash
pip install -r requirements.txt
DEMO_MODE=true uvicorn app.main:app --reload
open http://localhost:8000
```

Try it:

```bash
curl -X POST localhost:8000/events -H 'Content-Type: application/json' -d \
  '{"id":"evt-1","type":"order_created","key":"user_7",
    "payload":{"product":"laptop","amount":1299.99}}'

curl localhost:8000/stats
curl localhost:8000/dlq
```

## API

| Method | Path | Description |
|--------|------|-------------|
| POST | `/events` | Ingest an event (idempotent by `id`) |
| GET | `/events?limit=N` | Recent events |
| GET | `/stats` | Counts, revenue, flags, DLQ depth, mode |
| GET | `/flagged` | Fraud-flagged orders |
| GET | `/dlq` | Dead-letter queue |
| POST | `/dlq/{event_id}/replay` | Reprocess a DLQ event |
| GET | `/stream` | SSE live event feed |
| GET | `/health` | Mode + broker status |

## Project structure

```
app/
  main.py       FastAPI app, demo generator, SSE feed
  bus.py        MemoryBus + KafkaBus (one interface)
  consumers.py  fraud rules + shared pipeline + DLQ routing
  models.py     Event / DLQEntry / FlaggedOrder
  store.py      in-memory read models
docs/index.html live dashboard (dual-mode: SSE, or browser simulation)
tests/          pytest suite (bus, fraud, DLQ + replay, API)
docker-compose.yml  Redpanda + API
```

## Tests

```bash
pip install pytest httpx
python -m pytest tests/ -q
```

## Stack

Python 3.11+ · FastAPI · aiokafka · Redpanda (Kafka-compatible) · vanilla JS dashboard
