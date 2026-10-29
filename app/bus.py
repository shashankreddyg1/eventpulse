"""Event bus abstraction.

Two implementations share one interface so the pipeline code never knows
which transport it runs on:

- ``MemoryBus``: asyncio queues, used in demo mode and in tests. No broker.
- ``KafkaBus``: aiokafka producer/consumer against any Kafka-compatible
  broker (Apache Kafka, Redpanda, Confluent). Used by docker-compose.

Consumers subscribe with a group name. In memory mode each subscriber gets
its own queue (fan-out, like separate consumer groups). In Kafka mode each
group gets a real consumer group id.
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable, Dict, List

from .models import Event

log = logging.getLogger(__name__)
Handler = Callable[[Event], Awaitable[None]]


class MemoryBus:
    def __init__(self, store=None) -> None:
        self._subscribers: Dict[str, List[asyncio.Queue]] = {}
        self._tasks: List[asyncio.Task] = []
        self._store = store

    async def publish(self, topic: str, event: Event) -> None:
        for queue in self._subscribers.get(topic, []):
            await queue.put(event)
        if self._store is not None:
            self._store.topic_depths[topic] = sum(
                q.qsize() for q in self._subscribers.get(topic, [])
            )

    def subscribe(self, topic: str, group: str, handler: Handler) -> None:
        queue: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(topic, []).append(queue)

        async def _run() -> None:
            while True:
                event = await queue.get()
                try:
                    await handler(event)
                except Exception as exc:  # noqa: BLE001 - consumer must not die
                    log.exception("consumer %s failed on %s: %s", group, event.id, exc)
                finally:
                    queue.task_done()
                    if self._store is not None:
                        self._store.topic_depths[topic] = sum(
                            q.qsize() for q in self._subscribers.get(topic, [])
                        )

        self._tasks.append(asyncio.create_task(_run()))

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()


class KafkaBus:
    """Kafka-backed bus. Import of aiokafka is deferred so demo mode never
    needs the dependency installed."""

    def __init__(self, brokers: str, store=None) -> None:
        self._brokers = brokers
        self._store = store
        self._producer = None
        self._consumers: List = []
        self._tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        from aiokafka import AIOKafkaProducer

        self._producer = AIOKafkaProducer(
            bootstrap_servers=self._brokers,
            value_serializer=lambda v: json.dumps(v, default=str).encode(),
        )
        await self._producer.start()

    async def publish(self, topic: str, event: Event) -> None:
        if self._producer is None:
            raise RuntimeError("KafkaBus not started")
        await self._producer.send_and_wait(topic, event.model_dump(mode="json"))

    def subscribe(self, topic: str, group: str, handler: Handler) -> None:
        from aiokafka import AIOKafkaConsumer

        consumer = AIOKafkaConsumer(
            topic,
            bootstrap_servers=self._brokers,
            group_id=f"eventpulse-{group}",
            value_deserializer=lambda v: json.loads(v.decode()),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
        )
        self._consumers.append(consumer)

        async def _run() -> None:
            await consumer.start()
            try:
                async for msg in consumer:
                    try:
                        await handler(Event(**msg.value))
                    except Exception as exc:  # noqa: BLE001
                        log.exception("consumer %s failed: %s", group, exc)
            finally:
                await consumer.stop()

        self._tasks.append(asyncio.create_task(_run()))

    async def close(self) -> None:
        for task in self._tasks:
            task.cancel()
        if self._producer is not None:
            await self._producer.stop()
