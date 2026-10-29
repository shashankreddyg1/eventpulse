"""EventPulse tests. Run: python -m pytest tests/ -q

Tests run the app with DEMO_MODE=false (no background generator) and no
broker, so everything exercises the in-memory bus path.
"""
import os
import time
import uuid

os.environ["DEMO_MODE"] = "false"
os.environ.pop("KAFKA_BROKERS", None)

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402


def uid() -> str:
    return f"evt-test-{uuid.uuid4().hex[:10]}"


def poll(fn, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = fn()
        if result:
            return result
        time.sleep(0.1)
    return None


def post_event(client, event_id, event_type, key, payload):
    resp = client.post(
        "/events",
        json={"id": event_id, "type": event_type, "key": key, "payload": payload},
    )
    assert resp.status_code == 202, resp.text
    return resp.json()


def test_health_reports_demo_mode():
    with TestClient(app) as client:
        resp = client.get("/health")
        assert resp.status_code == 200
        assert resp.json()["status"] == "ok"
        assert "demo" in resp.json()["mode"]


def test_ingest_and_dedupe():
    event_id = uid()
    with TestClient(app) as client:
        post_event(client, event_id, "order_created", "user_1", {"amount": 49.99})
        post_event(client, event_id, "order_created", "user_1", {"amount": 49.99})

        def found():
            events = client.get("/events?limit=500").json()
            return [e for e in events if e["id"] == event_id]

        assert poll(found) is not None
        assert len(found()) == 1, "same id ingested twice must appear once"


def test_unknown_type_rejected():
    with TestClient(app) as client:
        resp = client.post(
            "/events",
            json={"id": uid(), "type": "teleport", "key": "user_1", "payload": {}},
        )
        assert resp.status_code == 422


def test_high_value_order_flagged():
    event_id = uid()
    with TestClient(app) as client:
        post_event(client, event_id, "order_created", "user_2", {"amount": 9999.0})

        def flagged():
            flags = client.get("/flagged").json()
            return [f for f in flags if f["event_id"] == event_id]

        assert poll(flagged) is not None
        assert "high_value" in flagged()[0]["reason"]


def test_velocity_flagged():
    key = f"user_{uuid.uuid4().hex[:6]}"
    with TestClient(app) as client:
        for _ in range(3):
            post_event(client, uid(), "order_created", key, {"amount": 20.0})

        def flagged():
            flags = client.get("/flagged").json()
            return [f for f in flags if f["key"] == key]

        assert poll(flagged) is not None
        assert "velocity" in flagged()[0]["reason"]


def test_dlq_and_replay():
    event_id = uid()
    with TestClient(app) as client:
        post_event(
            client, event_id, "order_created", "user_3",
            {"amount": 10.0, "fail_processing": True},
        )

        def in_dlq():
            dlq = client.get("/dlq").json()
            return [d for d in dlq if d["event"]["id"] == event_id]

        assert poll(in_dlq) is not None
        resp = client.post(f"/dlq/{event_id}/replay")
        assert resp.status_code == 200

        def processed():
            events = client.get("/events?limit=500").json()
            return [e for e in events if e["id"] == event_id]

        assert poll(processed) is not None
        assert poll(lambda: not in_dlq()) is not None
