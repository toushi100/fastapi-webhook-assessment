import asyncio
import os
import sqlite3
import subprocess
import sys
import time
import httpx
import pytest

INGEST_PORT = 8901
DOWNSTREAM_PORT = 9001
INGEST_URL = f"http://127.0.0.1:{INGEST_PORT}"
DOWNSTREAM_URL = f"http://127.0.0.1:{DOWNSTREAM_PORT}"


async def _wait_for_server(url: str, timeout: float = 10.0):
    deadline = time.monotonic() + timeout
    async with httpx.AsyncClient() as client:
        while time.monotonic() < deadline:
            try:
                r = await client.get(url)
                if r.status_code < 500:
                    return
            except Exception:
                pass
            await asyncio.sleep(0.1)
    raise RuntimeError(f"Server at {url} did not start within {timeout}s")


@pytest.fixture
async def running_services(tmp_path):
    db_file = tmp_path / "test_webhooks.db"
    env = os.environ.copy()
    env["WEBHOOK_DB_PATH"] = str(db_file)
    env["DOWNSTREAM_URL"] = f"{DOWNSTREAM_URL}/events"

    downstream_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "mock_downstream:app", "--host", "127.0.0.1", "--port", str(DOWNSTREAM_PORT), "--log-level", "warning"],
        env=env,
    )
    ingest_proc = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(INGEST_PORT), "--log-level", "warning"],
        env=env,
    )

    try:
        await _wait_for_server(f"{DOWNSTREAM_URL}/stats")
        await _wait_for_server(f"{INGEST_URL}/health")
        yield db_file
    finally:
        ingest_proc.terminate()
        downstream_proc.terminate()
        ingest_proc.wait(timeout=5)
        downstream_proc.wait(timeout=5)


@pytest.mark.asyncio
async def test_burst_traffic_idempotency_and_event_loop_health(running_services):
    db_file = running_services
    unique_count = 30

    # Build 60 requests: 30 unique events, each duplicated with the same idempotency key
    requests_to_send = []
    for i in range(unique_count):
        event_id = f"evt_{i:03d}"
        idem_key = f"idem_{i:03d}"
        payload = {
            "event_id": event_id,
            "tenant_id": f"tenant_{i % 3}",
            "content": f"Customer user{i}@example.com SSN 123-45-6789 submitted ticket #{i}",
        }
        requests_to_send.append((idem_key, payload))
        requests_to_send.append((idem_key, payload))  # Concurrent retry duplicate

    health_latencies_ms: list[float] = []
    ingest_latencies_ms: list[float] = []
    stop_health_probe = asyncio.Event()

    async with httpx.AsyncClient(timeout=15.0) as client:
        await client.post(f"{DOWNSTREAM_URL}/reset")

        async def probe_health():
            while not stop_health_probe.is_set():
                t0 = time.perf_counter()
                resp = await client.get(f"{INGEST_URL}/health")
                elapsed_ms = (time.perf_counter() - t0) * 1000
                assert resp.status_code == 200
                health_latencies_ms.append(elapsed_ms)
                await asyncio.sleep(0.05)

        async def send_webhook(idem_key: str, payload: dict):
            t0 = time.perf_counter()
            resp = await client.post(
                f"{INGEST_URL}/webhooks/ingest",
                json=payload,
                headers={"X-Idempotency-Key": idem_key},
            )
            elapsed_ms = (time.perf_counter() - t0) * 1000
            ingest_latencies_ms.append(elapsed_ms)
            return resp

        probe_task = asyncio.create_task(probe_health())
        try:
            responses = await asyncio.gather(
                *(send_webhook(key, body) for key, body in requests_to_send)
            )
        finally:
            stop_health_probe.set()
            await probe_task

        # 1. All 60 webhook requests must succeed quickly (200 or 202)
        status_codes = [r.status_code for r in responses]
        assert all(code in (200, 202) for code in status_codes), f"Failed responses: {status_codes}"

        ingest_latencies_ms.sort()
        p95_ingest = ingest_latencies_ms[int(len(ingest_latencies_ms) * 0.95) - 1]
        assert p95_ingest < 250.0, f"Ingest p95 latency too high: {p95_ingest:.1f}ms (target < 250ms)"

        # 2. Event loop must never stall during burst traffic
        max_health = max(health_latencies_ms)
        assert max_health < 50.0, f"Event loop starved! Max /health latency: {max_health:.1f}ms (target < 50ms)"

        # 3. Allow background delivery & retries to settle
        deadline = time.monotonic() + 6.0
        stats = {}
        while time.monotonic() < deadline:
            stats_resp = await client.get(f"{DOWNSTREAM_URL}/stats")
            stats = stats_resp.json()
            if stats["unique_events"] == unique_count:
                break
            await asyncio.sleep(0.2)

        assert stats["unique_events"] == unique_count, (
            f"Expected {unique_count} delivered events, got {stats['unique_events']}"
        )
        assert stats["total_accepted"] == unique_count, (
            f"Duplicate deliveries detected! Total accepted: {stats['total_accepted']} vs unique: {stats['unique_events']}"
        )

    # 4. Verify SQLite database integrity & PII redaction
    conn = sqlite3.connect(db_file)
    rows = conn.execute("SELECT event_id, cleaned_text, status FROM events").fetchall()
    conn.close()

    assert len(rows) == unique_count, f"Expected {unique_count} DB rows, found {len(rows)}"
    for event_id, cleaned_text, status in rows:
        assert "123-45-6789" not in cleaned_text, f"PII leak in {event_id}"
        assert "@example.com" not in cleaned_text, f"Email leak in {event_id}"
        assert status == "delivered", f"Event {event_id} stuck in status '{status}'"
