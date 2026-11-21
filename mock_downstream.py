import asyncio
import random
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Mock Flaky Downstream Consumer")

received_events: list[dict] = []
attempt_counts: dict[str, int] = {}


class DownstreamEvent(BaseModel):
    event_id: str
    text: str
    fingerprint: str


@app.post("/reset")
async def reset_state():
    received_events.clear()
    attempt_counts.clear()
    return {"status": "reset"}


@app.get("/stats")
async def get_stats():
    unique_ids = {e["event_id"] for e in received_events}
    return {
        "total_accepted": len(received_events),
        "unique_events": len(unique_ids),
        "events": received_events,
    }


@app.post("/events")
async def receive_event(event: DownstreamEvent):
    # Simulate realistic network + processing latency
    await asyncio.sleep(0.25)

    count = attempt_counts.get(event.event_id, 0) + 1
    attempt_counts[event.event_id] = count

    # Simulate transient 503 failure on ~25% of first attempts
    if count == 1 and random.random() < 0.25:
        raise HTTPException(status_code=503, detail="Upstream service temporarily unavailable")

    received_events.append(event.model_dump())
    return {"accepted": True, "event_id": event.event_id}
