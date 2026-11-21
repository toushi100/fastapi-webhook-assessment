from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel

from app.database import init_db, save_event, mark_delivered
from app.downstream import forward_event
from app.processor import scrub_and_fingerprint

app = FastAPI(title="Webhook Ingestion Service")


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str


@app.on_event("startup")
async def startup():
    await init_db()


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/webhooks/ingest")
async def ingest_webhook(
    payload: WebhookPayload,
    x_idempotency_key: str | None = Header(default=None),
):
    if not x_idempotency_key:
        raise HTTPException(status_code=400, detail="Missing X-Idempotency-Key header")

    cleaned_text, fingerprint = scrub_and_fingerprint(payload.content)
    await save_event(payload.event_id, payload.tenant_id, cleaned_text, fingerprint)
    downstream_res = await forward_event(payload.event_id, cleaned_text, fingerprint)
    await mark_delivered(payload.event_id)

    return {"status": "processed", "downstream": 
    downstream_res}
