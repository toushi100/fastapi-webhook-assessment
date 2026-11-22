from fastapi import FastAPI, Header, HTTPException, BackgroundTasks, Request
from pydantic import BaseModel
from contextlib import asynccontextmanager
from concurrent.futures import ProcessPoolExecutor
import asyncio

from app.database import init_db, save_event, close_db, reserve_key
from app.downstream import deliver_with_retry, get_client, close_client
from app.processor import scrub_and_fingerprint

@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    app.state.process_pool = ProcessPoolExecutor(max_workers=4)
    await get_client()
    yield

    app.state.process_pool.shutdown()
    await close_client()
    await close_db()
    
app = FastAPI(title="Webhook Ingestion Service", lifespan=lifespan)


class WebhookPayload(BaseModel):
    event_id: str
    tenant_id: str
    content: str





@app.get("/health")
async def health():
    return {"status": "ok"}



async def process_webhook(payload: WebhookPayload, idempotency_key: str,process_pool):
    loop = asyncio.get_event_loop()
    cleaned_text, fingerprint = await loop.run_in_executor( process_pool,scrub_and_fingerprint,payload.content)
    await save_event(payload.event_id, payload.tenant_id, idempotency_key, cleaned_text, fingerprint)
    await deliver_with_retry(payload.event_id, cleaned_text, fingerprint, idempotency_key)





@app.post("/webhooks/ingest")
async def ingest_webhook(request: Request,payload: WebhookPayload, background_tasks: BackgroundTasks,x_idempotency_key: str | None = Header(default=None)       ):
    if not x_idempotency_key:
        raise HTTPException(status_code=400,detail="Missing X-Idempotency-Key header")

    inserted = await reserve_key(payload.event_id, payload.tenant_id, x_idempotency_key)

    if not inserted:
        return {"status": "duplicate"}

    background_tasks.add_task(process_webhook, payload, x_idempotency_key, request.app.state.process_pool)

    return {"status": "accepted"}
