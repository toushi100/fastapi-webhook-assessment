import os
import httpx
import asyncio


from app.database import mark_delivered


DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")
MAX_RETRIES = int(os.getenv("DOWNSTREAM_MAX_RETRIES", "3"))
client: httpx.AsyncClient | None = None

async def get_client() -> httpx.AsyncClient:
    global client
    if client is None:
        client = httpx.AsyncClient()
    return client

async def close_client():
    global client
    if client is not None:
        await client.aclose()
        client = None



async def forward_event( event_id: str, cleaned_text: str, fingerprint: str):

    response = await client.post(
        DOWNSTREAM_URL,
        json={
            "event_id": event_id,
            "text": cleaned_text,
            "fingerprint": fingerprint
        }
    )

    response.raise_for_status()
    return response.json()


async def deliver_with_retry( event_id: str, cleaned_text: str, fingerprint: str, idem_key: str):
    for attempt in range(MAX_RETRIES):
        try:
            await forward_event( event_id, cleaned_text, fingerprint)

            await mark_delivered(idem_key)
            return True

        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code

            # Permanent client errors, except rate limiting
            if 400 <= status_code < 500 and status_code != 429:
                return False

        except httpx.TransportError:
            pass

        if attempt < MAX_RETRIES - 1:
            await asyncio.sleep(
                0.1 * (2 ** attempt)
            )

    return False
