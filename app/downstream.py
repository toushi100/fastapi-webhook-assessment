import os
import httpx

DOWNSTREAM_URL = os.getenv("DOWNSTREAM_URL", "http://127.0.0.1:9001/events")


async def forward_event(event_id: str, cleaned_text: str, fingerprint: str) -> dict:
    """
    Forwards a processed webhook event to the downstream consumer API.
    """
    async with httpx.AsyncClient() as client:
        response = await client.post(
            DOWNSTREAM_URL,
            json={
                "event_id": event_id,
                "text": cleaned_text,
                "fingerprint": fingerprint,
            },
        )
        response.raise_for_status()
        return response.json()
