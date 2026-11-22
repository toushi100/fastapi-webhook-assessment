import os
import aiosqlite

DB_PATH = os.getenv("WEBHOOK_DB_PATH", "webhooks.db")
db: aiosqlite.Connection | None = None

async def init_db(db_path: str | None = None):
    path = db_path or os.getenv("WEBHOOK_DB_PATH", DB_PATH)
    global db
    db = await aiosqlite.connect(path)
    

    await db.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                event_id TEXT NOT NULL ,
                tenant_id TEXT NOT NULL,
                idempotency_key TEXT NOT NULL UNIQUE,
                cleaned_text TEXT ,
                fingerprint TEXT ,
                status TEXT NOT NULL DEFAULT 'pending',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    
    # Create index for faster lookups
    await db.execute("""
        CREATE INDEX IF NOT EXISTS idx_events_idempotency_key 
        ON events(idempotency_key)
    """)
    await db.commit()

async def close_db() -> None:
    global db
    if db is not None:
        await db.close()
        db = None
 
 
def get_db() -> aiosqlite.Connection:
    if db is None:
        raise RuntimeError("database not initialized")
    return db

async def reserve_key(
    event_id: str,
    tenant_id: str,
    idempotency_key: str,
):
    cursor = await db.execute(
        """
        INSERT OR IGNORE INTO events (event_id, tenant_id, idempotency_key)
        VALUES (?, ?, ?)
        """,
        (event_id, tenant_id, idempotency_key),
    )
    await db.commit()
    return  cursor.rowcount == 1


async def save_event(
    event_id: str,
    tenant_id: str,
    idempotency_key: str,
    cleaned_text: str,
    fingerprint: str ,
):

    await db.execute("""
        UPDATE events
        SET cleaned_text = ?,
            fingerprint = ?
        WHERE event_id = ?
          AND tenant_id = ?
          AND idempotency_key = ?
    """, (
        cleaned_text,
        fingerprint,
        event_id,
        tenant_id,
        idempotency_key
    ))
    await db.commit()


async def mark_delivered(idempotency_key: str):
    await db.execute(
        "UPDATE events SET status = 'delivered' WHERE idempotency_key = ?",
        (idempotency_key,),
    )
    await db.commit()





