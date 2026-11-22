# Architecture & Debugging Decisions

Please fill out the four sections below concisely before submitting your solution.

## 1. Root-Cause Diagnosis
What specific bottlenecks and failure modes did you observe in the original codebase under concurrent load?

- **Event-loop / CPU behavior:**
    -   The event loop is blocked by CPU-bound operations in `scrub_and_fingerprint` as it performs 120k SHA-256 rounds of synchronous operations inside an async function, preventing other requests from being processed concurrently.
    -   The event loop is also blocked by I/O operations in `send_to_downstream` as it performs synchronous HTTP requests, preventing other requests from being processed.
- **Concurrency & race conditions:**
    -   No concurrency control is implemented, which means that multiple requests can be processed simultaneously, leading to race conditions.
    -   `indest_webhook` function only checks for the presence of `X-Idempotency-Key` header, but doesn't validate its uniqueness.
    -   In the `events` table, there is no unique constraint on the `event_id` column and there is no `idempotency_key` column to track idempotency and prevent duplicate processing.
    -   The `send_to_downstream` function doesn't handle exceptions, it only uses `response.raise_for_status()` so the sender would receive a 500 error and the row would stay as `pending` and never be retried.
- **Resource & connection management:**
    -   Both `save_event` and `mark_delivered` open a new database connection for each query.
    -   In `send_to_downstream`, `httpx.AsyncClient()` is created for each request, which is inefficient.


## 2. Concurrency & Offloading Strategy
How did you separate CPU-bound preprocessing, HTTP acknowledgment, and downstream delivery? Why did you choose your specific concurrency primitives over alternatives (e.g., `asyncio.to_thread` vs. `ProcessPoolExecutor` vs. FastAPI `BackgroundTasks`)?

- **CPU-bound preprocessing:** 
    -   `scrub_and_fingerprint` (120k SHA-256 rounds) is dispatched via `loop.run_in_executor` to a `ProcessPoolExecutor(max_workers=4)` created in the lifespan. I chose it over `asyncio.to_thread` since the GIL would serialize the hashing work across threads without real concurrency.
- **HTTP acknowledgment:** 
    -   `ingest_webhook` returns `{"status": "accepted"}` immediately after `reserve_key` commits the idempotency row. Only the DB insert happens on the request path, so the sender is acked in milliseconds regardless of downstream or CPU load.
- **Downstream delivery:**
    -   `process_webhook` runs as a FastAPI `BackgroundTask`, so delivery happens after the response without blocking the sender. I chose `BackgroundTasks` over a separate queue/worker since delivery is already tracked in SQLite (`status='pending'`/`'delivered'`), which acts as the durable outbox — the row is written before ack, so work survives the ack boundary.
- **HTTP client:**
    -   `deliver_with_retry` uses a shared `httpx.AsyncClient` created in the lifespan rather than `asyncio.to_thread`, since native async I/O yields the event loop during network waits, which would contend for the GIL and need pool management. Threads are only needed for sync libraries that cannot be awaited.


## 3. Idempotency & SQLite Concurrency
How does your implementation guarantee zero duplicate processing when two identical webhook retries arrive at the exact same millisecond? What SQLite settings or constraints did you rely on?

- **Atomic reservation:** 
    -   `reserve_key` executes `INSERT OR IGNORE` against a `UNIQUE` constraint on `idempotency_key`. Two concurrent requests with the same key race on the same unique index; exactly one `INSERT` succeeds (`rowcount == 1`) and the loser gets `rowcount == 0` and returns `{"status": "duplicate"}` without scheduling any work. The check-and-set is a single SQL statement, so there is no check-then-act window even at the same millisecond.
- **Ordering guarantee:** 
    -   The reservation is committed *before* the `accepted` response, so a retry can never observe an acked-but-unreserved event.
- **SQLite settings:** 
    -   The schema relies on the `UNIQUE` constraint as the sole dedup mechanism. `aiosqlite` serializes statements on a single connection per process, and SQLite's own write locking serializes across processes, so the unique-index check is atomic. I did not enable WAL or `busy_timeout`; under the single-worker deployment the connection-level serialization is sufficient.
- **Known limitation:** 
    -   `idempotency_key` is the dedup key only — `event_id`/`tenant_id` are stored alongside it but not part of the uniqueness check, so two different payloads sharing a key would dedupe to the first one.

## 4. Failure Modes & Production Trade-offs
- What happens in your design if the downstream service is completely unreachable for 60 seconds?
    -   `deliver_with_retry` makes 3 attempts with exponential backoff (0.1s, 0.2s) and a ~5s `httpx` timeout per attempt, so it gives up after roughly 15 seconds and returns `False` without raising.
    -   The `events` row stays `status='pending'` forever — there is no background sweeper or dead-letter requeue, so the event is silently stranded rather than retried. The sender already received `accepted`, so it will not retry on its own either.
- What happens if this service process crashes 50ms after acknowledging a webhook?
    -   The `events` row exists (committed by `reserve_key` before the response) but `cleaned_text`/`fingerprint` are NULL and `status='pending'`, because `process_webhook` hadn't finished the CPU work or delivery.
    -   Any sender retry hits `INSERT OR IGNORE` → `duplicate` and is never reprocessed, so the event is permanently lost. There is no startup recovery scan for stale `pending` rows; this is the main durability gap in the design.
- What would break first if we deployed this container with `--workers 4` instead of 1?
    -   SQLite write contention: all 4 worker processes share one `webhooks.db` file over separate `aiosqlite` connections with no WAL mode or `busy_timeout`, so concurrent commits can raise `database is locked` on `reserve_key` — ingest itself would start returning 500s before downstream delivery is even reached.
    -   Secondary: each worker also spins up its own `ProcessPoolExecutor(4)`, so CPU preprocessing would scale to 16 worker processes, oversubscribing the container.
