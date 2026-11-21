# Architecture & Debugging Decisions

Please fill out the four sections below concisely before submitting your solution.

## 1. Root-Cause Diagnosis
What specific bottlenecks and failure modes did you observe in the original codebase under concurrent load?

- **Event-loop / CPU behavior:**
    -   The event loop is blocked by CPU-bound operations in `scrub_and_fingerprint` as it performs 120k SHA-256 rounds of synchronous operations inside an async function, preventing other requests from being processed concurrently.
    -   The event loop is also blocked by I/O operations in `send_to_downstream` as it performs synchronous HTTP requests, preventing other requests from being processed.
- **Concurrency & race conditions:**
    -   No concurrency control is implemented, which means that multiple requests can be processed simultaneously, leading to race conditions.
    -  `indest_webhook` function only checks for the presence of `X-Idempotency-Key` header, but doesn't validate its uniqueness.
    -   In the `events` table, there is no unique constraint on the `event_id` column and there is no `idempotency_key` column to track idempotency and prevent duplicate processing.
    -   The `send_to_downstream` function doesn't handle exceptions, it only uses `response.raise_for_status()` so the sender would receive a 500 error and the row would stay as `pending` and never be retried.
- **Resource & connection management:**
    -   Both `save_event` and `mark_delivered` open a new database connection for each query.
    -   In `send_to_downstream`, `httpx.AsyncClient()` is created for each request, which is inefficient.


## 2. Concurrency & Offloading Strategy
How did you separate CPU-bound preprocessing, HTTP acknowledgment, and downstream delivery? Why did you choose your specific concurrency primitives over alternatives (e.g., `asyncio.to_thread` vs. `ProcessPoolExecutor` vs. FastAPI `BackgroundTasks`)?

## 3. Idempotency & SQLite Concurrency
How does your implementation guarantee zero duplicate processing when two identical webhook retries arrive at the exact same millisecond? What SQLite settings or constraints did you rely on?

## 4. Failure Modes & Production Trade-offs
- What happens in your design if the downstream service is completely unreachable for 60 seconds?
- What happens if this service process crashes 50ms after acknowledging a webhook?
- What would break first if we deployed this container with `--workers 4` instead of 1?
