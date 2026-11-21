# Architecture & Debugging Decisions

Please fill out the four sections below concisely before submitting your solution.

## 1. Root-Cause Diagnosis
What specific bottlenecks and failure modes did you observe in the original codebase under concurrent load?

- **Event-loop / CPU behavior:**
- **Concurrency & race conditions:**
- **Resource & connection management:**

## 2. Concurrency & Offloading Strategy
How did you separate CPU-bound preprocessing, HTTP acknowledgment, and downstream delivery? Why did you choose your specific concurrency primitives over alternatives (e.g., `asyncio.to_thread` vs. `ProcessPoolExecutor` vs. FastAPI `BackgroundTasks`)?

## 3. Idempotency & SQLite Concurrency
How does your implementation guarantee zero duplicate processing when two identical webhook retries arrive at the exact same millisecond? What SQLite settings or constraints did you rely on?

## 4. Failure Modes & Production Trade-offs
- What happens in your design if the downstream service is completely unreachable for 60 seconds?
- What happens if this service process crashes 50ms after acknowledging a webhook?
- What would break first if we deployed this container with `--workers 4` instead of 1?
