# ADR-007: Idempotency Mechanism

## Status
Accepted

## Context
Clients may re-submit HTTP task requests during network retries, connection drops, or client-side timeout loops.

## Problem
Without idempotency controls, repeated client submissions result in duplicate computational tasks, wasted resources, and corrupted state in non-idempotent operations.

## Alternatives Considered
1. **Client-side hashing of payload:** Implicit deduplication based on content hash. Fragile if timestamps or random IDs exist in payload.
2. **Explicit Client Idempotency Keys:** Client supplies an optional `idempotency_key` via request body or `Idempotency-Key` header.
3. **No deduplication:** Burden placed entirely on end user.

## Decision
TaskForge implements **Client-Supplied Idempotency Keys** backed by a unique index on the `tasks.idempotency_key` database column.

## Advantages
- Explicit control for clients.
- If a task with the key already exists, the API returns HTTP 200 with the existing task record without queueing a new task or running duplicate work.
- Database unique constraint prevents race conditions during concurrent duplicate submissions.

## Disadvantages
- Clients must generate and track unique keys (e.g. UUIDv4 or business transaction IDs).

## Consequences
- Deduplication is atomic at the database level.
- Tested and verified up to 100 concurrent duplicate requests in storm tests.

## Validation Evidence
Verified in `test_p3_01_idempotency_100_repeated_submissions` (100 submissions produce 1 logical task).
