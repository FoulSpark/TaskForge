# ADR-005: Transactional Outbox Pattern

## Status
Accepted

## Context
When a task is submitted via HTTP, the platform must persist the task record into PostgreSQL AND publish an event to the message broker (Redis).

## Problem
Performing a database commit and a broker publish in separate operations introduces the Dual-Write Problem. If the database commit succeeds but Redis is unreachable, the task is saved but will never execute. If the broker publish succeeds but the database commit fails, workers execute a ghost task with no database record.

## Alternatives Considered
1. **Direct Publish (No Outbox):** Commit to DB, then publish to Redis. Unsafe under network partition or broker crash.
2. **Two-Phase Commit (2PC):** Distributed transaction across DB and Redis. Extremely slow, fragile, and not natively supported by Redis.
3. **Transactional Outbox Table:** The task record and an `outbox_events` record are inserted atomically within the exact same database transaction. A separate asynchronous publisher worker periodically drains pending outbox events and pushes them to Redis.

## Decision
We implement the **Transactional Outbox Pattern** using the `outbox_events` table and a background `OutboxPublisherWorker`.

## Advantages
- Guaranteed zero task loss: Even if Redis is completely offline, tasks are safely committed in PostgreSQL and published automatically upon Redis recovery.
- Atomic state creation: Database transaction commits both task and event together or rolls back both.

## Disadvantages
- Introduces slight propagation latency (poll interval, default 200ms) between task submission and broker arrival.

## Consequences
- Requires background process to drain the outbox.
- Implements at-least-once delivery (ADR-006).

## Validation Evidence
Verified in `test_async_submission_and_outbox_flush`. Outbox events safely staged in DB before publisher dispatch.

