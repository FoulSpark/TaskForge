# ADR-003: Redis as Message Broker

## Status
Accepted

## Context
Asynchronous task dispatching requires high-throughput, low-latency queueing between API producers and distributed worker consumers.

## Problem
Using relational database polling alone for task queueing can lead to lock contention, table bloat, and elevated database CPU consumption under high throughput.

## Alternatives Considered
1. **PostgreSQL SKIP LOCKED Queue:** Eliminates external broker dependency, but increases database write and polling pressure.
2. **Apache Kafka:** Highly scalable distributed log, but introduces heavy operational overhead (Zookeeper/KRaft, partition management) disproportionate to single-node and local-first requirements.
3. **RabbitMQ:** Feature-rich AMQP broker, but requires additional container configuration and management.
4. **Redis (with In-Memory Fallback):** Extremely fast, lightweight, supports sorted sets for priority queuing, pub/sub, key expiry for heartbeats, and caching.

## Decision
We select **Redis** as the message broker, complemented by a zero-dependency thread-safe in-memory priority queue fallback for local test environments and standalone execution.

## Advantages
- Ultra-low latency enqueue and dequeue operations (< 1 ms).
- Priority queues easily mapped via Redis Sorted Sets (`ZSET`).
- Key expiration (`TTL`) naturally implements worker heartbeat monitoring.
- Serves double duty as read cache for task lookups.

## Disadvantages
- Redis memory is finite and requires persistence configuration (`AOF`) to prevent data loss on container restart.

## Consequences
- The transactional outbox pattern (ADR-005) is mandatory to guarantee zero task loss even if Redis crashes or is temporarily unavailable.

## Validation Evidence
Verified in Phase 2 multi-worker execution tests and Phase 4 priority scheduling experiments.
