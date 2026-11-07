# ADR-002: PostgreSQL as Source of Truth

## Status
Accepted

## Context
Asynchronous task platforms require reliable storage for task definitions, attempt history, audit events, execution results, and transactional outbox events.

## Problem
Relying solely on in-memory message brokers (such as Redis or RabbitMQ) as the primary state store risks catastrophic data loss if the broker restarts, loses persistence snapshots, or experiences network partitions.

## Alternatives Considered
1. **Redis as primary database:** Fast in-memory key-value store, but lacks ACID multi-table transactions, schema constraints, and reliable long-term historical querying.
2. **Document store (MongoDB):** Flexible JSON schema, but lacks relational integrity and strict foreign-key cascades across attempts and events.
3. **PostgreSQL (with SQLite dev fallback):** Relational database with full ACID compliance, row-level locking (`FOR UPDATE`), indexing, and JSONB capabilities.

## Decision
We designate **PostgreSQL** as the definitive source of truth for all persistent entities (`tasks`, `task_attempts`, `task_results`, `task_events`, `outbox_events`).

## Advantages
- Strong ACID transactions guarantee consistent state machine transitions.
- Row-level locking allows safe lease acquisition across concurrent workers.
- Cascade deletes ensure clean garbage collection of attempts and events when tasks are purged.
- Relational integrity ensures no orphaned attempts or results.

## Disadvantages
- Higher disk write latency compared to purely in-memory queues.
- Requires database connection pool management.

## Consequences
- Every task state change is backed by a committed database transaction.
- Read performance is accelerated via composite indexes and read-through caching.

## Validation Evidence
Verified in test criteria P1-01 through P1-08 and AC-03. All 100 submitted tasks persisted and survived application lifecycles.

