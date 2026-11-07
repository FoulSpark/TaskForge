# ADR-004: Celery and Custom Async Dispatcher as Execution Frameworks

## Status
Accepted

## Context
Worker processes need to consume task messages, execute registered computational workloads, enforce timeouts, handle failures, and publish results.

## Problem
Relying strictly on an external Celery worker process can increase onboarding friction for developers running on local machines without Docker or Redis installed, while pure in-process queues cannot scale horizontally across independent nodes.

## Alternatives Considered
1. **Celery Only:** Industry standard in the Python ecosystem, robust features, but historically complex to configure on Windows without prefork limitations.
2. **RQ (Redis Queue) Only:** Simpler than Celery, but lacks multi-broker support and fine-grained lease timeouts.
3. **Dual Execution Architecture (Celery + Custom Async Dispatcher):** Provide standard Celery tasks (`taskforge.workers.celery_app`) for production Redis worker fleets, while providing an embedded, zero-friction Python `WorkerDispatcher` for local developer testing, self-contained benchmarking, and automated test execution.

## Decision
We implement a **Dual Execution Architecture**. `WorkerDispatcher` provides lightweight in-process and CLI worker concurrency, while `celery_app` enables distributed Celery multi-worker deployment.

## Advantages
- Out-of-the-box local execution on Windows without requiring separate terminal worker daemons.
- Full compatibility with standard production Celery worker commands (`celery -A taskforge.workers.celery_app worker`).
- Seamless pytest integration without external daemon dependencies.

## Disadvantages
- Requires maintaining consistency between Celery task invocation and Dispatcher execution.

## Consequences
- Core execution logic is isolated in `taskforge.workers.execution_engine.execute_task()`, which both Celery and the Dispatcher invoke as a shared entry point.

## Validation Evidence
Verified in `tests/integration/test_execution.py` across all 5 workload types and in automated benchmark runner.

