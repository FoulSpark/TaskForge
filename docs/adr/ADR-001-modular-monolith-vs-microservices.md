# ADR-001: Modular Monolith vs Microservices

## Status
Accepted

## Context
TaskForge requires task submission, persistent state management, queuing, distributed worker execution, monitoring, and administrative APIs. When architecting distributed platforms, engineering teams frequently default to decomposing every capability into standalone microservices.

## Problem
Splitting an early-stage distributed processing platform into separate microservices introduces network partition complexities, distributed transactions, deployment friction, and high operational overhead before domain boundaries are proven.

## Alternatives Considered
1. **Full Microservices Architecture:** Independent services for API, Queue Dispatcher, State Manager, and Audit Logger communicating via gRPC.
2. **Traditional Monolith:** All components coupled tightly in a single codebase with shared in-memory states and synchronous calls.
3. **Modular Monolith:** Single deployment artifact with strictly segregated domain modules (API, core, database, queue, workers, monitoring) sharing a single persistence layer and decoupled via explicit interfaces.

## Decision
We choose the **Modular Monolith** architecture for TaskForge. All domain logic resides within structured modules in `taskforge.*`, while background workers can be executed in-process or scaled horizontally as independent standalone worker processes.

## Advantages
- Simplified local development and reproducible Docker deployment.
- High developer velocity and atomic transactions across domain models.
- Clean module boundaries allow easy future extraction into standalone services if horizontal scale demands it.
- Low latency without internal network hops.

## Disadvantages
- Worker and API share the same codebase dependencies.
- A bug in shared core code could impact both worker and API components.

## Consequences
- Domain dependencies must remain strictly decoupled. Cross-module calls must go through repositories or service interfaces.
- Horizontal scaling is achieved by running multiple worker process instances of the same container image.

## Validation Evidence
Verified in Phase 0 and Phase 1 test suites. Clean startup, zero cross-service network flakiness, and test runs executing in seconds.

