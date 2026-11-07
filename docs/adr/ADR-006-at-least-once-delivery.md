# ADR-006: At-Least-Once Delivery Semantics

## Status
Accepted

## Context
Distributed systems cannot guarantee exactly-once message transmission across unreliable networks without incurring prohibitive coordination overhead.

## Problem
In the event of worker crashes, network timeouts, or outbox publisher retries, the same task message may be delivered more than once to worker consumers.

## Alternatives Considered
1. **At-Most-Once Delivery:** Messages are fire-and-forgotten. Fast, but risks losing tasks if workers fail.
2. **Exactly-Once Delivery:** Theoretically impossible in arbitrary failure models; requires heavy distributed consensus.
3. **At-Least-Once Delivery with Idempotent Execution:** Messages are guaranteed to arrive at least once, with worker-side deduplication ensuring safe execution.

## Decision
TaskForge implements **At-Least-Once Delivery** semantics paired with idempotent worker handlers and state machine safeguards.

## Advantages
- Resilient against worker, broker, and publisher crashes.
- Tasks are never silently dropped.

## Disadvantages
- Workers must be capable of safely receiving duplicate task messages without causing duplicate side effects.

## Consequences
- The execution engine inspects the current task state before execution; if already in a terminal state (`SUCCESS`, `CANCELLED`, `DEAD_LETTER`), the duplicate message is acknowledged and skipped safely without re-execution.

## Validation Evidence
Verified in `test_p3_06_duplicate_delivery_safety` (Acceptance Criterion P3-06).

