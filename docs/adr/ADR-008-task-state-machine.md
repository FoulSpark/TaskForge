# ADR-008: Task State Machine

## Status
Accepted

## Context
Tasks transition through multiple phases during their lifecycle from initial creation to eventual completion or failure.

## Problem
Allowing arbitrary or unvalidated state mutations leads to race conditions, zombie tasks, inconsistent attempt records, and unrecoverable system states (e.g. transitioning directly from `PENDING` to `SUCCESS` or modifying a `CANCELLED` task).

## Alternatives Considered
1. **Ad-hoc string status column:** Freeform updates across services. Prone to invalid status bugs.
2. **Database ENUM with stored procedures:** Highly coupled to specific SQL dialects, difficult to unit test without DB.
3. **Explicit Python State Machine with Database Validation:** Enforced transition graph defined in `taskforge.core.state_machine` validating every transition in application code and recorded in `task_events`.

## Decision
We enforce a **Strict Task State Machine** with 8 explicit states:
- `PENDING`
- `QUEUED`
- `RUNNING`
- `SUCCESS` (Terminal)
- `RETRY_WAIT`
- `FAILED`
- `DEAD_LETTER` (Terminal)
- `CANCELLED` (Terminal)

### Legal Transition Graph
- `PENDING` → `QUEUED`, `CANCELLED`
- `QUEUED` → `RUNNING`, `CANCELLED`
- `RUNNING` → `SUCCESS`, `RETRY_WAIT`, `FAILED`, `CANCELLED`
- `RETRY_WAIT` → `QUEUED`, `CANCELLED`
- `FAILED` → `DEAD_LETTER`
- Terminal states (`SUCCESS`, `DEAD_LETTER`, `CANCELLED`) have zero outgoing transitions.

## Advantages
- Every transition is validated; illegal transitions raise `InvalidStateTransitionError` (HTTP 409).
- Full auditability via `task_events` recording timestamp, previous state, new state, and metadata reason.

## Disadvantages
- State transitions require explicit calls rather than raw database column updates.

## Validation Evidence
Verified in `tests/unit/test_state_machine.py`. 100% legal transitions accepted, all illegal transitions rejected.
