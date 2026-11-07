# ADR-009: Worker Recovery and Leases

## Status
Accepted

## Context
Worker nodes can crash unexpectedly due to hardware faults, OOM kills, power outages, or deployment restarts while executing active tasks.

## Problem
If a worker crashes while processing a task in `RUNNING` status, the task remains perpetually stuck in `RUNNING` without a recovery mechanism, resulting in lost work.

## Alternatives Considered
1. **Short Heartbeat with Global Process Monitor:** External supervisor checks worker PIDs. Fails across distributed machine clusters.
2. **Distributed Leases with Background Auto-Renewal & Reaper:** Worker acquires a time-bounded lease (`lease_token`, `lease_expires_at = now + 30s`) upon picking up a task. While executing, the worker's lease manager thread periodically renews the lease. A background reaper process checks for `RUNNING` tasks whose lease has expired and automatically transitions them to `RETRY_WAIT`.

## Decision
TaskForge implements **Distributed Execution Leases with Auto-Renewal and Background Reaper Sweeps**.

## Advantages
- If a worker crashes, its renewal thread ceases, causing the lease to expire naturally.
- The reaper detects expired leases, increments the retry counter, and re-enqueues the task safely.
- If retries are exhausted, the task safely transitions to `DEAD_LETTER` rather than hanging forever.

## Disadvantages
- Adds database updates during lease renewal (mitigated by setting renewal intervals to half the lease duration).

## Validation Evidence
Verified in `test_p3_07_task_timeout_enforcement`, Experiment E07, and chaos endpoint `/api/v1/chaos/recover-stale`.
