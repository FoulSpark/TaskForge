# Failure Analysis: FA-001 — Worker Crash and Lease Expiration

**Component:** Worker Pool & Execution Engine  
**Severity:** High  
**Failure Mode:** Worker process terminates ungracefully (`SIGKILL`, host OOM, hardware failure) while actively executing a task.

---

## 1. Problem Statement

If a worker process crashes mid-task, the task remains marked as `RUNNING` in the database. In traditional architectures without leasing, the task remains stuck in `RUNNING` forever ("zombie task").

---

## 2. Root Cause Analysis

Processes cannot guarantee execution of termination cleanup handlers when killed via `SIGKILL` or hardware power outage.

---

## 3. Mitigation & Recovery Architecture

TaskForge mitigates this via **Distributed Execution Leases** and a **Stale Lease Recovery Reaper**:

1. **Lease Acquisition:** Before executing any task, the worker must write a cryptographic `lease_token` and a `lease_expires_at = now + 30s` timestamp to the database row.
2. **Lease Renewal:** For tasks running longer than 10 seconds, the worker renews the lease timestamp via background heartbeats.
3. **Detection (Reaper):** A background daemon periodically queries:
   ```sql
   SELECT id FROM tasks 
   WHERE status = 'RUNNING' 
     AND lease_expires_at < CURRENT_TIMESTAMP;
   ```
4. **Recovery Action:**
   - Increments `retry_count`.
   - Records an aborted `TaskAttempt` ("Worker lease expired: presumed crashed").
   - If `retry_count <= max_retries`: resets `status = 'RETRY_WAIT'` and reenqueues to the broker.
   - If `retry_count > max_retries`: marks `status = 'DEAD_LETTER'`.

---

## 4. Verification Evidence

Verified by automated integration test `tests/reliability/test_reliability.py`. Stale tasks are reaped and completed without operator intervention.

