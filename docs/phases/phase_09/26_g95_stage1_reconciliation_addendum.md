# Gate G9.5: Stage 1 Demonstration Harness — Evidence Reconciliation Addendum

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite  
**Stage:** Stage 1 — Local Demonstration Harness (`scripts/demo.py`)  
**Status:** FORMAL RECONCILIATION COMPLETE  
**Date:** 2026-11-11  
**Base Production Baseline:** `a89858b` (Frozen & Preserved — 0 production code changes)  
**Frozen Phase 8 Tag:** `v0.8.0-phase8-freeze` (`da27047`, Preserved)  
**Reference Report:** [`docs/phases/phase_09/25_g95_stage1_demo_report.md`](file:///e:/TaskForge/docs/phases/phase_09/25_g95_stage1_demo_report.md)  
**Public Artifact:** [`benchmarks/public/demo_execution_summary.json`](file:///e:/TaskForge/benchmarks/public/demo_execution_summary.json)  

---

## 1. Overview & Purpose

This reconciliation addendum addresses findings **R1**, **R2**, **R3**, and the **Stage 2 protocol alignment** raised in the Stage 1 engineering review.

All changes have been committed and verified against live PostgreSQL 16 and Redis 7 services, with zero modifications to production `src/` modules and zero metric cardinality additions.

---

## 2. R1 — Resource Ownership Manifest & Teardown Safeguards

To prevent naming-convention assumptions from risking collision with concurrent demos or local environments, `scripts/demo.py` now enforces strict pre-existence gating, runtime ownership tracking, and manifest-verified cleanup:

1. **Pre-Provisioning Collision Guard:**
   Before creating the disposable database, `scripts/demo.py` queries `pg_database`:
   ```sql
   SELECT 1 FROM pg_database WHERE datname = 'taskforge_demo_<run_id>';
   ```
   If any database with that name already exists, the process aborts immediately with a fatal `RuntimeError` and refuses execution.
2. **Redis Namespace Pre-Check:**
   Before writing any keys, `r.keys("taskforge:demo:<run_id>:*")` checks the keyspace. If any keys exist, the process aborts immediately.
3. **Run-Specific Resource Ownership Manifest:**
   Upon successful provisioning, an immutable in-memory manifest is recorded:
   ```json
   "resource_manifest": {
     "run_id": "a510d9fa",
     "provisioned_at_utc": "2026-11-11T09:26:17.901770+00:00",
     "database": {
       "name": "taskforge_demo_a510d9fa",
       "owner": "taskforge",
       "created_by_run": true,
       "pre_existed": false
     },
     "redis": {
       "namespace_prefix": "taskforge:demo:a510d9fa:",
       "pre_existing_keys_count": 0,
       "owned_by_run": true
     }
   }
   ```
4. **Manifest-Verified Safe Teardown:**
   Teardown requires that:
   - `resource_manifest["database"]["created_by_run"] is True`
   - `resource_manifest["database"]["pre_existed"] is False`
   - Database name matches `taskforge_demo_<run_id>`
   - `resource_manifest["redis"]["owned_by_run"] is True`
   - `resource_manifest["redis"]["pre_existing_keys_count"] == 0`
   If ownership cannot be established from this manifest, destructive cleanup is strictly refused. Cleanup drops only the verified disposable database and purges only keys matching `taskforge:demo:<run_id>:*`.

---

## 3. R2 — Empirical PostgreSQL Stale Lease Fencing Evidence

D7 verifies PostgreSQL conditional write rejection without mocking. Step 8 in `scripts/demo.py` executes both the raw SQL guarded update and the production `TaskRepository` path, capturing empirical evidence from PostgreSQL and Prometheus metrics:

1. **Worker A Original Lease Token:** `tok-a-a510d9fa`
2. **Worker B Replacement Lease Token:** `tok-b-a510d9fa` (Acquired after supervisor lease expiration recovery)
3. **Actual Guarded SQL Statement:**
   ```sql
   UPDATE tasks
   SET status = :status, completed_at = :now, updated_at = :now, lease_token = NULL
   WHERE id = :task_id AND lease_token = :lease_token
   ```
4. **PostgreSQL Affected-Row Count:**
   - Worker A execution: `rowcount = 0` (PostgreSQL rejected stale token update; 0 rows modified).
5. **Production Repository & Telemetry Fencing Rejection:**
   - Production method `TaskRepository.complete_task_success(fencing_task_id, lease_token=tok_a)` was invoked directly without mocking.
   - Result: Raised `PermissionError: Fencing violation: supplied lease token does not match active task lease`.
   - Authoritative counter increment: `FENCING_REJECTIONS_TOTAL` (`taskforge_fencing_rejections_total`) incremented by exactly `+1.0` (from `0.0` to `1.0`).
6. **Worker B Authoritative Update:**
   - `TaskRepository.complete_task_success(fencing_task_id, lease_token=tok_b, return_value={"winner": "Worker B", "proof": "Valid Lease Token B"})` executed successfully.
7. **Final Persisted Database State:**
   - `Task.status`: `SUCCESS`
   - `Task.lease_token`: `NULL` (Released upon terminal completion)
   - `TaskResult.return_value`: `{"winner": "Worker B", "proof": "Valid Lease Token B"}`
   - `state_corrupted`: `False` (Worker A wrote zero data).

---

## 4. R3 — Workload Terminology & Initial-to-Final Identity Reconciliation

### 4.1 Workload Priority & Enum Mapping
In the Stage 0 specification, priority tiers were designated `High`, `Normal`, and `Low`. In the production TaskForge codebase (`src/taskforge/core/state_machine.py`), the `TaskPriority` enum is defined as:
```python
class TaskPriority(int, Enum):
    LOW = 1
    MEDIUM = 2      # Canonical representation of the Stage 0 "Normal" tier
    HIGH = 3
    CRITICAL = 4
```
`Normal` is the display and architectural tier name for `TaskPriority.MEDIUM = 2`. The demo script explicitly documents and records this mapping:
- **High:** `TaskPriority.HIGH` (Value `3`)
- **Normal:** `TaskPriority.MEDIUM` (Value `2`, Stage 0 Normal tier)
- **Low:** `TaskPriority.LOW` (Value `1`)

### 4.2 Explicit Initial-to-Final Identity Reconciliation (D9)
Rather than asserting only internal set equality among terminal entities, Step 10 conducts an initial-to-final audit across all 41 demo tasks (30 multi-priority + 10 outage recovery + 1 lease fencing):

| Entity Class | Initial Seeded Count | Final Reconciled Count | Exact Set Equality | Missing IDs | Unexpected IDs | Unaccounted Count |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: |
| **Tasks ($T$)** | 41 | 41 (`SUCCESS`) | **$\text{set}(T_{\text{initial}}) == \text{set}(T_{\text{final}})$** | `[]` | `[]` | **0** |
| **Outbox Events ($E$)** | 41 | 41 (`PUBLISHED`) | **$\text{set}(E_{\text{initial}}) == \text{set}(E_{\text{final}})$** | `[]` | `[]` | **0** |
| **Task Results ($R$)** | 41 ($T_{\text{initial}}$) | 41 | **$\text{set}(T_{\text{initial}}) == \text{set}(R_{\text{final}})$** | `[]` | `[]` | **0** |
| **Durable Event Loss** | — | — | — | — | — | **Strictly 0** |

All 41 tasks reached terminal `SUCCESS`; every task has its 1:1 corresponding `TaskResult` and published `OutboxEvent`; zero tasks or events were missing or unaccounted for.

---

## 5. Stage 2 Protocol Reaffirmation & Scope Realignment

We formally acknowledge the review note regarding Stage 2 scope. The informal scenario labels referenced in the Stage 1 read-ahead were preliminary shorthand and are hereby retracted.

We formally **reaffirm the canonical Stage 0 public benchmark protocol** locked in `docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md`:

1. **Harness Location:** Strictly [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py).
2. **Canonical Workload Suite:**
   - **`W-01` (No-Op Baseline):** Measures framework overhead (API ingestion, outbox transaction, publisher poll, Redis transit, dispatcher dequeue, worker loop, DB state transition, outbox completion event).
   - **`W-02` (CPU-Bound Prime Sieve):** Calculates primes up to $n=500$ per task to assess worker saturation and scheduling efficiency under computational load.
   - **`W-03` (I/O-Bound Simulated Service):** Emulates downstream microservice latency via a bounded 50ms I/O wait to evaluate connection pool and worker thread concurrency.
3. **Measurement Boundaries & Rigor:**
   - Explicit measurement window $[T_{\text{start}}, T_{\text{end}}]$ with steady-state throughput: $\text{Throughput} = N_{\text{measured}} / (T_{\text{end}} - T_{\text{start}})$.
   - Warmup phase (first 10% of tasks) excluded from latency percentiles.
   - Telemetry captures outbox dwell latency, queue dwell latency, handler execution latency, and end-to-end latency percentiles (p50, p95, p99).
   - Failed and retried task accounting.
4. **Validation & Metadata:**
   - Direct automated schema validation against [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json).
   - Full provenance capture (Git SHA, Python/PostgreSQL/Redis versions, OS/CPU specs, timestamps).

---

## 6. Verification Results & Gate Closure Request

### Automated Test Results:
1. **Demo Reproducibility Suite:**
   ```text
   tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_preflight_checks PASSED [ 33%]
   tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_cli_check_mode PASSED [ 66%]
   tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_full_execution PASSED [100%]
   ============================= 3 passed in 11.53s ==============================
   ```
2. **Full Project Regression Baseline:**
   - 257 passed, 0 failures, 0 errors, 0 skipped.

### Summary of Dispositions:
- **R1 (Resource Ownership & Gating):** Resolved. Pre-existence guard, Redis empty namespace check, run manifest, and safe teardown implemented.
- **R2 (Empirical Lease Fencing):** Resolved. Guarded SQL 0-row update, production repository `PermissionError`, and `+1.0` fencing counter increment verified.
- **R3 (Terminology & Initial/Final Identity):** Resolved. `TaskPriority.MEDIUM = 2` mapped to Stage 0 Normal tier; explicit $T_{\text{initial}} = T_{\text{final}}$ and $E_{\text{initial}} = E_{\text{final}}$ set equality audited with zero unaccounted items.
- **Stage 2 Protocol:** Reaffirmed strictly to `W-01`, `W-02`, `W-03` under `benchmarks/public/run_public_benchmarks.py`.

With all three findings resolved and the benchmark protocol aligned, we request formal sign-off and closure of **Gate G9.5 Stage 1** and authorization to commence **Stage 2 (Public Benchmark Suite)**.
