# Gate G9.5: Stage 1 Demonstration Harness Verification Report

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite  
**Stage:** Stage 1 — Reproducible Local Demonstration Harness (`scripts/demo.py`)  
**Status:** IMPLEMENTATION COMPLETE & VERIFIED  
**Date:** 2026-11-11  
**Base Production Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Preserved & Immutable)  
**Frozen Phase 8 Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**Protocol Lock Reference:** `docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md` & `24_g95_stage0_reconciliation_addendum.md` (Commit `7a6dd87`)

---

## 1. Executive Summary

In accordance with the Stage 0 Scoping & Protocol Review and Reconciliation Addendum (Commit `7a6dd87`), Stage 1 delivers a fully self-contained, reproducible, and verifiable demonstration harness: [`scripts/demo.py`](file:///e:/TaskForge/scripts/demo.py).

The harness enables an external engineer or evaluator to clone the repository, run a single command (`python scripts/demo.py`), and observe all core architectural invariants, failure-recovery mechanics, and priority guarantees operating against real PostgreSQL and Redis services without manual setup or state contamination.

### Key Milestones Achieved:
1. **Full D1–D12 Compliance:** All twelve acceptance criteria established in Stage 0 are strictly satisfied.
2. **Deterministic Multi-Priority Ordering (D4, D5):** Validated `ZPOPMAX` score encoding (`score = (priority * 10^12) - seq`) against a 30-task workload seeded in reverse order, verifying strict inter-tier precedence (`HIGH > MEDIUM > LOW`) and intra-tier FIFO arrival ordering.
3. **Real OS TCP Transport Interruption (D6):** Interrupted live broker traffic for 3.11s via an in-process raw socket proxy (`RealSocketTCPProxy`). Active worker threads survived, backed off exponentially, and resumed upon socket restoration with **0 durable event loss**.
4. **Authoritative PostgreSQL Stale-Token Fencing (D7):** Verified conditional write rejection (`WHERE lease_token = :token`) against a stale worker attempting terminal state update after supervisor failover, while authoritative replacement Worker B committed successfully.
5. **Terminal Duplicate Message Suppression (D8):** Injected duplicate delivery into Redis for an already completed task; worker verified PostgreSQL terminal status, safely suppressed execution, and acknowledged the message as `TERMINAL_DUPLICATE` without state regression.
6. **Complete Identity Reconciliation (D9, D10):** Proved exact set equality across 41 seeded tasks, 41 persisted results, and 41 published outbox events ($T_{\text{seeded}} = T_{\text{success}} = R_{\text{persisted}} = E_{\text{published}}$) with 100.0% terminal completion.
7. **Automated Verification & Zero Regression:** Verified via automated pytest suite [`tests/integration/test_g95_demo_reproducibility.py`](file:///e:/TaskForge/tests/integration/test_g95_demo_reproducibility.py) (3 passed in 10.78s) and the full project regression suite (257 passed in 370s, 0 failures, 0 skips).

---

## 2. Acceptance Criteria Fulfillment Matrix (D1–D12)

| Criterion | Requirement | Implementation & Empirical Evidence | Status |
| :--- | :--- | :--- | :---: |
| **D1: Environment Validation** | Pre-flight validation (`--check`) of Python $\ge 3.12$, PostgreSQL 16 on 5432, Redis 7 on 6379, and Alembic migrations. | Implemented in `run_preflight_checks()`. Verifies runtime version, live PostgreSQL socket query (`SELECT version()`), live Redis PING (`INFO server`), Docker availability, and Alembic configuration presence. Exits with code 0. | **SATISFIED** |
| **D2: Isolated Resources** | Disposable database `taskforge_demo_<run_id>` and Redis prefix `taskforge:demo:<run_id>:*`. | Step 2 generates 8-char hex UUID `run_id`, provisions isolated PostgreSQL DB, sets isolated Redis keyspace, and reconfigures global SessionLocal without touching production/dev databases. | **SATISFIED** |
| **D3: Automatic Migrations** | Schema applied to `HEAD` via Alembic without manual intervention. | Step 3 clears SQLAlchemy settings cache and programmatically executes `alembic.command.upgrade(alembic_cfg, "head")` against disposable DB URL. | **SATISFIED** |
| **D4: Deterministic Workload** | Multi-priority 30-task workload (10 High, 10 Medium, 10 Low). | Step 4 seeds exactly 30 tasks with transactional outbox events in intentional reverse arrival order (Low first, then Medium, then High). | **SATISFIED** |
| **D5: Priority Ordering** | Verification of `ZPOPMAX` ordering: High precedes Medium, Medium precedes Low, FIFO within tiers. | Step 6 pops 30 tasks via `DequeueTrackingBroker`. Verified: High tasks popped first, Medium second, Low last. Within each tier, index 0 to 9 popped in exact chronological order. | **SATISFIED** |
| **D6: Broker Outage Recovery** | Real OS TCP transport interruption & recovery via socket proxy; threads survive without restart; 0 event loss. | Step 7 boots `RealSocketTCPProxy`, routes Redis traffic through proxy port, severs socket for 3.11s during 10-task workload, worker exponential backoff activated, socket restored, 10/10 tasks succeed, durable event loss = 0. | **SATISFIED** |
| **D7: Stale Lease Fencing** | PostgreSQL conditional write rejection of stale worker lease token; authoritative winner commits. | Step 8 simulates Worker A lease expiration, supervisor lease recovery, Worker B replacement lease acquisition. Worker A stale completion rejected (`stale_commit_rejected = True`). Worker B commits authoritative result (`winner = "Worker B"`). | **SATISFIED** |
| **D8: Duplicate Suppression** | Terminal duplicate message suppressed without side effects; authoritative state untouched. | Step 9 enqueues duplicate message for completed fencing task. Worker checks PostgreSQL status (`SUCCESS`), suppresses handler, and ACKs message as `TERMINAL_DUPLICATE`. Return value preserved. | **SATISFIED** |
| **D9: Identity Reconciliation** | Full identity set equality ($T = R = E$) and 0 durable outbox event loss. | Step 10 audits all 41 demo tasks. Set equality confirmed: $\text{set}(T_{\text{success}}) == \text{set}(R_{\text{persisted}}) == \text{set}(E_{\text{published}})$. Zero durable events lost. | **SATISFIED** |
| **D10: Terminal Completion** | 100% terminal completion across all seeded tasks. | Step 10 audits terminal states: 41/41 tasks in `SUCCESS` (100.0%), 0 `FAILED`, 0 `PENDING`. Redis ready depth = 0, Redis in-flight depth = 0. | **SATISFIED** |
| **D11: Safe Teardown** | Safe, ownership-checked teardown of disposable database and Redis keys. | Step 11 checks DB name against `taskforge_demo_<run_id>`, forces drop of disposable DB, and purges Redis keys matching prefix `taskforge:demo:<run_id>:*`. Preserves original test/dev env. | **SATISFIED** |
| **D12: Output Schema** | Human-readable terminal output and structured JSON summary artifact. | Terminal output displays colorized progress banners and audit metrics. Generates schema-compliant JSON artifact [`benchmarks/public/demo_execution_summary.json`](file:///e:/TaskForge/benchmarks/public/demo_execution_summary.json). | **SATISFIED** |

---

## 3. Detailed Verification & Empirical Evidence

### 3.1 Pre-Flight Environment Validation (`--check`)

Invocation of `python scripts/demo.py --check` produces clean, colorized terminal verification and exits with code 0:

```text
================================================================================
  TASKFORGE DEMO: PRE-FLIGHT ENVIRONMENT VALIDATION
================================================================================
  [PASS] Python Runtime (>= 3.12)       Detected Python 3.12.10
  [PASS] PostgreSQL Database (localhost:5432) Connected (PostgreSQL 16.2)
  [PASS] Redis Message Broker (localhost:6379) Connected (Redis v7.4.12)
  [PASS] Docker Container Engine        Docker CLI available
  [PASS] Alembic Migrations Configuration alembic.ini and migrations directory found
================================================================================
```

### 3.2 End-to-End Demonstration Execution Log

Execution of `python scripts/demo.py` performs the complete multi-stage workflow in under 12 seconds:

```text
================================================================================
  TASKFORGE ARCHITECTURE DEMONSTRATION & RELIABILITY VERIFICATION HARNESS
================================================================================
  Run ID:              a25eea5b
  Disposable Database: taskforge_demo_a25eea5b
  Redis Namespace:     taskforge:demo:a25eea5b:
  Authorized Base:     a89858b73f8488e05cbcc2393a61f3080ffc7a38
  Frozen Baseline:     v0.8.0-phase8-freeze (da27047f56fe2eec75acdb8502f505542cf61202)
================================================================================

[STEP 1] Validating Environment Prerequisites (D1)...
  ... All 5 pre-flight checks passed ...

[STEP 2] Provisioning Isolated Database 'taskforge_demo_a25eea5b' (D2)...
  [OK] Created database 'taskforge_demo_a25eea5b'

[STEP 3] Applying Alembic Migrations to Disposable Schema (D3)...
  [OK] Database schema migrated to HEAD successfully.

[STEP 4] Seeding Deterministic Multi-Priority Workload (D4)...
  [OK] Seeded 30 tasks (10 Low, 10 Normal, 10 High) in reverse arrival order.

[STEP 5] Publishing Outbox Events to Redis Sorted Set Queue...
  [OK] All 30 events published. Redis ready queue depth = 30.

[STEP 6] Verifying Dequeue Priority Ordering under ZPOPMAX (D5)...
  [OK] High Priority (prio=3) popped before Normal (prio=2): True
  [OK] Normal Priority (prio=2) popped before Low (prio=1): True
  [OK] Strict Intra-Tier FIFO Arrival Ordering Preserved: True

[STEP 7] Executing Real OS TCP Transport Interruption & Recovery Drill (D6)...
  TCP Proxy listening on localhost:49321 -> forwarding to localhost:6379
  [FAULTS] Injected raw TCP socket reset. Severing broker traffic for 3.0s...
  [RECOVERY] TCP Proxy restored. Restoring broker traffic...
  [OK] Outage resolved. All 10 tasks completed via proxy recovery.
  Durable event loss = 0. Worker threads survived without process restart.

[STEP 8] Executing PostgreSQL Stale Lease-Token Fencing Drill (D7)...
  [OK] Worker A stale write rejected by PostgreSQL conditional update!
  [OK] Authoritative Worker B committed terminal SUCCESS with payload {'winner': 'Worker B'}

[STEP 9] Executing Terminal Duplicate Message Suppression Drill (D8)...
  [OK] Redelivered duplicate message suppressed as TERMINAL_DUPLICATE. Authoritative state untouched.

[STEP 10] Auditing Identity Reconciliation & 100% Terminal Completion (D9, D10)...
  Identity Reconciliation Audit:
    - Tasks Seeded:             41
    - Tasks Reached SUCCESS:    41 / 41 (True)
    - Task Results Persisted:   41 / 41 (True)
    - Outbox Events PUBLISHED:  41 / 41 (True)
    - Durable Event Loss:       0 (Strictly zero)
    - Final Redis Ready Depth:  0 (Fully drained)
    - Final Redis In-Flight:    0 (All ACKed)

[STEP 11] Performing Ownership-Checked Teardown (D11)...
  [OK] Dropped disposable database 'taskforge_demo_a25eea5b'
  [OK] Purged 14 isolated Redis keys matching 'taskforge:demo:a25eea5b:*'

[DEMO COMPLETE] Machine-readable summary written to:
  E:\TaskForge\benchmarks\public\demo_execution_summary.json
```

### 3.3 Machine-Readable Execution Summary (`demo_execution_summary.json`)

```json
{
  "run_id": "a25eea5b",
  "timestamp_utc": "2026-11-10T22:47:13.679154+00:00",
  "provenance": {
    "authorized_base_commit": "a89858b73f8488e05cbcc2393a61f3080ffc7a38",
    "frozen_tag": "v0.8.0-phase8-freeze (da27047f56fe2eec75acdb8502f505542cf61202)",
    "git_commit": "3eb1d51"
  },
  "disposable_resources": {
    "database_name": "taskforge_demo_a25eea5b",
    "redis_prefix": "taskforge:demo:a25eea5b:"
  },
  "acceptance_criteria": {
    "D1_environment_validation": true,
    "D2_isolated_resources": true,
    "D3_automatic_migrations": true,
    "D4_deterministic_workload": {
      "total_tasks": 30,
      "high_count": 10,
      "normal_count": 10,
      "low_count": 10
    },
    "D5_priority_ordering": {
      "high_before_normal": true,
      "normal_before_low": true,
      "fifo_within_tiers": true,
      "dequeue_order_sample": [
        "demo-a25eea5b-high-00",
        "demo-a25eea5b-high-01",
        "demo-a25eea5b-high-02",
        "demo-a25eea5b-high-03",
        "demo-a25eea5b-high-04",
        "demo-a25eea5b-high-05"
      ]
    },
    "D6_broker_outage_recovery": {
      "outage_duration_seconds": 3.11,
      "tasks_completed_success": 10,
      "durable_event_loss": 0,
      "threads_survived_without_restart": true
    },
    "D7_stale_lease_fencing": {
      "stale_commit_rejected": true,
      "authoritative_winner": "Worker B",
      "state_corrupted": false
    },
    "D8_terminal_duplicate_suppression": {
      "duplicate_message_acked": true,
      "authoritative_result_preserved": true
    },
    "D9_identity_reconciliation": {
      "total_demo_tasks": 41,
      "set_equality_tasks": true,
      "set_equality_results": true,
      "set_equality_events": true,
      "durable_outbox_event_loss": 0
    },
    "D10_terminal_completion": {
      "success_count": 41,
      "failed_count": 0,
      "pending_count": 0,
      "completion_percentage": 100.0
    },
    "D11_safe_teardown": true
  },
  "exit_code": 0,
  "status": "SUCCESS"
}
```

---

## 4. Automated Verification & Regression Audit

### 4.1 Demo Harness Test Suite (`tests/integration/test_g95_demo_reproducibility.py`)

A dedicated integration test module validates all aspects of the demonstration harness:
- `test_g95_demo_preflight_checks`: Direct unit validation of environment prerequisites.
- `test_g95_demo_cli_check_mode`: External process invocation of `python scripts/demo.py --check`.
- `test_g95_demo_full_execution`: External process invocation of full demo with custom summary path, verifying 100% completion, JSON emission, and all D1–D12 assertions.

```text
============================= test session starts =============================
platform win32 -- Python 3.12.10, pytest-8.4.2, pluggy-1.7.0
rootdir: E:\TaskForge
configfile: pyproject.toml
plugins: anyio-4.16.0, cov-6.3.0
collected 3 items

tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_preflight_checks PASSED [ 33%]
tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_cli_check_mode PASSED [ 66%]
tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_full_execution PASSED [100%]

============================= 3 passed in 10.78s ==============================
```

### 4.2 Full Regression Suite Audit

Following the implementation of `scripts/demo.py` and `test_g95_demo_reproducibility.py`, the entire 257-test project test suite was executed:

```text
============================= test session starts =============================
platform win32 -- Python 3.12.10, pytest-8.4.2, pluggy-1.7.0
rootdir: E:\TaskForge
configfile: pyproject.toml
plugins: anyio-4.16.0, cov-6.3.0
collected 257 items

........................................................................ [ 28%]
........................................................................ [ 56%]
........................................................................ [ 84%]
.........................................                                [100%]
============================= 257 passed in 370.37s ============================
```

**Results:**
- Total Tests: **257**
- Passed: **257 (100%)**
- Failed: **0**
- Errors: **0**
- Skipped: **0**

---

## 5. Architectural Invariants Confirmed

1. **Strict Production Lineage:**
   `scripts/demo.py` operates directly against core production modules:
   - `taskforge.queue.broker.RedisBroker` (`src/taskforge/queue/broker.py`)
   - `taskforge.outbox.publisher.OutboxPublisherWorker` (`src/taskforge/outbox/publisher.py`)
   - `taskforge.worker.dispatcher.TaskDispatcherWorker` (`src/taskforge/worker/dispatcher.py`)
   - `taskforge.repositories.task.TaskRepository` (`src/taskforge/repositories/task.py`)
   - Zero modifications were made to `src/` production code.
2. **PostgreSQL as Sole Authoritative Authority:**
   All lease validations, state transitions, and idempotency checks occur within PostgreSQL ACID transactions with conditional `WHERE lease_token = :token` fencing. Redis serves exclusively as a best-effort, low-latency transit broker.
3. **Zero Metric Contamination:**
   The demonstration script defines zero net-new Prometheus metrics, preserving the canonical Option A metric cardinality contract ($25 \le 25$ net-new G9.4 series, $28 \le 28$ combined).

---

## 6. Stage 2 Read-Ahead & Readiness

With Stage 1 verified and closed, TaskForge is fully prepared to enter **Stage 2: Public Benchmark Suite (`scripts/benchmarks/`)**:

### Stage 2 Deliverables:
1. **Benchmark Execution Harness (`scripts/benchmarks/run_benchmarks.py`):**
   - Implements the 3 canonical workloads defined in Stage 0:
     * `W1`: Pure ingestion rate (1,000 tasks, burst ingestion, transactional outbox).
     * `W2`: Burst-drain rate (5,000 tasks pre-enqueued, multi-worker drain).
     * `W3`: Saturated multi-worker concurrency with chaos (fencing, lease expiry, TCP proxy jitter).
2. **Validation Against Versioned Schema (`benchmarks/public/schema/v1.json`):**
   - Automatically validates benchmark output against the `v1.json` JSON schema.
3. **Artifact Generation:**
   - Produces machine-readable artifacts in `benchmarks/public/` adhering to the frozen metadata and measurement boundary definitions.

---

## 7. Stage 1 Closure & Authorization Request

Stage 1 is complete. All 12 criteria (D1–D12) are empirically verified and regression-tested.

We respectfully request formal review and acceptance of **Gate G9.5 Stage 1** and authorization to commence **Stage 2 (Public Benchmark Suite)**.
