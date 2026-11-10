# Gate G9.5 — Public Architecture Package: Stage 0 Protocol & Provenance Lock Report

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite (Stage 0: Protocol & Provenance Lock)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-11  
**Status:** Stage 0 Complete — Implementation Ready  
**Authorized Production Base Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Immutable & Preserved)  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**Active Working Branch:** `phase-9-hardening`  
**Telemetry Contract:** Option A Canonical Exposition (25 Net-New G9.4 Series, 28 Combined Series)  

---

## 1. Executive Summary & Objective

In accordance with the Formal Review Decision on Gate G9.5 Scoping, this Stage 0 report establishes the **Protocol and Provenance Lock** required before initiating implementation of the final Phase 9 deliverables (Stages 1 through 4).

This document addresses all five mandatory review findings (R1–R5):
1. **R1:** Resolves benchmark provenance discrepancies and formalizes a benchmark lineage table distinguishing historical diagnostic campaigns from clean-checkout public benchmarks.
2. **R2:** Reconciles the batch-dequeue trade-off analysis to reflect empirical boundaries at $c \le 10$ and $c=20$.
3. **R3:** Reconciles the Option A Prometheus metric exposition against the actual runtime contract.
4. **R4:** Specifies the reproducible local demonstration contract (`scripts/demo.py`) with isolated disposable environments and prerequisite pre-flight checks (`--check`).
5. **R5:** Freezes the public benchmark measurement protocol and JSON schema contract (`benchmarks/public/schema/v1.json`).

---

## 2. Finding R1 — Historical Benchmark Provenance & Lineage Table

To prevent historical diagnostic figures from being cited interchangeably or without context, this section establishes the formal provenance and experimental conditions of all referenced performance measurements.

### 2.1. Historical Benchmark Lineage Table
| Campaign / Source | Run ID / Artifact | Target Workload | Metric | Reported Value | Experimental Conditions | Harness & App Commit |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Stage 1B Diagnostics** | `benchmarks/scenarios/g93/stage1b_attribution.json` | **W-01** (Noop)<br>**W-02** (Prime)<br>**W-03** (I/O) | Eligible Outbox Lag ($p95$) | **$71.78\,\text{ms}$** (W-01)<br>**$99.85\,\text{ms}$** (W-02)<br>**$145.13\,\text{ms}$** (W-03) | Baseline production dispatcher (`a89858b`); concurrent workload ingestion; default $100\,\text{ms}$ publisher poll cadence; 500 tasks per workload. | App: `a89858b`<br>Harness: `036eb53` |
| **G9.4.1 Campaign A** | `benchmarks/scenarios/g94/g941_publisher_diagnostic_report.json` | **W-01** (Noop)<br>**W-02** (Prime)<br>**W-03** (I/O) | Eligible Outbox Lag ($p95$) | **$190.50\,\text{ms}$** (W-01)<br>**$100.52\,\text{ms}$** (W-02)<br>**$193.66\,\text{ms}$** (W-03) | Quiescent steady-state background profiling; publisher cycle variations under isolated test database; 3 independent trials per workload. | App: `3fee428`<br>Harness: `3fee428` |
| **G9.4.1 Campaign B** | `benchmarks/scenarios/g94/g941_publisher_diagnostic_report.json` | 1,000-Task Burst Backlog | Publisher Drain Rate & Duration | **$276.10\,\text{tasks/s}$**<br>**$3.622\,\text{s}$** mean duration | 1,000 tasks pre-inserted while publisher paused; quiescent arrival; 20 batches of 50 tasks; active SQL/Redis time $42.25\%$, sleep $57.75\%$. Exact Redis ZSET membership verified. | App: `3fee428`<br>Harness: `2ea407c` |

### 2.2. Public Documentation Policy:
- All figures in Section 2.1 are designated **Historical Reference Measurements**.
- The public whitepaper and architecture package will **not** present historical figures as universal performance constants.
- The Public Benchmark Harness (Stage 2) will generate **Fresh Clean-Checkout Measurements** accompanied by hardware specifications, CPU topology, git SHA, and dirty-working-tree status.

---

## 3. Finding R2 — Corrected Batch-Dequeue Trade-off Analysis

The architecture whitepaper will accurately characterize the experimental findings of Stage 2 Phase 2C rather than asserting universal claims:

### Corrected Architectural Trade-off Statement:
1. **Operating Envelope Sufficiency ($c \le 10$):**  
   For the standard production operating envelope ($c=4$ to $c=10$), sequential single-item priority dequeue (`ZPOPMIN` via Lua) demonstrated minimal queue starvation ($< 6\%$) and lower tail-latency variance ($p99$ jitter) compared to batch dequeue.
2. **Serialization Onset ($c=20$):**  
   Under heavy concurrency ($c=20$), lock and command serialization onset was observed on sequential dequeue, where batch dequeue began to show aggregate throughput benefits.
3. **Design Choice Justification:**  
   Because TaskForge is targeted for the $c \le 10$ worker envelope, batch dequeue was **not adopted for production** because:
   - It introduced priority-inversion risk within batches.
   - It increased latency variance for high-priority tasks arriving behind a batch.
   - Single-item dequeue met all SLA targets with lower implementation complexity.

---

## 4. Finding R3 — Canonical Option A Metric Contract Reconciliation

A direct runtime exposition audit of `get_metrics_content()` confirms the exact canonical series structure under the adopted **Option A** contract:

```
Runtime Metric Exposition (Fresh Python Verification)
─────────────────────────────────────────────────────────────────────────────
taskforge_lease_renewals_total{status="success"} 0.0
taskforge_lease_renewals_total{status="failed"} 0.0
taskforge_lease_renewals_created{status="success"} 1.794349883e+09
taskforge_lease_renewals_created{status="failed"} 1.794349883e+09
taskforge_fencing_rejections_total 0.0
taskforge_fencing_rejections_created 1.794349883e+09
─────────────────────────────────────────────────────────────────────────────
```

### 4.1. Reconciliation of the Synthesis Transcription Discrepancy
In `docs/phases/phase_09/22_g94_operational_resilience_and_reliability_synthesis.md`, a transcription shorthand reported "Lease renewals: 3 series; Fencing rejections: 3 series". 

The **canonical, implemented, and accepted contract** in `src/taskforge/monitoring/metrics.py` is:
- **`taskforge_lease_renewals_total`:** Labeled by `status` (`success`, `failed`). Emits **4 runtime series** (2 value series + 2 Prometheus client `_created` timestamp gauges).
- **`taskforge_fencing_rejections_total`:** Unlabeled global counter. Emits **2 runtime series** (1 value series + 1 Prometheus client `_created` timestamp gauge).
- **Subtotal Lease & Fencing:** Exactly **$4 + 2 = 6\text{ runtime series}$**.

### 4.2. Master Cardinality Allocation Table
| Sub-System | Metric Name | Metric Type | Labels | Runtime Series Count |
| :--- | :--- | :---: | :--- | :---: |
| **Legacy Pool (G9.2)** | `taskforge_db_pool_connections` | Gauge | `state="active\|overflow\|idle"` | **3** |
| **G9.4.2 Pool Additions** | `taskforge_db_pool_checkout_duration_seconds` | Histogram | 11 buckets + sum + count | **13** |
| | `taskforge_db_pool_checkout_timeouts_total` | Counter | `pool_name="primary"` (+ `_created`) | **2** |
| | `taskforge_db_pool_active_connections` | Gauge | `pool_name="primary"` | **2** |
| | `taskforge_db_pool_overflow_connections` | Gauge | `pool_name="primary"` | **2** |
| **G9.4.3 Lease & Fencing** | `taskforge_lease_renewals_total` | Counter | `status="success\|failed"` (+ 2 `_created`) | **4** |
| | `taskforge_fencing_rejections_total` | Counter | None (+ 1 `_created`) | **2** |
| **Total Net-New G9.4** | — | — | — | **25 (Ceiling: 25)** |
| **Total Combined Exposition** | — | — | — | **28 (Ceiling: 28)** |

The 25 net-new and 28 combined series limits are preserved and locked.

---

## 5. Finding R4 — Reproducible Local Demo Contract (`scripts/demo.py`)

The public demo script must run safely and deterministically without touching normal development databases or production services.

### 5.1. Execution Modes
1. **Pre-flight Environment Validation:**
   ```bash
   python scripts/demo.py --check
   ```
   Checks PostgreSQL 16 on `localhost:5432`, Redis 7 on `localhost:6379`, Python $\ge 3.12$, and required libraries. Emits clear exit codes (0 = ready, 1 = missing dependency) with remediation instructions.
2. **Interactive Demonstration:**
   ```bash
   python scripts/demo.py
   ```
   Runs pre-flight checks, provisions disposable resources, executes workloads and fault injection, and cleans up.

### 5.2. Safety & Isolation Invariants
- **Disposable Database:** All demo operations target `taskforge_demo` (created at start, dropped at finish). The development database `taskforge` and test databases are strictly untouched.
- **Isolated Redis Namespace:** All broker keys use prefix `taskforge:demo:<run_id>:*` (flushed at teardown).
- **Deterministic Workload:** Seeds 30 tasks with explicit priorities:
  - 10 High Priority
  - 10 Normal Priority
  - 10 Low Priority
- **Live Fault Injection (Dual Scenario):**
  1. *Transient Broker Disconnect:* 3-second transport severance during dispatch; publisher and dispatcher apply exponential backoff and recover with 0 event loss.
  2. *Authoritative Lease Fencing Drill:* Injects duplicate message delivery against an already terminal task; demonstrates suppression as `TERMINAL_DUPLICATE` without state corruption.
- **Terminal Presentation:** ASCII status progress bars, real-time stage logs, and identity-level reconciliation summary table.

---

## 6. Finding R5 — Public Benchmark Protocol & Schema Contract

To enable external reproduction, the public benchmark runner (`benchmarks/public/run_public_benchmarks.py`) adheres to a versioned contract:

### 6.1. Measurement Protocol Rules
1. **Workloads:**
   - **W-01 (Noop):** Baseline latency and dispatch overhead (500 tasks).
   - **W-02 (CPU Prime Sieve):** CPU-bound computational workload ($N=50{,}000$, 200 tasks).
   - **W-03 (I/O Simulation):** I/O dwell simulation ($50\,\text{ms}$ sleep, 200 tasks).
2. **Measurement Boundaries:**
   - $\Delta t_{\text{outbox}} = t_{\text{broker\_enqueued}} - t_{\text{outbox\_available}}$ (Outbox dwell lag)
   - $\Delta t_{\text{queue}} = t_{\text{worker\_dequeued}} - t_{\text{broker\_enqueued}}$ (Broker queue dwell)
   - $\Delta t_{\text{exec}} = t_{\text{task\_completed}} - t_{\text{worker\_dequeued}}$ (Handler execution time)
   - $\Delta t_{\text{e2e}} = t_{\text{result\_committed}} - t_{\text{task\_ingested}}$ (End-to-end user-perceived latency)
3. **Throughput Calculation:**
   $$R = \frac{N_{\text{completed}}}{\Delta t_{\text{wall\_elapsed}}}$$
4. **Treatment of Tasks:** All tasks (success, failure, retry) are retained. No task is discarded.
5. **Percentile Calculation:** Standard linear interpolation method (numpy percentile equivalent).

### 6.2. JSON Schema Specification (`benchmarks/public/schema/v1.json`)
```json
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "TaskForgePublicBenchmarkV1",
  "type": "object",
  "required": [
    "schema_version",
    "timestamp_utc",
    "provenance",
    "environment",
    "workload_results"
  ],
  "properties": {
    "schema_version": { "type": "string", "enum": ["1.0.0"] },
    "timestamp_utc": { "type": "string", "format": "date-time" },
    "provenance": {
      "type": "object",
      "required": ["git_commit", "git_dirty", "production_baseline_sha"],
      "properties": {
        "git_commit": { "type": "string" },
        "git_dirty": { "type": "boolean" },
        "production_baseline_sha": { "type": "string" }
      }
    },
    "environment": {
      "type": "object",
      "required": ["os", "cpu_model", "cpu_logical_cores", "ram_gb", "python_version", "postgres_version", "redis_version"],
      "properties": {
        "os": { "type": "string" },
        "cpu_model": { "type": "string" },
        "cpu_logical_cores": { "type": "integer" },
        "ram_gb": { "type": "number" },
        "python_version": { "type": "string" },
        "postgres_version": { "type": "string" },
        "redis_version": { "type": "string" }
      }
    },
    "workload_results": {
      "type": "object",
      "additionalProperties": {
        "type": "object",
        "required": ["task_count", "elapsed_seconds", "throughput_tasks_per_sec", "latencies_ms", "raw_observations_count"],
        "properties": {
          "task_count": { "type": "integer" },
          "elapsed_seconds": { "type": "number" },
          "throughput_tasks_per_sec": { "type": "number" },
          "latencies_ms": {
            "type": "object",
            "required": ["p50", "p95", "p99", "min", "max", "mean"],
            "properties": {
              "p50": { "type": "number" },
              "p95": { "type": "number" },
              "p99": { "type": "number" },
              "min": { "type": "number" },
              "max": { "type": "number" },
              "mean": { "type": "number" }
            }
          },
          "raw_observations_count": { "type": "integer" }
        }
      }
    }
  }
}
```

---

## 7. Canonical Delivery Guarantees & Invariant Framework

### 7.1. Canonical Guarantee Statement
The following statement is formally adopted as the platform's core delivery guarantee in all public documentation:
> *"TaskForge provides durable task submission through PostgreSQL transactions and a transactional outbox. Queue delivery is at-least-once. Worker ownership is protected by leases and fencing tokens. Task completion is persisted transactionally, and duplicate delivery is handled through idempotent execution and authoritative task state."*

### 7.2. Invariant Proof Framework
| Invariant ID | Invariant Statement | Enforcement Mechanism | Failure Scenario Guarded | Test / Verification Evidence |
| :--- | :--- | :--- | :--- | :--- |
| **INV-1** | A stale worker cannot commit an authoritative task transition using an obsolete lease token. | PostgreSQL conditional atomic update: `WHERE id = :id AND lease_token = :token`. | Worker network stall or GC pause exceeding 30s lease heartbeat. | `test_g943_fencing_rejections` (all 5 guarded paths verified). |
| **INV-2** | PostgreSQL remains the authoritative source of truth when Redis is degraded or unavailable. | Task state, attempts, and results committed strictly to PostgreSQL tables before broker ACK. | Redis disconnect, crash, or memory exhaustion. | `test_g944_broker_outage_recovery` (5.71s real OS transport failure). |
| **INV-3** | An outbox event remains durably recoverable until publication is acknowledged and committed. | Two-phase outbox commit (`status='PENDING' -> 'PUBLISHED'`) with incremented retry count. | Redis connection drops during outbox sweep. | 50-task identity reconciliation drill ($E_{\text{initial}} = E_{\text{final}}$). |
| **INV-4** | Duplicate message redelivery cannot overwrite or corrupt an existing terminal task state. | Status transition validation: terminal states (`SUCCESS`, `FAILED`) reject non-initial transitions and return `TERMINAL_DUPLICATE`. | Worker crashes after DB commit but before Redis broker ACK. | `test_g944_scenario4_duplicate_delivery` & duplicate benchmark drill. |

---

## 8. Staged Implementation Authorization & Boundary Lock

With this Stage 0 report, the protocol, measurement boundaries, and metric reconciliation are formally locked:

- **Production Source Code:** Strictly frozen at `a89858b`.
- **Phase 8 Tag:** `v0.8.0-phase8-freeze` (`da27047`) preserved.
- **Telemetry Budget:** Option A strictly maintained (28 combined / 25 net-new series).
- **Stage 1 (Demo Implementation):** Ready to proceed upon submission of this Stage 0 report.
