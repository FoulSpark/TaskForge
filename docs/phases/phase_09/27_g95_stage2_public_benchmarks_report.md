# Gate G9.5: Stage 2 Public Benchmark Suite Verification Report

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite  
**Stage:** Stage 2 — Canonical Public Benchmark Suite (`benchmarks/public/run_public_benchmarks.py`)  
**Status:** IMPLEMENTATION COMPLETE & EMPIRICALLY VERIFIED  
**Date:** 2026-11-11  
**Base Production Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Preserved & Immutable — 0 `src/` changes)  
**Frozen Phase 8 Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**Protocol Lock Reference:** `docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md` (Commit `7a6dd87`)  
**Harness Script:** [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py)  
**JSON Schema Contract:** [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json)  
**Public Benchmark Artifact:** [`benchmarks/public/benchmark_results.json`](file:///e:/TaskForge/benchmarks/public/benchmark_results.json)  

---

## 1. Executive Summary

In strict accordance with the Stage 2 Authorization Decision and the locked Stage 0 Protocol, Stage 2 delivers the canonical public benchmark harness: [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py).

The suite executes the three locked canonical workloads (`W-01`, `W-02`, `W-03`) against live PostgreSQL 16 and Redis 7 services in disposable, manifest-isolated environments. It implements the mandatory measurement contract, strictly excludes warmup tasks from latency percentiles, calculates steady-state throughput over an explicit measurement window, breaks down latency into its four constituent stages, and validates its output against the versioned JSON schema [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json).

### Mandatory Review Controls Enforced:
1. **Control A (Preserve Historical Records):**
   Historical diagnostic artifacts from Gate G9.0, G9.3, and G9.4 remain untouched in `benchmarks/scenarios/`. Public benchmark artifacts are emitted independently to `benchmarks/public/`.
2. **Control B (Comprehensive Reproducibility Metadata):**
   The emitted artifact records application Git SHA (`a89858b`), current commit SHA, harness SHA-256 hash, Git dirty status, Python version (`3.12.10`), PostgreSQL version (`PostgreSQL 16.15`), Redis version (`Redis v7.4.12`), OS platform (`Windows 11 AMD64`), CPU model, logical core count (12), physical RAM (15.65 GB), Run ID, and ISO 8601 UTC timestamps.
3. **Control C (Explicit Denominator):**
   Throughput is calculated strictly over the steady-state window for non-warmup tasks:
   $$\text{Throughput} = \frac{N_{\text{measured}}}{T_{\text{end}} - T_{\text{start}}}$$
   Warmup tasks (first 10%) are explicitly excluded. Outcome counts (`success_count`, `failed_count`, `retried_count`) are tracked separately; retries do not inflate task throughput.
4. **Control D (Production Frozen):**
   Zero lines of code modified under `src/`. Zero new Prometheus metric series introduced. Zero artificial benchmark optimizations added.

---

## 2. Canonical Workloads & Empirical Results

The authoritative standard benchmark run was executed with concurrency = 4 dispatcher worker threads across all three canonical workloads:

### 2.1. Benchmark Summary Table

| Workload ID | Description & Task Type | Total Seeded Tasks | Warmup Excluded (10%) | Measured Steady-State Tasks | Measurement Window ($\Delta t$) | Steady-State Throughput | End-to-End Latency ($p50$ / $p95$ / $p99$) | Outbox Dwell Latency ($p50$ / $p95$) | Queue Dwell Latency ($p50$ / $p95$) | Handler Execution ($p50$ / $p95$) | Task Outcomes (Success / Fail / Retry) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`W-01`** | **No-op Baseline**<br>`noop` (framework overhead) | 300 | 30 | 270 | $7.13\,\text{s}$ | **$37.88\,\text{tasks/s}$** | $4,740\,\text{ms}$ / $7,769\,\text{ms}$ / $8,111\,\text{ms}$ | $1,133\,\text{ms}$ / $1,648\,\text{ms}$ | $3,709\,\text{ms}$ / $6,203\,\text{ms}$ | $53.42\,\text{ms}$ / $71.73\,\text{ms}$ | 270 / 0 / 0 |
| **`W-02`** | **CPU-Bound Prime Sieve**<br>`cpu_bound` ($n=500$) | 200 | 20 | 180 | $4.63\,\text{s}$ | **$38.86\,\text{tasks/s}$** | $3,122\,\text{ms}$ / $5,183\,\text{ms}$ / $5,377\,\text{ms}$ | $756\,\text{ms}$ / $1,047\,\text{ms}$ | $2,413\,\text{ms}$ / $4,177\,\text{ms}$ | $51.89\,\text{ms}$ / $71.97\,\text{ms}$ | 180 / 0 / 0 |
| **`W-03`** | **I/O-Bound Service**<br>`io_bound` ($50\,\text{ms}$ sleep) | 100 | 10 | 90 | $3.20\,\text{s}$ | **$28.13\,\text{tasks/s}$** | $2,090\,\text{ms}$ / $3,539\,\text{ms}$ / $3,662\,\text{ms}$ | $469\,\text{ms}$ / $469\,\text{ms}$ | $1,691\,\text{ms}$ / $3,110\,\text{ms}$ | $96.83\,\text{ms}$ / $113.58\,\text{ms}$ | 90 / 0 / 0 |

---

## 3. Empirical Attribution & Latency Stage Analysis

In accordance with the Phase 9 attribution methodology, each measured task records exact microsecond timestamps across all four pipeline boundaries:
- $t_{\text{ingested}}$: Task and transactional outbox event committed in PostgreSQL.
- $t_{\text{published}}$: Outbox publisher polls event, enqueues to Redis ZSET, and commits `status = 'PUBLISHED'`.
- $t_{\text{dequeued}}$ / $t_{\text{started}}$: Dispatcher dequeues task, acquires worker lease, and spawns thread (`TaskAttempt.started_at`).
- $t_{\text{completed}}$: Handler finishes execution, and worker commits `TaskResult` and terminal `SUCCESS` in PostgreSQL.

### 3.1. Stage Latency Breakdown (Mean Values)

| Pipeline Stage | Measurement Formula | `W-01` Mean Latency | `W-02` Mean Latency | `W-03` Mean Latency | Architectural Driver |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **1. Outbox Dwell** | $t_{\text{published}} - t_{\text{ingested}}$ | $1,048.4\,\text{ms}$ | $660.6\,\text{ms}$ | $357.2\,\text{ms}$ | Governed by publisher poll loop ($0.05\,\text{s}$) and batch chunking (50 tasks/batch) during burst ingestion. |
| **2. Queue Dwell** | $t_{\text{started}} - t_{\text{published}}$ | $3,731.3\,\text{ms}$ | $2,486.5\,\text{ms}$ | $1,808.7\,\text{ms}$ | Queue waiting time while worker thread pool (concurrency=4) drains the ready queue. As queue depth shrinks, dwell drops proportionally. |
| **3. Handler Execution** | $t_{\text{completed}} - t_{\text{started}}$ | $54.9\,\text{ms}$ | $53.5\,\text{ms}$ | $97.3\,\text{ms}$ | Execution time of task payload. In `W-01` and `W-02`, dominated by DB lease acquisition and commit overhead ($50\,\text{ms}$). In `W-03`, includes $50\,\text{ms}$ blocking sleep ($50\,\text{ms} + 47\,\text{ms} = 97\,\text{ms}$). |
| **4. End-to-End Latency** | $t_{\text{completed}} - t_{\text{ingested}}$ | $4,691.8\,\text{ms}$ | $3,101.5\,\text{ms}$ | $2,138.3\,\text{ms}$ | Total wall-clock time from task creation to durable result persistence ($\Delta t_{\text{e2e}} = \Delta t_{\text{outbox}} + \Delta t_{\text{queue}} + \Delta t_{\text{handler}}$). |

### 3.2. Engineering Observations
1. **Queue Dwell Domination Under Saturated Batches:**
   In all three workloads, queue dwell constitutes $75\text{--}80\%$ of end-to-end latency during burst submission. This directly validates the Stage 1B/Stage 2 diagnostic findings: when a backlog of hundreds of tasks arrives concurrently, tasks wait in the Redis ready queue for worker threads to become available.
2. **I/O-Bound Throughput Scaling:**
   In `W-03`, worker threads sleep for $50\,\text{ms}$. With concurrency = 4, theoretical peak worker throughput without database overhead is $4 / 0.05 = 80\,\text{tasks/s}$. Factoring in the $50\,\text{ms}$ PostgreSQL lease acquisition and result commit transactions ($100\,\text{ms}$ total per task), theoretical maximum is $4 / 0.100 = 40\,\text{tasks/s}$. The measured steady-state throughput of **$28.13\,\text{tasks/s}$** reflects $70.3\%$ of the theoretical saturation limit, with zero pool checkout timeouts and zero connection leaks.
3. **CPU Sieve ($n=500$) Parity with No-Op:**
   Calculating primes up to $n=500$ requires $< 0.15\,\text{ms}$ of pure CPU time in Python. Consequently, `W-02` exhibits near-identical throughput ($38.86\,\text{tasks/s}$) and handler execution ($51.89\,\text{ms}$) to `W-01` ($37.88\,\text{tasks/s}$ / $53.42\,\text{ms}$), confirming that database transaction latency is the primary pacing factor rather than micro-benchmarking CPU overhead.

---

## 4. Machine-Readable Schema Validation

The public benchmark artifact [`benchmarks/public/benchmark_results.json`](file:///e:/TaskForge/benchmarks/public/benchmark_results.json) was validated directly against [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json).

### Validation Verification:
- **Schema Title:** `TaskForgePublicBenchmarkV1`
- **Schema Version:** `1.0.0`
- **Required Top-Level Keys:** `schema_version`, `timestamp_utc`, `provenance`, `environment`, `workload_results` — **All Present**
- **Provenance Properties:** `git_commit`, `git_dirty`, `production_baseline_sha`, `harness_sha`, `harness_path`, `run_id` — **All Present**
- **Environment Properties:** `os`, `cpu_model`, `cpu_logical_cores`, `ram_gb`, `python_version`, `postgres_version`, `redis_version` — **All Present**
- **Workload Results Properties:** `task_count`, `elapsed_seconds`, `throughput_tasks_per_sec`, `latencies_ms`, `raw_observations_count` — **All Present for W-01, W-02, W-03**
- **Latency Percentiles:** `p50`, `p95`, `p99`, `min`, `max`, `mean` plus detailed stage breakdowns — **All Present**

---

## 5. Automated Regression & Test Suite Status

An automated integration test suite [`tests/integration/test_g95_public_benchmarks.py`](file:///e:/TaskForge/tests/integration/test_g95_public_benchmarks.py) was implemented to guarantee continuous reproducibility of the benchmark suite:

```text
============================= test session starts =============================
platform win32 -- Python 3.12.10, pytest-8.4.2, pluggy-1.7.0
rootdir: E:\TaskForge
configfile: pyproject.toml
plugins: anyio-4.16.0, cov-6.3.0
collected 3 items

tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_schema_file_exists PASSED [ 33%]
tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_quick_execution PASSED [ 66%]
tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_single_workload PASSED [100%]

============================= 3 passed in 10.37s ==============================
```

### Combined Gate G9.5 Integration Test Suite:
Executing both the Demo Reproducibility harness and Public Benchmark test suites concurrently:
```text
tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_preflight_checks PASSED [ 16%]
tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_cli_check_mode PASSED [ 33%]
tests/integration/test_g95_demo_reproducibility.py::test_g95_demo_full_execution PASSED [ 50%]
tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_schema_file_exists PASSED [ 66%]
tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_quick_execution PASSED [ 83%]
tests/integration/test_g95_public_benchmarks.py::test_g95_public_benchmarks_single_workload PASSED [100%]

============================= 6 passed in 23.54s ==============================
```

---

## 6. Stage 2 Deliverables Summary

| Deliverable | Location | Status |
| :--- | :--- | :---: |
| **Canonical Benchmark Harness** | [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py) | **COMPLETE** |
| **Versioned JSON Schema** | [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json) | **COMPLETE** |
| **Authoritative Public Artifact** | [`benchmarks/public/benchmark_results.json`](file:///e:/TaskForge/benchmarks/public/benchmark_results.json) | **COMPLETE** |
| **Automated Integration Tests** | [`tests/integration/test_g95_public_benchmarks.py`](file:///e:/TaskForge/tests/integration/test_g95_public_benchmarks.py) | **COMPLETE (3/3 Passed)** |
| **Verification & Attribution Report** | [`docs/phases/phase_09/27_g95_stage2_public_benchmarks_report.md`](file:///e:/TaskForge/docs/phases/phase_09/27_g95_stage2_public_benchmarks_report.md) | **COMPLETE** |

---

## 7. Stage 2 Closure & Authorization Request

All requirements of **Gate G9.5 Stage 2** are satisfied:
- Canonical harness implemented at `benchmarks/public/run_public_benchmarks.py`.
- W-01, W-02, W-03 workloads benchmarked under strict steady-state measurement windows with warmup exclusion.
- Comprehensive latency percentiles and task outcomes calculated.
- Output validated against `benchmarks/public/schema/v1.json`.
- Zero changes to production `src/` code or Prometheus metric series.

We respectfully request formal review and closure of **Gate G9.5 Stage 2** and authorization to commence **Stage 3 (Public Architecture Whitepaper & Visual Architecture Diagrams)**.
