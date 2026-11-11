# Gate G9.5: Stage 2 Public Benchmark Suite Verification Report

**Milestone:** Gate G9.5 — Public Architecture Package & Reproducible Verification Suite  
**Stage:** Stage 2 — Canonical Public Benchmark Suite (`benchmarks/public/run_public_benchmarks.py`)  
**Status:** IMPLEMENTATION COMPLETE, EMPIRICALLY RECONCILED & CLEAN PROVENANCE LOCKED  
**Date:** 2026-11-11  
**Base Production Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Preserved & Immutable — 0 `src/` changes)  
**Frozen Phase 8 Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`, Preserved)  
**Harness Commit SHA:** `1d4a76cd047a61548e92e57cfb1f032ba7914068` (`git_dirty = false`)  
**Protocol Lock Reference:** `docs/phases/phase_09/23_g95_stage0_protocol_and_provenance_lock.md` (Commit `7a6dd87`)  
**Reconciliation Reference:** `docs/phases/phase_09/28_g95_stage2_reconciliation_addendum.md`  
**Harness Script:** [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py)  
**JSON Schema Contract:** [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json)  
**Public Benchmark Artifact:** [`benchmarks/public/benchmark_results.json`](file:///e:/TaskForge/benchmarks/public/benchmark_results.json)  

---

## 1. Executive Summary

In strict accordance with the Stage 2 Authorization Decision and the locked Stage 0 Protocol, Stage 2 delivers the canonical public benchmark harness: [`benchmarks/public/run_public_benchmarks.py`](file:///e:/TaskForge/benchmarks/public/run_public_benchmarks.py).

The suite executes the three locked canonical workloads (`W-01`, `W-02`, `W-03`) against live PostgreSQL 16 and Redis 7 services in disposable, manifest-isolated environments. It implements the mandatory measurement contract, strictly excludes warmup tasks from latency percentiles, calculates steady-state throughput over an explicit measurement window, breaks down latency into its four constituent stages with exact mathematical identity reconciliation ($L_{\text{outbox}} + L_{\text{queue}} + L_{\text{handler}} \equiv L_{\text{e2e}}$), and validates its output against the versioned JSON schema [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json).

### Mandatory Review Controls Enforced:
1. **Control A (Preserve Historical Records):**
   Historical diagnostic artifacts from Gate G9.0, G9.3, and G9.4 remain untouched in `benchmarks/scenarios/`. Public benchmark artifacts are emitted independently to `benchmarks/public/`.
2. **Control B (Clean Git Provenance & Reproducibility Metadata):**
   The emitted artifact records application Git SHA (`a89858b`), harness commit SHA (`1d4a76c`), harness SHA-256 hash (`6576de5ba6...`), clean Git dirty status (`git_dirty = false`), Python version (`3.12.10`), PostgreSQL version (`PostgreSQL 16.15`), Redis version (`Redis v7.4.12`), OS platform (`Windows 11 AMD64`), CPU model, logical core count (12), physical RAM (15.65 GB), Run ID (`fc557f14`), and ISO 8601 UTC timestamps.
3. **Control C (Explicit Denominator & Distinct Outcome Accounting):**
   Throughput is calculated strictly over the steady-state window for non-warmup tasks:
   $$\text{Throughput} = \frac{N_{\text{measured}}}{T_{\text{end}} - T_{\text{start}}}$$
   Warmup tasks (first 10%) are excluded from latency/throughput calculations. Outcome counters are separately and unambiguously recorded: `seeded_count`, `warmup_count`, `measured_count`, `success_count`, `failed_count`, `retried_count`, `measured_success_count`, and `measured_failed_count`.
4. **Control D (Production Frozen):**
   Zero lines of code modified under `src/`. Zero new Prometheus metric series introduced. Zero artificial benchmark optimizations added.

---

## 2. Canonical Workloads & Empirical Results

The authoritative standard benchmark run (`Run ID: fc557f14`) was executed with concurrency = 4 dispatcher worker threads across all three canonical workloads against clean checkout commit `1d4a76c`:

### 2.1. Benchmark Summary Table

| Workload ID | Description & Task Type | Seeded Tasks | Warmup Excl. | Measured Tasks | Measurement Window ($\Delta t$) | Steady-State Throughput | End-to-End Latency ($p50$ / $p95$ / $p99$) | Outbox Dwell Latency ($p50$ / $p95$) | Queue Dwell Latency ($p50$ / $p95$) | Handler Execution ($p50$ / $p95$) | Run Outcomes (Total Success / Fail / Retry) | Measured Outcomes (Success / Fail) |
| :--- | :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`W-01`** | **No-op Baseline**<br>`noop` (framework overhead) | 300 | 30 | 270 | $6.03\,\text{s}$ | **$44.78\,\text{tasks/s}$** | $4,064\,\text{ms}$ / $6,757\,\text{ms}$ / $7,003\,\text{ms}$ | $1,012\,\text{ms}$ / $1,507\,\text{ms}$ | $2,997\,\text{ms}$ / $5,198\,\text{ms}$ | $51.87\,\text{ms}$ / $61.47\,\text{ms}$ | 300 / 0 / 0 | 270 / 0 |
| **`W-02`** | **CPU-Bound Prime Sieve**<br>`cpu_bound` ($n=500$) | 200 | 20 | 180 | $4.04\,\text{s}$ | **$44.59\,\text{tasks/s}$** | $2,843\,\text{ms}$ / $4,608\,\text{ms}$ / $4,743\,\text{ms}$ | $672\,\text{ms}$ / $965\,\text{ms}$ | $2,129\,\text{ms}$ / $3,586\,\text{ms}$ | $52.90\,\text{ms}$ / $67.53\,\text{ms}$ | 200 / 0 / 0 | 180 / 0 |
| **`W-03`** | **I/O-Bound Service**<br>`io_bound` ($50\,\text{ms}$ sleep) | 100 | 10 | 90 | $3.12\,\text{s}$ | **$28.82\,\text{tasks/s}$** | $2,309\,\text{ms}$ / $3,556\,\text{ms}$ / $3,675\,\text{ms}$ | $431\,\text{ms}$ / $431\,\text{ms}$ | $1,901\,\text{ms}$ / $3,030\,\text{ms}$ | $98.77\,\text{ms}$ / $132.65\,\text{ms}$ | 100 / 0 / 0 | 90 / 0 |

---

## 3. Latency Decomposition & Arithmetic Identity Reconciliation

### 3.1. Mathematical Invariant Formulation

For each task $i$, the end-to-end processing pipeline traverses four causally sequential timestamps:
1. $t_{\text{ingested}}^{(i)}$: Task creation and transactional outbox event insertion.
2. $t_{\text{published}}^{(i)}$: Outbox publisher polls event and commits published state.
3. $t_{\text{started}}^{(i)}$: Dispatcher thread acquires worker lease and starts execution (`TaskAttempt.started_at`).
4. $t_{\text{completed}}^{(i)}$: Handler completes payload execution, and worker commits `TaskResult` and terminal `SUCCESS`.

The stage latencies are defined as:
$$L_{\text{outbox}}^{(i)} = t_{\text{published}}^{(i)} - t_{\text{ingested}}^{(i)}$$
$$L_{\text{queue}}^{(i)} = t_{\text{started}}^{(i)} - t_{\text{published}}^{(i)}$$
$$L_{\text{handler}}^{(i)} = t_{\text{completed}}^{(i)} - t_{\text{started}}^{(i)}$$
$$L_{\text{e2e}}^{(i)} = t_{\text{completed}}^{(i)} - t_{\text{ingested}}^{(i)}$$

By algebraic identity:
$$L_{\text{outbox}}^{(i)} + L_{\text{queue}}^{(i)} + L_{\text{handler}}^{(i)} = (t_{\text{published}}^{(i)} - t_{\text{ingested}}^{(i)}) + (t_{\text{started}}^{(i)} - t_{\text{published}}^{(i)}) + (t_{\text{completed}}^{(i)} - t_{\text{started}}^{(i)}) \equiv t_{\text{completed}}^{(i)} - t_{\text{ingested}}^{(i)} = L_{\text{e2e}}^{(i)}$$

By linearity of expectation, the arithmetic mean of end-to-end latency equals the sum of the arithmetic means of its constituent stages:
$$\overline{L}_{\text{e2e}} = \frac{1}{N} \sum_{i=1}^N L_{\text{e2e}}^{(i)} = \frac{1}{N} \sum_{i=1}^N \left( L_{\text{outbox}}^{(i)} + L_{\text{queue}}^{(i)} + L_{\text{handler}}^{(i)} \right) = \overline{L}_{\text{outbox}} + \overline{L}_{\text{queue}} + \overline{L}_{\text{handler}}$$

### 3.2. Empirical Reconciliation Table

| Workload ID | $\overline{L}_{\text{outbox}}$ (ms) | $\overline{L}_{\text{queue}}$ (ms) | $\overline{L}_{\text{handler}}$ (ms) | Sum of Means $\sum \overline{L}_i$ (ms) | Reported E2E Mean $\overline{L}_{\text{e2e}}$ (ms) | Mean $\Delta$ (ms) | Max Per-Task $\Delta$ (ms) | Reconciled Status |
| :--- | :---: | :---: | :---: | :---: | :---: | :---: | :---: | :---: |
| **`W-01`** | $955.318$ | $3,084.507$ | $52.310$ | **$4,092.135$** | **$4,092.136$** | **$0.001$** | **$0.000000$** | **RECONCILED (TRUE)** |
| **`W-02`** | $599.911$ | $2,173.850$ | $53.526$ | **$2,827.287$** | **$2,827.287$** | **$0.000$** | **$0.000000$** | **RECONCILED (TRUE)** |
| **`W-03`** | $316.172$ | $1,876.566$ | $103.291$ | **$2,296.029$** | **$2,296.029$** | **$0.000$** | **$0.000000$** | **RECONCILED (TRUE)** |

*Note: The $0.001\,\text{ms}$ mean discrepancy in `W-01` arises solely from independent 3-decimal floating-point rounding of each reported component ($955.318 + 3084.507 + 52.310 = 4092.135$ vs $4092.1356\dots \approx 4092.136$). The raw underlying unrounded floating-point discrepancy is $0.000000\,\text{ms}$.*

---

## 4. Machine-Readable Schema Validation & Automated Testing

The benchmark artifact [`benchmarks/public/benchmark_results.json`](file:///e:/TaskForge/benchmarks/public/benchmark_results.json) is programmatically validated on every invocation against [`benchmarks/public/schema/v1.json`](file:///e:/TaskForge/benchmarks/public/schema/v1.json).

In addition, dedicated automated integration test suite [`tests/integration/test_g95_public_benchmarks.py`](file:///e:/TaskForge/tests/integration/test_g95_public_benchmarks.py) verifies:
1. `test_g95_public_benchmarks_schema_file_exists`: Validates schema JSON structure and title.
2. `test_g95_public_benchmarks_quick_execution`: Executes the complete benchmark across W-01, W-02, and W-03, verifying schema compliance, throughput calculations, explicit outcome accounting, and mathematical latency reconciliation ($\Delta \le 0.002\,\text{ms}$).
3. `test_g95_public_benchmarks_single_workload`: Verifies selective `--workload W-01` execution.
4. `test_g95_public_benchmarks_latency_invariant_enforcement`: Tests harness validation failure when presented with malformed latency arithmetic.

All 4 benchmark tests and all 3 demo reproducibility tests pass 100% (7/7 passed).

---

## 5. Gate G9.5 Stage 2 Acceptance Summary

- **Deliverables Completed:**
  - Canonical benchmark harness: `benchmarks/public/run_public_benchmarks.py`
  - Versioned JSON schema: `benchmarks/public/schema/v1.json`
  - Canonical benchmark results artifact: `benchmarks/public/benchmark_results.json`
  - Dedicated integration tests: `tests/integration/test_g95_public_benchmarks.py`
  - Reconciliation Addendum: `docs/phases/phase_09/28_g95_stage2_reconciliation_addendum.md`
- **Provable Clean State:**
  - `git_commit`: `1d4a76cd047a61548e92e57cfb1f032ba7914068`
  - `git_dirty`: `false`
  - `harness_sha`: `6576de5ba663a77a07256b20ae0700adababb1d7b906343a66dce68ef853efc0` (matches script)
  - Zero changes to production `src/` code or Prometheus metric series.
