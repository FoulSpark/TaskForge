# Gate G9.3 — Final Performance Profiling, Bottleneck Attribution & Optimization Report

**Milestone:** Gate G9.3 — Stage 3 (End-to-End Milestone Synthesis)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Complete Milestone Synthesis & Closure Report  
**Production Commit:** `a89858b73f8488e05cbcc2393a61f3080ffc7a38` (Production adoption of event-driven wake-up)  
**Reconciliation Commit:** `2d3b7f6` (Phase 2C closure)  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Full Platform Regression Suite:** **235 / 235 passed** (0 failures, 0 skips)  

---

## 1. Executive Summary & Purpose

Gate G9.3 was established to replace unmeasured assumptions about platform performance with rigorous, empirical lifecycle attribution. Through a sequence of staged investigations, TaskForge diagnosed end-to-end task turnaround bottlenecks, isolated internal dispatcher scheduling delay, developed and verified a targeted optimization, and established an evidence-based target-lock:

1. **Empirical Lifecycle Attribution (Stage 1B):** Resolved the critical distinction between composite queue wait and actual handler execution. Tasks spent $85\% - 95\%$ of their lifetime resident in Redis sorted sets ($T_{\text{dwell}} \approx 2,000 - 5,000\,\text{ms}$) while workers were idle for $58\% - 78\%$ of queue-backed time, despite actual handler execution requiring only $0.05 - 5.0\,\text{ms}$.
2. **Dispatcher Scheduling Diagnosis (Stage 2 Phase 2A):** Isolated internal scheduling delays. $74.5\% - 89.9\%$ of worker completions occurred while the dispatcher was asleep in its polling loop, contributing $75\% - 82\%$ ($23 - 24\,\text{ms}$) of total slot replenishment latency.
3. **Targeted Optimization & Adoption (Stage 2 Phase 2B):** Implemented an isolated, lost-wakeup-proof `threading.Event` capacity wake-up signal. Replenishment latency dropped by **$74.5\% - 89.4\%$** ($29 - 34\,\text{ms} \to 3.1 - 8.6\,\text{ms}$), sleep lag dropped by **$94.9\% - 98.7\%$** ($24 - 26\,\text{ms} \to 0.35 - 1.21\,\text{ms}$), and queue starvation collapsed by **$86.4\% - 93.5\%$** ($25\% - 31\% \to 1.7\% - 4.0\%$). Production adoption was hardened and adopted at commit `a89858b`.
4. **Target-Lock & Batch Dequeue Rejection (Stage 2 Phase 2C):** Rebaselined the adopted dispatcher across 18 trials. Redis server-side execution per `EVALSHA` is only $50 - 62\,\mu\text{s}$. In the standard production configuration ($c=4$), worker capacity is **$91.3\%$ saturated** and dispatcher active-duty is only **$7.5\%$**. **Batch dequeue was formally REJECTED for the production operating envelope**, avoiding unnecessary architectural complexity.
5. **Identified Scaling Boundary:** A single-threaded dispatcher serializing single-task round-trips exhibits a substantial serialization constraint at the tested $c=20$ configuration (dispatcher duty cycle rises to $74\%$, worker saturation drops to $25\%$), establishing an evidence-based scaling boundary for future high-concurrency architectures.

---

## 2. Baseline & Measurement Methodology

### 2.1 Workload Definitions Lock
Workload definitions were locked strictly to historical milestone G9.0 specifications, independently defining task payload parameters and task counts:

| Workload ID | Name | Task Payload Parameters | Measured Tasks ($n$) | Warmup Tasks | Concurrency ($c$) |
| :--- | :--- | :--- | :---: | :---: | :---: |
| **W-01** | `baseline_noop_profiling` | `{"benchmark": "g90_noop", "sample": true}` | 500 | 50 | 10 |
| **W-02** | `cpu_bound_prime_sieve` | `{"n": 1000}` (deterministic sieve up to 1,000) | 200 | 20 | 4 |
| **W-03** | `io_simulation_delay` | `{"delay_seconds": 0.005, "jitter": false}` (5 ms simulated I/O) | 200 | 20 | 10 |

- Artifact File: `benchmarks/scenarios/g90/workload_definitions.json`
- Workload Hash (SHA-256): `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`

### 2.2 Measurement Boundaries & Clock Synchronization
- **Clock Limitations:** All measurements were captured via host monotonic nanoseconds (`time.perf_counter_ns()`) for high-precision durations, paired with UTC epoch timestamps (`time.time()`). Zero cross-machine clock drift exists because all benchmark components (FastAPI application, outbox publisher, dispatcher, PostgreSQL, Redis) executed on a single Windows 11 host.
- **Clock Offset Measurement:** A dedicated clock synchronization audit confirmed monotonic counter linearity with $< 50\,\text{ns}$ jitter and host epoch synchronization error $< 0.1\,\text{ms}$.
- **Measurement Isolation:** Every benchmark run executed against disposable database (`taskforge_g93_benchmark`) and isolated Redis databases, with full database truncation (`TRUNCATE CASCADE`) and Redis `FLUSHDB` executed between every trial.

### 2.3 Diagnostic Instrumentation & Overhead Qualifications
The 16-event lifecycle diagnostic profiler was designed with strict non-blocking guarantees:
- Dedicated flusher background thread with bounded queue capacity (65,536 events).
- Zero event drops and zero unpersisted records across all reported campaigns.
- **Overhead Qualifications:** Across 3-paired alternating evaluations on historical baselines, descriptive wall-clock duration deltas ranged between $+1.21\%$ and $+6.91\%$ (throughput deltas: $-1.32\%$ to $-6.36\%$). With $N=3$ paired observations per workload, these measurements are descriptive characterizations of instrumentation cost, not statistically conclusive variance models.

---

## 3. Lifecycle Bottleneck Attribution (Stage 1B)

Stage 1B evaluated the complete 16-event task lifecycle:
1. `api_request_received` $\to$ `task_transaction_commit`: HTTP ingestion and synchronous PostgreSQL persistence.
2. `task_transaction_commit` $\to$ `outbox_claimed`: Transactional outbox polling delay (conservative commit-return ordering).
3. `outbox_claimed` $\to$ `redis_enqueue_complete`: Outbox publisher serialization and Redis sorted set insertion.
4. `redis_enqueue_complete` $\to$ `redis_dequeue_complete`: **Redis Broker Dwell Time ($T_{\text{dwell}}$)**.
5. `redis_dequeue_complete` $\to$ `handler_start`: Worker thread assignment and lease acquisition.
6. `handler_start` $\to$ `handler_complete`: Task function execution.
7. `handler_complete` $\to$ `terminal_commit`: Terminal state write in PostgreSQL.
8. `terminal_commit` $\to$ `redis_ack_complete`: Delivery ACK and Redis in-flight cleanup.

```
Ingestion         Outbox Publishing           Redis Queue Dwell             Execution & Terminal ACK
 [DB Commit] ──> [Outbox Claim] ──> [Enqueue] ────────────────────> [Dequeue] ──> [Handler] ──> [DB Commit] ──> [ACK]
     │                  │              │                               │              │              │            │
     └──── 10-20 ms ────┴─── 2-5 ms ───┴──────── 2,000-5,000 ms ───────┴─── 1-3 ms ──┴── 0.05-5 ms ┴── 5-10 ms ─┘
                                          ▲
                                          │
                        (85% - 95% of Total Lifecycle Duration)
```

### Empirical Discovery:
- **Broker Dwell Dominance:** Median queue dwell time ($T_{\text{dwell}}$) was $2,000 - 5,000\,\text{ms}$ under backpressure, while actual handler execution ($T_{\text{exec}}$) was only $0.05 - 1.5\,\text{ms}$ (W-01/W-02) or $5.0\,\text{ms}$ (W-03).
- **Idle Worker Paradox:** Sampling queue depth $Q(t)$ and active worker occupancy $W(t)$ across 1,000 continuous time-steps revealed that although the Redis queue was consistently non-empty ($Q(t) > 0$), active worker occupancy was zero ($W=0$) for **$58\% - 78\%$** of queue-positive time.
- **Attribution Finding:** The primary bottleneck in TaskForge was not task execution, database commit latency, or Redis throughput limits, but the **internal scheduling efficiency of `WorkerDispatcher` in replenishing available worker slots**.

---

## 4. Dispatcher Optimization Workstream (Stage 2)

### 4.1 Phase 2A — Scheduling Diagnostics
To isolate the cause of worker idling, `WorkerDispatcher` was instrumented with 6 scheduling events (`dispatcher_sleep_start`, `dispatcher_sleep_end`, `dispatcher_dequeue_start`, `redis_dequeue_complete`, `dispatcher_task_submitted`, and `worker_slot_released`).

An authorized 3-paired interleaved diagnostic campaign (18 trials) measured:
- **Releases During Sleep:** **$74.5\% - 89.9\%$** of all worker slot releases occurred while the dispatcher thread was asleep in its fixed $200\,\text{ms}$ polling sleep.
- **Sleep Lag Contribution:** Sleep lag ($T_{\text{sleep-lag}}$ $p50 \approx 22.9 - 24.0\,\text{ms}$) accounted for **$75\% - 82\%$** of the total slot replenishment latency ($T_{\text{replenish-lag}} \approx 28.6 - 32.6\,\text{ms}$).
- **Redis Dequeue Latency:** Client dequeue round-trip ($T_{\text{dequeue-exec}}$) took only $1.47 - 4.26\,\text{ms}$ ($5\% - 15\%$ of replenishment lag).
- **Historical Verdict:** Hypothesis H1 (Polling Wake-up Delay) was **partially supported under original pre-registered thresholds** (the original criteria $T_{\text{sleep-lag}} > 50\,\text{ms}$ and $R_{\text{starve}} > 30\%$ were not met); however, the $75\% - 82\%$ dominance of sleep lag in replenishment latency and high proportion of releases during sleep definitively justified testing an event-driven capacity wake-up signal in Phase 2B.

### 4.2 Phase 2B — Capacity Wake-up Signal & Production Adoption
Phase 2B evaluated an isolated `threading.Event`-based capacity wake-up mechanism to eliminate polling sleep delay.

#### Lost-Wakeup Prevention Architecture:
```
[Dispatch Thread (Consumer)]                   [Worker Pool Thread (Producer)]
           │                                                  │
 1. _capacity_event.clear()                                   │
           │                                                  │
 2. with _active_lock:                                        │
      is_sat = (_active_tasks >= concurrency)                 │
           │                                                  │
 3. if is_sat:                                                │
      _capacity_event.wait(timeout=poll_interval)             │
           │                                                  │
           │ <---------------- (Slot released) -------------- │
           │                                           A. with _active_lock:
           │                                                _active_tasks -= 1
           │                                           B. _capacity_event.set()
           │                                                  │
 4. Dispatcher wakes immediately                              │
    (proceeds to broker.dequeue())                            │
```

- **Race Window Proof:**
  * Worker finishes before `clear()` $\implies$ `_active_tasks` decremented $\implies$ check returns `False` $\implies$ no wait.
  * Worker finishes between `clear()` and check $\implies$ check returns `False` $\implies$ no wait.
  * Worker finishes between check and `wait()` $\implies$ event is set $\implies$ `wait()` returns immediately.
  * Worker finishes during `wait()` $\implies$ event is set $\implies$ OS unblocks dispatcher in $< 1\,\text{ms}$.
  * Bounded timeout fallback $\implies$ elapses cleanly at `poll_interval` ($0.2\,\text{s}$) if queue empty.
  * Clean shutdown $\implies$ `stop()` sets `_stop_event` and `_capacity_event`, unblocking the thread promptly.

#### Safeguard Hardening & Concurrency Verification:
The implementation was hardened with 9 permanent deterministic unit tests ([`tests/unit/test_dispatcher_capacity_wakeup.py`](file:///e:/TaskForge/tests/unit/test_dispatcher_capacity_wakeup.py)):
1. `test_capacity_wakeup_signal_before_wait`
2. `test_capacity_wakeup_signal_during_wait`
3. `test_capacity_wakeup_interleaved_race`
4. `test_capacity_wakeup_multiple_notifications_no_spinning`
5. `test_capacity_wakeup_clean_shutdown`
6. `test_capacity_wakeup_fallback_timeout`
7. `test_capacity_wakeup_default_enabled`
8. `test_capacity_wakeup_worker_exception_signals_event` (verifies `finally:` execution upon handler failure)
9. `test_capacity_wakeup_high_contention_simultaneous_completions` (4-worker barrier releasing simultaneously while 8 tasks queued)

**Production Adoption:** Adopted by default under commit `a89858b` in [`src/taskforge/workers/dispatcher.py`](file:///e:/TaskForge/src/taskforge/workers/dispatcher.py) and configured explicitly in [`src/taskforge/main.py`](file:///e:/TaskForge/src/taskforge/main.py), with `enable_capacity_wakeup=False` preserved for zero-friction rollback.

### 4.3 Phase 2C — Post-Adoption Rebaseline & Batch Dequeue Rejection
An 18-trial diagnostic campaign was executed to determine whether sequential single-element dequeue constitutes an active bottleneck after wake-up optimization:
- Redis server execution per `EVALSHA` is only **$50 - 62\,\mu\text{s}$** (mean server-reported time via `INFO commandstats`).
- Under standard production concurrency ($c=4$):
  * Dequeue round-trip $p50 \approx 2.0\,\text{ms}$.
  * Replenishment lag $p50 \approx 2.7\,\text{ms}$.
  * Dispatcher active-duty ratio is only **$7.5\%$**.
  * Worker capacity saturation is **$91.3\%$**.
  * Queue starvation is **$1.6\%$**.
- **Formal Target-Lock Decision:** Batch dequeue was **REJECTED for the production operating envelope ($c \le 10$)**.
- **High-Concurrency Boundary ($c=20$):** At $c=20$, sequential single-task round-trips showed a substantial dispatcher-serialization constraint: dispatcher duty cycle surged to $74.0\%$, worker saturation dropped to $24.8\%$, and cohort replacement lag reached $73.2\,\text{ms}$. The onset threshold between $c=10$ and $c=20$ remains unmeasured.

---

## 5. Comprehensive Before / After Performance Comparison

All figures represent multi-trial means across 3 paired observations per condition:

| Metric | Workload / Concurrency | Pre-Optimization Baseline (Phase 2A / 2B Baseline) | Post-Optimization Production (`a89858b` / Phase 2C) | Measured Delta | Attribution / Operational Impact |
| :--- | :--- | :---: | :---: | :---: | :--- |
| **Sleep Lag ($T_{\text{sleep-lag}}$)** | W-01 (`noop`, $c=10$) | 23.85 ms | **1.21 ms** | **$-94.94\%$** | Sleep delay collapsed |
| ($p50$) | W-02 (`prime`, $c=4$) | 25.82 ms | **0.35 ms** | **$-98.66\%$** | Near-instantaneous capacity wake-up |
| | W-03 (`io`, $c=10$) | 24.14 ms | **1.05 ms** | **$-95.64\%$** | Sub-millisecond signaling |
| **Replenishment Lag ($T_{\text{replenish}}$)** | W-01 (`noop`, $c=10$) | 33.56 ms | **6.81 – 7.14 ms** | **$-79.2\%$** | Unambiguous 1-to-1 slot turnaround |
| ($p50$) | W-02 (`prime`, $c=4$) | 29.36 ms | **2.64 ms** | **$-91.0\%$** | Turnover bounded only by Redis RTT |
| | W-03 (`io`, $c=10$) | 33.99 ms | **6.46 ms** | **$-81.0\%$** | Immediate replenishment |
| **Queue Starvation Ratio ($R_{\text{starve}}$)** | W-01 (`noop`, $c=10$) | 28.48% | **3.52% – 3.60%** | **$-87.4\%$** | Idling while queue backed eliminated |
| | W-02 (`prime`, $c=4$) | 26.78% | **1.30% – 1.74%** | **$-93.5\%$** | Worker capacity continuously fed |
| | W-03 (`io`, $c=10$) | 25.09% | **3.41% – 3.80%** | **$-86.4\%$** | Capacity starvation $< 4\%$ |
| **Worker Saturation Ratio ($S_{\text{work}}$)** | W-01 (`noop`, $c=10$) | 44.5% | **78.0% – 78.5%** | **$+76.4\%$** | Available capacity actively utilized |
| ($W(t) \ge c$ during $Q(t) > 0$) | W-02 (`prime`, $c=4$) | 48.3% | **90.9% – 91.3%** | **$+88.6\%$** | Pool saturated $> 90\%$ of time |
| | W-03 (`io`, $c=10$) | 41.2% | **79.5%** | **$+93.0\%$** | High concurrency saturation |
| **Throughput (TPS)** | W-01 (`noop`, $c=10$) | 26.35 TPS | **27.40 – 27.42 TPS** | **$+4.0\%$** | Sustained turnover improvement |
| | W-02 (`prime`, $c=4$) | 24.96 TPS | **28.75 TPS** | **$+15.2\%$** | Compute turnover accelerated |
| | W-03 (`io`, $c=10$) | 26.29 TPS | **27.81 TPS** | **$+5.8\%$** | I/O turnaround improved |
| **End-to-End Latency ($T_{\text{e2e}}$)** | W-01 (`noop`, $c=10$) | 5,718.77 ms | **5,172.12 ms** | **$-9.56\%$** | Faster task lifecycle |
| ($p50$) | W-02 (`prime`, $c=4$) | 2,811.45 ms | **2,147.21 ms** | **$-23.63\%$** | Substantial turnaround reduction |
| | W-03 (`io`, $c=10$) | 2,460.87 ms | **2,297.87 ms** | **$-6.62\%$** | Consistent E2E reduction |
| **Process CPU Utilization** | All Workloads | — | — | **Neutral ($\pm 3\%$)** | Zero busy-spinning, zero storms |

*Statistical Qualification:* With three paired observations per workload and concurrency level, these results are descriptive characterizations of performance in the tested environment rather than universal statistical guarantees across arbitrary operating conditions.

---

## 6. Final Architectural Decisions & Target-Lock State

### 6.1 What Was Optimized:
- **Event-Driven Capacity Wake-up Signal:** Replaced blind polling delay with `threading.Event` signaling upon slot release (`a89858b`).
- **Defensive Error Handling:** Ensured `finally:` blocks and executor submission exception handlers release capacity and signal the dispatcher under all failure paths.

### 6.2 What Was Rejected:
- **Batch Dequeue / Prefetching:** Formally **REJECTED** for the production operating envelope ($c \le 10$). Single-element sequential dequeue supplies tasks faster than workers consume them, with $> 90\%$ worker saturation and only $7.5\%$ dispatcher active-duty at $c=4$. Adding batching would introduce lease prefetching hazards, fencing complexity, and partial failure ambiguity for zero measurable throughput gain.
- **Unmeasured Broker Replacements:** The hypothesis that Redis throughput bounded system capacity was refuted; server-side Lua execution requires only $\sim 50\,\mu\text{s}$.

### 6.3 What Remains a Measured Limitation:
- **High-Concurrency Serialization ($c=20$):** At $c=20$, a single dispatcher thread serializing single-task round-trips incurs $74\%$ duty cycle and drops worker saturation to $25\%$. The exact onset threshold between $c=10$ and $c=20$ remains unmeasured.
- **Outbox Publisher Polling Lag:** Transactional outbox polling contributes $\sim 2 - 5\,\text{ms}$ of initial lag between PostgreSQL task commit and Redis availability.

### 6.4 What Must Not Be Changed Without New Evidence:
- **Authoritative PostgreSQL State:** PostgreSQL remains the single source of truth for task state and lease ownership.
- **Lease Fencing Contracts:** Fencing tokens and heartbeat expirations remain strictly locked.
- **Priority ZSET Semantics:** Single-element dequeue respecting Redis sorted set score ordering must be preserved.
- **Transactional Outbox Boundaries:** Tasks must commit to PostgreSQL before outbox publication.

---

## 7. Milestone Provenance & Audit Artifacts

| Component | SHA-256 Digest / Git Reference |
| :--- | :--- |
| **Phase 8 Frozen Tag** | `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`) |
| **Production Adoption Commit** | `a89858b73f8488e05cbcc2393a61f3080ffc7a38` |
| **Phase 2C Reconciliation Commit** | `2d3b7f6` |
| **Synthesis Report Commit** | Current Head |
| **Workload Definitions Hash** | `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110` |
| **Phase 2C Campaign Harness Hash** | `9c58c77430f044f3e62503754f65af64c20b5e255c8a82eb1014b590206979a8` |
| **Full Regression Suite State** | **235 / 235 tests passed** in 266.42s (0 failures, 0 skips) |
| **Working Tree Status** | Clean (`nothing to commit, working tree clean`) |

---

## 8. Conclusion & Gate G9.3 Closure

Gate G9.3 is formally concluded. TaskForge has demonstrated an evidence-driven performance engineering workflow: measuring constraints empirically, testing targeted interventions with lost-wakeup-proof concurrency safety, adopting verified optimizations into production, and deliberately rejecting speculative optimizations that add complexity without measurable benefit.

The platform is certified ready to advance to **Gate G9.4 (Operational Resilience & Reliability Hardening)**.
