# Stage 1B Reconciliation Report — Lifecycle Attribution & Bottleneck Disentanglement

**Milestone:** Gate G9.3 — Stage 1B  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Formal Reconciliation for Stage 1B Gate Closure (Accepted with Qualifications)  
**Base Authorized Commit:** `c795315e3f49f8352406458bdbef4a692d62e9c9`  
**Implementation Commit:** `5ffd5fee2be12b4cd1118549de7d8701667acc08`  
**Reconciliation Commit:** `61f398f`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  

---

## 1. Handler Timing Reconciliation Against Stage 1A-R

### 1.1 Discrepancy Root Cause Analysis

A discrepancy was identified between handler duration figures reported in Stage 1A-R and Stage 1B:
- **Stage 1A-R:** W-01 $p50 = 129.57\,\text{ms}$, W-02 $p50 = 45.66\,\text{ms}$, W-03 $p50 = 125.52\,\text{ms}$.
- **Stage 1B 16-Event Profiler ($T_{\text{handler-exec}}$):** W-01 $p50 = 0.013\,\text{ms}$, W-02 $p50 = 0.123\,\text{ms}$, W-03 $p50 = 6.239\,\text{ms}$.
- **Stage 1B Summary Table (`execution_overhead_p50_ms`):**
  - Uninstrumented: W-01 $159.28\,\text{ms}$, W-02 $67.00\,\text{ms}$, W-03 $190.67\,\text{ms}$.
  - Instrumented: W-01 $148.19\,\text{ms}$, W-02 $63.28\,\text{ms}$, W-03 $171.51\,\text{ms}$.

This discrepancy arose from a metric naming ambiguity between two fundamentally distinct measurement scopes in TaskForge:
1. **Worker Attempt Pipeline Duration (`task_attempts.duration_ms`):** In `execute_task()` (`src/taskforge/workers/execution_engine.py`), a timer starts at entry to `execute_task()` before acquiring a DB session, runs `repo.record_attempt(status="RUNNING")` (a synchronous PostgreSQL write and commit), dispatches to a thread pool, executes the function, and records duration before calling `complete_task_success()`. Both Stage 1A-R and Stage 1B's summary tables queried this column from PostgreSQL. Stage 1A-R's measurements are therefore formally relabelled **Worker Attempt Pipeline Duration**.
2. **Pure Isolated Handler Duration ($T_{\text{handler-exec}}$):** In Stage 1B, the 16-event lifecycle profiler introduced explicit `handler_start` and `handler_end` events bracketing `fn(arg)` in `timed_callable()` in `execution_engine.py`. This isolates the target payload function alone.

### 1.2 Handler Measurement Comparison Table

| Dimension | Stage 1A-R Reported Metric | Stage 1B Summary Metric (`execution_overhead_p50_ms`) | Stage 1B 16-Event Profiler ($T_{\text{handler-exec}}$) |
| :--- | :--- | :--- | :--- |
| **Data Source** | DB query: `SELECT ta.duration_ms FROM task_attempts ta` | DB query: `SELECT ta.duration_ms FROM task_attempts ta` | Profiler JSONL: `handler_start` and `handler_end` |
| **Start Boundary** | Entry of `execute_task()` prior to DB `record_attempt()` | Entry of `execute_task()` prior to DB `record_attempt()` | Immediate invocation of `fn(arg)` in `timed_callable()` |
| **End Boundary** | Return of target function prior to `complete_task_success()` | Return of target function prior to `complete_task_success()` | Immediate return of `fn(arg)` in `timed_callable()` |
| **Clock Source** | `time.perf_counter()` (millisecond rounded) | `time.perf_counter()` (millisecond rounded) | `time.perf_counter_ns()` (nanosecond monotonic) |
| **Warmup Excluded?** | Yes (measured window tasks only) | Yes (measured window tasks only) | Yes (measured window tasks only) |
| **Payload & Handler** | Identical G9.0 benchmark definitions | Identical G9.0 benchmark definitions | Identical G9.0 benchmark definitions |
| **Scope** | **Worker Attempt Pipeline** (includes DB write) | **Worker Attempt Pipeline** (includes DB write) | **Isolated Handler-Only** (`fn(arg)`) |
| **W-01 (noop) $p50$** | $129.57\,\text{ms}$ (disabled / enabled) | $159.28\,\text{ms}$ (base) / $148.19\,\text{ms}$ (inst) | **$0.013\,\text{ms}$ ($13\,\mu\text{s}$)** |
| **W-02 (primes) $p50$** | $45.66\,\text{ms}$ (disabled / enabled) | $67.00\,\text{ms}$ (base) / $63.28\,\text{ms}$ (inst) | **$0.123\,\text{ms}$ ($123\,\mu\text{s}$)** |
| **W-03 (io) $p50$** | $125.52\,\text{ms}$ (disabled / enabled) | $190.67\,\text{ms}$ (base) / $171.51\,\text{ms}$ (inst) | **$6.239\,\text{ms}$** |

### 1.3 Qualification Regarding Overhead
Stage 1A-R cannot be used as evidence that isolated handler instrumentation had negligible overhead, because its reported measurements included database attempt writes and dispatch-related operations. Furthermore, lower observed handler durations in instrumented trials do not prove zero CPU overhead; they reflect normal host scheduling and DB commit jitter on Windows.

---

## 2. Instrumentation Overhead Trial Methodology & Inconclusive Assessment

### 2.1 Trial Methodology
The Stage 1B attribution campaign was executed with the following experimental setup:
- **Number of Trials:** Single paired trial ($N=1$) per workload (1 uninstrumented baseline trial followed by 1 instrumented trial).
- **Trial Ordering:** Sequential execution by workload: `W-01_baseline` $\to$ `W-01_instrumented` $\to$ `W-02_baseline` $\to$ `W-02_instrumented` $\to$ `W-03_baseline` $\to$ `W-03_instrumented`.
- **Database Reset:** Tables truncated between runs using `TRUNCATE tasks, task_attempts, task_results, outbox_events, task_events CASCADE;`. Redis was cleared via `FLUSHDB`.
- **Warmup and Measurement Windows:** 10% warmup tasks submitted and completely drained prior to triggering the measured burst window for every trial.

### 2.2 Per-Trial Empirical Observations

| Workload Trial | Measured Tasks | Wall-Clock Duration (s) | Achieved Throughput (TPS) | Worker Attempt $p50$ (`duration_ms`) |
| :--- | :---: | :---: | :---: | :---: |
| **W-01 Baseline** | 500 | 44.968 | 11.12 | 159.28 ms |
| **W-01 Instrumented** | 500 | 49.006 | 10.20 | 148.19 ms |
| *W-01 Delta* | — | *+8.98%* | *-8.27%* | *-6.97%* |
| **W-02 Baseline** | 200 | 24.718 | 8.09 | 67.00 ms |
| **W-02 Instrumented** | 200 | 32.135 | 6.22 | 63.28 ms |
| *W-02 Delta* | — | *+30.01%* | *-23.11%* | *-5.55%* |
| **W-03 Baseline** | 200 | 14.551 | 13.75 | 190.67 ms |
| **W-03 Instrumented** | 200 | 18.724 | 10.68 | 171.51 ms |
| *W-03 Delta* | — | *+28.68%* | *-22.33%* | *-10.05%* |

### 2.3 Formal Overhead Qualification
Because only a single paired trial was conducted per workload condition ($N=1$), **instrumentation overhead remains statistically inconclusive**. The measured $8\%$ to $30\%$ duration differences cannot be separated from outbox polling sleep phase alignment ($100\,\text{ms}$ sleep), worker dispatcher polling sleep ($200\,\text{ms}$ sleep), or natural run-to-run variance without a repeated, multi-trial paired campaign. The observed throughput reductions are unverified single-trial observations rather than an established natural baseline.

---

## 3. Explicit Mathematical Definitions of Reported Stage Intervals

| Stage Interval | Start Event | End Event | Mathematical Definition | Scope & Boundary Interpretation |
| :--- | :--- | :--- | :--- | :--- |
| **API Ingestion**<br>($T_{\text{api-server}}$) | `api_request_received` | `task_transaction_commit` | $t_{\text{task-transaction-commit}} - t_{\text{api-request-received}}$ | In-process FastAPI HTTP ingress through synchronous PostgreSQL task insertion commit. |
| **Outbox Claim**<br>($T_{\text{outbox-claim}}$) | `outbox_claim_start` | `outbox_claim_commit` | $t_{\text{outbox-claim-commit}} - t_{\text{outbox-claim-start}}$ | Batch database claim query execution duration (`SELECT ... FOR UPDATE SKIP LOCKED`). |
| **Outbox Lag**<br>($T_{\text{outbox-lag}}$) | `task_transaction_commit` | `outbox_claim_start` | $t_{\text{outbox-claim-start}} - t_{\text{task-transaction-commit}}$ | Dwell from task commit return until the start of the first outbox claim cycle that claimed it. |
| **Redis Enqueue**<br>($T_{\text{redis-enq}}$) | `redis_enqueue_start` | `redis_enqueue_complete` | $t_{\text{redis-enqueue-complete}} - t_{\text{redis-enqueue-start}}$ | Client-observed Redis round-trip latency for evaluating Lua enqueue script (`ZADD`). |
| **Outbox Publication**<br>($T_{\text{outbox-commit}}$) | `redis_enqueue_complete` | `outbox_published_commit` | $t_{\text{outbox-published-commit}} - t_{\text{redis-enqueue-complete}}$ | **Composite application and DB commit duration.** Includes enqueuing subsequent tasks in the batch and committing `status='PUBLISHED'`. |
| **Broker Dwell**<br>($T_{\text{broker-dwell}}$) | `redis_enqueue_complete` | `redis_dequeue_complete` | $t_{\text{redis-dequeue-complete}} - t_{\text{redis-enqueue-complete}}$ | **Client-observed queue residence time.** Measures total time a task resides in Redis before worker dequeue. |
| **Worker Lease**<br>($T_{\text{worker-lease}}$) | `lease_claim_start` | `lease_commit` | $t_{\text{lease-commit}} - t_{\text{lease-claim-start}}$ | Database transaction duration for acquiring task lease row lock in PostgreSQL. |
| **Handler Execution**<br>($T_{\text{handler-exec}}$) | `handler_start` | `handler_end` | $t_{\text{handler-end}} - t_{\text{handler-start}}$ | **Isolated handler-only execution duration.** Measures `fn(arg)` invocation directly. |
| **Terminal Persistence**<br>($T_{\text{terminal-persist}}$) | `terminal_transaction_start` | `terminal_commit` | $t_{\text{terminal-commit}} - t_{\text{terminal-transaction-start}}$ | Database transaction commit duration for writing final task state (`status='SUCCESS'`). |
| **Redis ACK**<br>($T_{\text{redis-ack}}$) | `terminal_commit` | `redis_ack_complete` | $t_{\text{redis-ack-complete}} - t_{\text{terminal-commit}}$ | Interval from DB terminal commit return through Redis Lua `ack()` (`ZREM`) return. |
| **Total Turnaround**<br>($T_{\text{end-to-end}}$) | `api_request_received` | `terminal_commit` | $t_{\text{terminal-commit}} - t_{\text{api-request-received}}$ | End-to-end task turnaround duration from API ingress to database terminal commit. |

---

## 4. Empirical Queue Depth and Worker Utilization Analysis

### 4.1 Reconstruction Methodology
The timeline reconstruction was derived from the raw JSONL timestamp logs using the following exact rules:
- **Client-Observed Logical Queue Depth ($Q(t)$):** At time $t$, $Q(t) = \sum \mathbf{1}(t_{\text{redis-enqueue-complete}, i} \le t < t_{\text{redis-dequeue-complete}, i})$ for all tracked tasks. Initial queue depth $Q(0) = 0$ (verified by draining warmup tasks and resetting the queue before timing). All events are ordered by host monotonic nanoseconds (`t_mono_ns`).
- **Active Worker Slot Count ($W(t)$):** Measured as the count of in-flight task occupancies active at time $t$: $W(t) = \sum \mathbf{1}(t_{\text{redis-dequeue-complete}, i} \le t < t_{\text{redis-ack-complete}, i})$. This serves as a client-observed proxy for occupied worker slots in the thread pool.
- **Sampling:** 1,000 uniform temporal intervals across the active campaign duration.

### 4.2 Empirical Metrics & Per-Task Analysis

| Dimension | W-01 ($n=500$, $c=10$) | W-02 ($n=200$, $c=4$) | W-03 ($n=200$, $c=10$) |
| :--- | :---: | :---: | :---: |
| **Peak Logical Queue Depth** | 427 tasks | 178 tasks | 180 tasks |
| **Mean Logical Queue Depth** | 208.2 tasks | 92.9 tasks | 83.6 tasks |
| **Worker Slot Occupancy** ($t_{\text{ack}} - t_{\text{dequeue}}$)<br>Mean / $p50$ / $p95$ | **285.19 ms** / 240.29 ms / 623.65 ms | **128.29 ms** / 111.31 ms / 282.37 ms | **335.60 ms** / 281.69 ms / 816.74 ms |
| **Illustrative Capacity Estimate** ($c / E[T_{\text{slot}}]$) | **35.06 tasks/s** | **31.18 tasks/s** | **29.80 tasks/s** |
| **Dequeue-to-Lease Handoff** ($t_{\text{lease-start}} - t_{\text{dequeue}}$)<br>$p50$ / $p95$ | **0.384 ms** / 3.032 ms | **0.290 ms** / 1.491 ms | **0.458 ms** / 3.779 ms |
| **Saturated Workers ($W \ge c$) when $Q > 0$** | 26.0% of queue-positive time | 18.0% of queue-positive time | 30.9% of queue-positive time |
| **Idle Workers ($W = 0$) when $Q > 0$** | **65.9% of queue-positive time** | **78.7% of queue-positive time** | **58.4% of queue-positive time** |
| **Per-Task Ratio $r_i = T_{\text{dwell},i} / T_{\text{e2e},i}$**<br>$p50$ / $p95$ | **97.79%** / 98.93% | **98.49%** / 99.32% | **93.65%** / 96.35% |
| **Combined Non-Dwell DB/Worker Stages (Per-Task)**<br>Mean / $p50$ / $p95$ | 161.46 ms / 144.26 ms / 301.83 ms | 76.84 ms / 68.30 ms / 146.27 ms | 189.91 ms / 165.82 ms / 396.62 ms |
| **Outbox Lag Ratio ($p50_{\text{outbox}} / p50_{\text{e2e}}$)** | 0.48% (110.91 ms / 22,936.6 ms) | 0.46% (71.78 ms / 15,670.9 ms) | **1.46%** (145.13 ms / 9,942.1 ms) |

### 4.3 Detailed Architectural Mechanics
1. **Per-Task Queue Residence Dominance:** The genuine per-task ratio $r_i = T_{\text{broker-dwell},i} / T_{\text{end-to-end},i}$ confirms that broker dwell accounts for $93.65\% - 98.49\%$ ($p50$) of individual task turnaround across all three workloads. (Note: Because publication and worker dequeue overlap in $2-5\%$ of tasks, stage percentiles cannot be additively summed to 100%).
2. **Outbox Contribution is Workload-Dependent:** Outbox lag represents $0.46\% - 0.48\%$ of turnaround for W-01 and W-02, but rises to $1.46\%$ for W-03 ($p50 = 145.13\,\text{ms}$, $p95 = 270.23\,\text{ms}$). Outbox lag is not universally $< 1\%$.
3. **Illustrative Capacity vs. Burst Backlog:** Using the mean slot occupancy duration $E[T_{\text{slot}}]$, the illustrative steady-state worker drain capacity estimate is $\sim 30 - 35\,\text{tasks/s}$. Under burst submissions of 200–500 tasks, the arrival rate temporarily exceeds this sustainable drain capacity, causing a temporary queue backlog in Redis.
4. **Dispatcher Sleep Loop Idle Gaps:** The substantial percentage of time ($58\% - 78\%$) with $Q > 0$ and $W = 0$ is governed by `WorkerDispatcher._dispatch_loop()` (`src/taskforge/workers/dispatcher.py`):
   ```python
   with self._active_lock:
       if self._active_tasks >= self.concurrency:
           time.sleep(self.poll_interval)  # 0.2s fixed sleep
           continue
   ```
   When all worker slots are occupied, the loop sleeps for $200\,\text{ms}$. In-flight tasks complete within their slot occupancy interval (mean $128 - 335\,\text{ms}$). If running tasks finish during the sleep, the dispatcher thread remains asleep, leaving available capacity unused while tasks wait in Redis.
5. **Sequential Dequeue Serialization:** The dispatcher loop executes a single-element dequeue per cycle (`broker.dequeue()`), serializing replenishment of the worker pool.

---

## 5. Revised Bottleneck Attribution Qualifications

1. **Redis Server Performance:** No current evidence identifies Redis server execution as the primary bottleneck. Client-observed enqueue round trip ($T_{\text{redis-enq}}$) is $2.8 - 8.1\,\text{ms}$ ($p50$). However, because server-side CPU utilization and standalone dequeue round-trip distributions were not directly instrumented, Redis execution cannot be conclusively ruled out.
2. **Worker-Side Scheduling & Drain Capacity:** The empirical evidence points toward worker-side scheduling efficiency and concurrency drain limits as the primary factors governing queue residence time under burst arrivals.

---

## 6. Stage 2 Investigation Target & Hypotheses

Authorized diagnostic investigation target: **`WorkerDispatcher` scheduling efficiency under positive queue depth.**

### Hypotheses to Disentangle in Stage 2:
- **H1 — Polling Wake-up Delay:** The dispatcher remains asleep after one or more worker slots become available due to the fixed $200\,\text{ms}$ polling sleep.
- **H2 — Sequential Dequeue Overhead:** Single-task dequeue and loop scheduling materially delay replenishment of available worker slots.
- **H3 — Service-Time Limitation:** Workers remain occupied because their synchronous database operations (lease acquisition, terminal persistence) genuinely bound slot turnover.
- **H4 — Queue/Worker State Mismatch:** Reconstructed logical queue depth and active worker proxy do not accurately reflect internal dispatcher state.

### Scope & Constraints:
Production semantics, leases, fencing, retry policies, outbox boundaries, and PostgreSQL authoritative state remain strictly protected and unmodified. Diagnostic instrumentation will be applied separately to test wake-up mechanisms before evaluating batch dequeue.
