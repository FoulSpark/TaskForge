# Stage 1B Reconciliation Report — Lifecycle Attribution & Bottleneck Disentanglement

**Milestone:** Gate G9.3 — Stage 1B  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Formal Reconciliation for Stage 1B Gate Closure  
**Base Authorized Commit:** `c795315e3f49f8352406458bdbef4a692d62e9c9`  
**Implementation Commit:** `5ffd5fee2be12b4cd1118549de7d8701667acc08`  
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
1. **Worker Attempt Execution Pipeline Duration (`task_attempts.duration_ms`):** In `execute_task()` (`src/taskforge/workers/execution_engine.py`), a timer starts at entry to `execute_task()` before acquiring a DB session, runs `repo.record_attempt(status="RUNNING")` (a synchronous PostgreSQL write and commit), dispatches to a thread pool, executes the function, and records duration before calling `complete_task_success()`. Both Stage 1A-R and Stage 1B's summary tables queried this column from PostgreSQL.
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

### 1.3 Correction Regarding Profiler CPU Overhead
The previous assertion that lower observed handler duration proved zero execution overhead is formally retracted. A slight downward shift in duration during an instrumented trial reflects normal host scheduling and DB commit jitter on Windows, not negative profiler overhead. Statistical determination of execution overhead requires repeated multi-trial sampling under controlled CPU pinning.

---

## 2. Instrumentation Overhead Trial Methodology & Results

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

### 2.3 Formal Overhead Conclusion
Because only a single paired trial was conducted per workload condition ($N=1$), **instrumentation overhead remains statistically inconclusive**. The measured $8\%$ to $30\%$ duration differences cannot be separated from outbox polling sleep phase alignment ($100\,\text{ms}$ sleep) and worker dispatcher polling sleep ($200\,\text{ms}$ sleep) without a multi-trial paired campaign. The $23\%$ throughput reduction is acknowledged as an unverified single-trial delta rather than established natural variance.

---

## 3. Explicit Mathematical Definitions of Reported Stage Intervals

| Stage Interval | Start Event | End Event | Mathematical Definition | Precise Interpretation |
| :--- | :--- | :--- | :--- | :--- |
| **API Ingestion**<br>($T_{\text{api-server}}$) | `api_request_received` | `task_transaction_commit` | $t_{\text{task-transaction-commit}} - t_{\text{api-request-received}}$ | In-process FastAPI HTTP ingress through synchronous PostgreSQL task insertion commit. |
| **Outbox Claim**<br>($T_{\text{outbox-claim}}$) | `outbox_claim_start` | `outbox_claim_commit` | $t_{\text{outbox-claim-commit}} - t_{\text{outbox-claim-start}}$ | Batch database claim query execution duration (`SELECT ... FOR UPDATE SKIP LOCKED`). |
| **Outbox Lag**<br>($T_{\text{outbox-lag}}$) | `task_transaction_commit` | `outbox_claim_start` | $t_{\text{outbox-claim-start}} - t_{\text{task-transaction-commit}}$ | Dwell time from task commit return until the start of the first outbox claim cycle that observed the task. |
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

To distinguish insufficient worker capacity from dispatcher scheduling inefficiency, an empirical time-series reconstruction was executed from the raw JSONL event streams (`benchmarks/raw/g93_b1/events_16_g93_*.jsonl`).

### 4.1 Queue Depth, Occupancy, and Handoff Metrics

| Dimension | W-01 ($n=500$, $c=10$) | W-02 ($n=200$, $c=4$) | W-03 ($n=200$, $c=10$) |
| :--- | :---: | :---: | :---: |
| **Peak Redis Queue Depth** | 427 tasks | 178 tasks | 180 tasks |
| **Mean Redis Queue Depth** | 208.2 tasks | 92.9 tasks | 83.6 tasks |
| **Worker Occupancy Duration** ($t_{\text{ack}} - t_{\text{dequeue}}$)<br>$p50$ / $p95$ | 240.29 ms / 623.65 ms | 111.31 ms / 282.37 ms | 281.69 ms / 816.74 ms |
| **Dequeue-to-Lease Latency** ($t_{\text{lease-start}} - t_{\text{dequeue}}$)<br>$p50$ / $p95$ | **0.384 ms** / 3.032 ms | **0.290 ms** / 1.491 ms | **0.458 ms** / 3.779 ms |
| **Saturated Workers ($active \ge c$) when $Q > 0$** | 26.0% of duration | 18.0% of duration | 30.9% of duration |
| **Idle Workers ($active = 0$) when $Q > 0$** | **65.9% of duration** | **78.7% of duration** | **58.4% of duration** |

### 4.2 Critical Architectural Finding
1. **Dequeue-to-Lease Handoff is Negligible:** Dequeue-to-lease latency is sub-millisecond ($0.29 - 0.46\,\text{ms}$). Once dequeued, workers immediately begin lease acquisition.
2. **Idle Worker Capacity During Positive Queue Depth:** While queue depth was positive, active workers dropped to 0 for **58% to 78% of the time**.
3. **Dispatcher Sleep Loop Mechanism:** In `WorkerDispatcher._dispatch_loop()` (`src/taskforge/workers/dispatcher.py`), when `_active_tasks >= concurrency`, the dispatcher executes `time.sleep(self.poll_interval)` where `poll_interval = 0.2s` ($200\,\text{ms}$). While the dispatcher is asleep, active tasks in the thread pool complete within $100-150\,\text{ms}$. Active tasks reach 0, but the dispatcher continues sleeping for the remainder of its $200\,\text{ms}$ interval before dequeuing the next task.
4. **Single-Element Sequential Dequeue:** The dispatcher loop dequeues only one task per iteration via `broker.dequeue()`. Under burst conditions, replenishing a pool of 10 workers requires 10 round trips interspersed with loop scheduling overhead.

---

## 5. Revised Bottleneck Attribution Report

Based on the empirical evidence, latency components are disentangled into four separate subsystems:

```
Total Turnaround Latency (p50: 9.9s - 22.9s)
  │
  ├── 1. Redis Server Performance: NON-BOTTLENECK (<10ms)
  │      Redis round-trip enqueue and dequeue Lua evaluations take 2-8ms.
  │      Redis server is non-saturated and exhibits sub-millisecond execution.
  │
  ├── 2. Outbox & DB Processing: MINOR COMPONENT (<350ms total)
  │      Outbox claim query, outbox publication commit, lease acquisition,
  │      terminal persistence, and ACK collectively account for <350ms.
  │
  └── 3. Queue Residence Time before Dequeue: PRIMARY COMPONENT (>93% - 98%)
         Driven by two interacting factors:
         ├── A. Worker Drain Capacity (Burst Volume vs. Concurrency):
         │      Burst submission of 200-500 tasks exceeds immediate worker capacity.
         │      Full worker occupancy service time is 111-281ms per task.
         └── B. Dispatcher Scheduling Inefficiency (Inter-Task Idle Gaps):
                Coarse 200ms sleep in dispatcher when full causes workers to sit
                idle (active=0) for 58%-78% of queue-backed duration.
                Single-element dequeue serializes worker pool replenishment.
```

---

## 6. Revised Hypotheses for Stage 2 Target-Lock Review

Following the User's direction, candidate optimization hypotheses are formally prioritized:

- **Hypothesis A — Worker Drain Capacity Ceiling:**  
  *Premise:* The worker pool service time ($111-281\,\text{ms}$ per task across lease acquisition, execution, and terminal persistence) sets a physical throughput ceiling of $\sim 35-40$ TPS at $c=10$ and $\sim 15-20$ TPS at $c=4$. Under burst submissions, tasks inevitably accumulate in Redis.
- **Hypothesis B — Dispatcher Scheduling Inefficiency & Inter-Task Sleep Gaps (Priority 1 for Investigation):**  
  *Premise:* The dispatcher loop's coarse `time.sleep(0.2)` upon reaching concurrency and its single-element sequential dequeue loop leave worker threads idle for $58\%-78\%$ of the time while tasks wait in Redis. Replacing coarse polling sleeps with event-driven capacity notification and/or batch prefetching will eliminate inter-task worker starvation without violating lease contracts.
- **Hypothesis C — Worker Service-Time Ceiling:**  
  *Premise:* Even with optimal dispatching, synchronous database transactions for lease acquisition and terminal state commit bound individual task service time.
- **Deferred / Rejected:**  
  - *Redis server optimization:* Rejected as unjustified by empirical evidence (Redis latency is $< 10\,\text{ms}$).
  - *Outbox redesign:* Deferred (outbox lag is $70-145\,\text{ms}$, representing $< 1\%$ of turnaround).
