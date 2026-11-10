# Stage 2 Diagnostic Protocol — Dispatcher Scheduling Efficiency Investigation

**Milestone:** Gate G9.3 — Stage 2 Phase 2A (Diagnostic Target-Lock Investigation)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Formally Authorized Diagnostic Protocol  
**Authorized Base Commit:** `61f398f`  
**Protocol Amendments Commit:** `2533717`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Scope Boundary:** **Strictly Diagnostic Investigation.** Zero production optimizations, zero changes to production state machines, zero modifications to worker concurrency, and zero changes to core queue or delivery semantics.

---

## 1. Investigation Objective & Pre-Registered Decision Rules

The objective of Stage 2 Phase 2A is to measure the internal scheduling dynamics of `WorkerDispatcher` during queue-positive periods to empirically determine the cause of worker-pool idle gaps ($W=0$ during $58\%-78\%$ of queue-backed time).

The investigation will evaluate the following four operational hypotheses against **pre-registered diagnostic decision rules**:

- **H1 — Polling Wake-up Delay:**  
  *Mechanism:* The dispatcher remains asleep in its fixed $200\,\text{ms}$ polling sleep after one or more worker slots become available, delaying task acquisition.  
  *Pre-Registered Decision Rule:* Supported if median wake-up delay $T_{\text{sleep-lag}} > 50\,\text{ms}$, starvation ratio $R_{\text{starve}} > 30\%$, and a substantial proportion ($> 40\%$) of slot releases occur while the dispatcher is asleep.
- **H2 — Sequential Dequeue Overhead:**  
  *Mechanism:* Single-task sequential dequeue and in-loop Python scheduling materially delay replenishing available worker slots.  
  *Pre-Registered Decision Rule:* Supported if $T_{\text{sleep-lag}}$ is low ($< 10\,\text{ms}$) but total slot replenishment time scales linearly with available capacity and $T_{\text{dequeue-exec}}$ accounts for a material fraction ($> 40\%$) of replenishment lag.
- **H3 — Service-Time Limitation:**  
  *Mechanism:* Workers remain continuously occupied because synchronous database transactions (lease acquisition, state transitions, terminal persistence) genuinely bound slot turnover.  
  *Pre-Registered Decision Rule:* Supported if active worker occupancy $W(t) \ge c$ for $> 80\%$ of queue-positive time and sustained throughput matches illustrative capacity limits.
- **H4 — Queue/Worker State Mismatch:**  
  *Mechanism:* Client-observed logical queue depth and active worker proxies do not accurately reflect internal dispatcher capacity or Redis queue state.  
  *Pre-Registered Decision Rule:* Supported if comparison between internal dispatcher `_active_tasks` and reconstructed occupancy $W(t)$ reveals material divergence, or if logical queue depth remains positive while Redis sorted sets are empty.

*(Note: If multiple hypotheses are supported, their relative percentage contributions to replenishment latency will be reported rather than selecting an exclusive single winner).*

---

## 2. Workload Definitions & Unambiguous Notation Lock

Workloads are locked strictly to the historical G9.0 definitions with independent specification of payload parameters and task counts:

| Workload ID | Name | Task Payload Parameters | Measured Tasks ($n$) | Warmup Tasks | Concurrency ($c$) |
| :--- | :--- | :--- | :---: | :---: | :---: |
| **W-01** | `noop` | `{"key": "value"}` | 500 | 50 | 10 |
| **W-02** | `compute_heavy` | `{"compute_bound": True, "prime_limit": 1000}` (prime-sieve limit $1,000$) | 200 | 20 | 4 |
| **W-03** | `io_heavy` | `{"io_bound": True, "size_kb": 10}` (10 KB buffer) | 200 | 20 | 10 |

---

## 3. Extended Diagnostic Event Contract & Correlation Model

To eliminate correlation ambiguity across concurrent worker completions, every diagnostic event is extended with monotonic sequence numbering, loop iteration tracking, and state correlation:

### 3.1 Event Fields Contract
1. `seq`: Monotonically increasing 64-bit integer assigned by the diagnostic profiler at emission.
2. `event`: Event type name string.
3. `task_id`: Task identifier string (when associated with a specific task).
4. `t_mono_ns`: Host monotonic nanosecond timestamp (`time.perf_counter_ns()`).
5. `t_epoch_utc`: Host UTC epoch timestamp (`time.time()`).
6. `metadata`: Key-value payload containing:
   - `sleep_id`: Unique identifier for each sleep interval (e.g., `sleep-1`, `sleep-2`).
   - `loop_iter`: Monotonic dispatcher loop iteration counter.
   - `iter_id`: Unique loop iteration identifier (e.g., `iter-142`).
   - `active_tasks`: Dispatcher internal `self._active_tasks` count at that exact transition.
   - `concurrency`: Worker pool concurrency limit.

### 3.2 Five Extended Dispatcher Diagnostic Events
| Event Name | Emission Point | Captured State & Identifiers |
| :--- | :--- | :--- |
| `dispatcher_sleep_start` | `WorkerDispatcher._dispatch_loop()` immediately before `time.sleep(self.poll_interval)` | `sleep_id`, `loop_iter`, `active_tasks`, `concurrency` |
| `dispatcher_sleep_end` | `WorkerDispatcher._dispatch_loop()` immediately after `time.sleep()` returns | `sleep_id`, `loop_iter`, `active_tasks`, `concurrency` |
| `dispatcher_dequeue_start` | `WorkerDispatcher._dispatch_loop()` immediately before calling `broker.dequeue()` | `loop_iter`, `iter_id`, `active_tasks` |
| `redis_dequeue_complete` | `WorkerDispatcher._dispatch_loop()` immediately upon `broker.dequeue()` return | `task_id`, `loop_iter`, `iter_id` |
| `dispatcher_task_submitted` | `WorkerDispatcher._dispatch_loop()` immediately after `_executor.submit()` | `task_id`, `loop_iter`, `iter_id`, `active_tasks` |
| `worker_slot_released` | `WorkerDispatcher._task_wrapper()` `finally:` block immediately after decrementing `_active_tasks` | `task_id`, `active_tasks`, `concurrency` |

---

## 4. Derived Latency Definitions & Event Matching Methodology

### 4.1 Slot Replenishment Matching Methodology
Because worker completions occur concurrently across thread pool workers, events are correlated as follows:
- **Wake-up Delay Matching:** For any `worker_slot_released` event with timestamp $t_{\text{rel}}$ occurring between a `dispatcher_sleep_start` ($t_{\text{start}}$) and `dispatcher_sleep_end` ($t_{\text{end}}$) with matching `sleep_id`:
  $$T_{\text{sleep-lag}} = t_{\text{end}} - t_{\text{rel}}$$
- **Awake vs. Asleep Release Categorization:** Every `worker_slot_released` is classified as:
  - $\text{RELEASE\_DURING\_SLEEP}$ if $\exists \text{ sleep interval } [t_{\text{start}}, t_{\text{end}}] \text{ containing } t_{\text{rel}}$.
  - $\text{RELEASE\_WHILE\_AWAKE}$ otherwise.
  The proportion of releases occurring during sleep will be reported explicitly.
- **Replenishment Lag Matching:** Evaluated via FIFO pairing of chronological `worker_slot_released` events with subsequent `dispatcher_task_submitted` events during queue-positive periods:
  $$T_{\text{replenish-lag}} = t_{\text{dispatcher-task-submitted}, k} - t_{\text{worker-slot-released}, k}$$
- **Loop Overhead:**
  $$T_{\text{loop-overhead}} = t_{\text{dispatcher-dequeue-start}} - t_{\text{dispatcher-sleep-end}}$$
- **Redis Dequeue Execution Duration:**
  $$T_{\text{dequeue-exec}} = t_{\text{redis-dequeue-complete}} - t_{\text{dispatcher-dequeue-start}}$$
- **Starvation Ratio:**
  $$R_{\text{starve}} = \frac{\text{Duration where } Q(t) > 0 \text{ AND } W(t) < c \text{ AND Dispatcher is Asleep}}{\text{Total Duration where } Q(t) > 0}$$

### 4.2 State Invariant Verification
The analysis harness must strictly verify that:
1. Reconstructed logical queue depth $Q(t) \ge 0$ at all sample points.
2. Active worker proxy count $W(t) \ge 0$ at all sample points.
3. Event balance at end of measured window: $\sum \text{enqueues} = \sum \text{dequeues} = \sum \text{acks}$.
4. Direct timestamp-by-timestamp comparison of internal `_active_tasks` vs. proxy $W(t)$.

---

## 5. Experimental Execution Campaign

1. **Paired Interleaved Design:**
   Three paired trials per workload, alternating condition:
   - Trial 1: Baseline Disabled (D1) $\to$ Trial 2: Instrumented Enabled (E1)
   - Trial 3: Baseline Disabled (D2) $\to$ Trial 4: Instrumented Enabled (E2)
   - Trial 5: Baseline Disabled (D3) $\to$ Trial 6: Instrumented Enabled (E3)
2. **Environment & State Hygiene:**
   - Database tables truncated between trials via `TRUNCATE tasks, task_attempts, task_results, outbox_events, task_events CASCADE;`.
   - Redis cleared between trials via `FLUSHDB`.
   - Warmup tasks submitted and completely drained prior to opening the measured burst window.
3. **Overhead & Regression Assurance:**
   - Full regression suite (224/224) and focused diagnostic unit tests must pass.
   - Non-blocking profiler queue ensures zero blocking I/O on dispatcher or worker critical paths.
