# Stage 2 Diagnostic Protocol — Dispatcher Scheduling Efficiency Investigation

**Milestone:** Gate G9.3 — Stage 2 (Diagnostic Target-Lock Investigation)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Authorized Diagnostic Protocol (Pre-Implementation Plan)  
**Authorized Base Commit:** `61f398f`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Scope Boundary:** **Strictly Diagnostic Investigation.** No production optimizations, no changes to production semantics, and no worker architecture changes are authorized in this stage.

---

## 1. Investigation Objective & Primary Hypotheses

The Stage 1B empirical attribution campaign established that queue residence time accounts for $93.6\% - 98.5\%$ ($p50$) of task end-to-end turnaround latency, and that the worker pool was idle ($W = 0$) during $58\% - 78\%$ of queue-positive time. 

The objective of Stage 2 is to conduct a fine-grained diagnostic measurement of `WorkerDispatcher` internal scheduling to distinguish between four distinct operational hypotheses:

- **H1 — Polling Wake-up Delay:** The dispatcher remains asleep in its fixed $200\,\text{ms}$ polling sleep after one or more worker slots become available, delaying task acquisition.
- **H2 — Sequential Dequeue Overhead:** Single-task sequential dequeue and in-loop Python scheduling materially delay replenishing available worker slots.
- **H3 — Service-Time Limitation:** Workers remain continuously occupied because database transactions (lease acquisition, state transitions, terminal persistence) genuinely bound slot turnover.
- **H4 — Queue/Worker State Mismatch:** Client-observed logical queue depth and active worker proxies do not accurately reflect internal dispatcher capacity or Redis queue state.

---

## 2. Diagnostic Event Contract Extension

To empirically isolate each component without altering dispatcher behavior, the diagnostic profiler will record the following 5 internal dispatcher events in addition to the frozen 16 lifecycle events:

| Event Name | Emission Location | Purpose & Scope |
| :--- | :--- | :--- |
| `dispatcher_sleep_start` | `WorkerDispatcher._dispatch_loop()` immediately before `time.sleep(self.poll_interval)` | Captures the instant the dispatcher thread enters a sleep due to worker saturation (`_active_tasks >= concurrency`). |
| `dispatcher_sleep_end` | `WorkerDispatcher._dispatch_loop()` immediately after `time.sleep()` returns | Captures the instant the dispatcher wakes up from polling sleep. |
| `worker_slot_released` | `WorkerDispatcher._task_wrapper()` `finally:` block immediately after decrementing `_active_tasks` | Captures the exact instant a worker thread releases its slot in the pool. Records `active_tasks` remaining. |
| `dispatcher_dequeue_start` | `WorkerDispatcher._dispatch_loop()` immediately before `self.broker.dequeue()` | Captures the start of Redis dequeue invocation, enabling isolation of Redis dequeue execution duration from sleep delay. |
| `dispatcher_task_submitted` | `WorkerDispatcher._dispatch_loop()` immediately after `self._executor.submit(self._task_wrapper, task_msg)` | Captures the completion of slot replenishment and submission to the thread pool. |

All diagnostic events will be emitted using the existing non-blocking `record_diagnostic_event()` singleton, preserving zero-lock overhead and bounded memory guarantees.

---

## 3. Mathematical Definitions of Stage 2 Derived Intervals

The extended event stream enables precise calculation of each candidate delay component:

| Diagnostic Metric | Mathematical Definition | Physical Interpretation |
| :--- | :--- | :--- |
| **Wake-up Delay**<br>($T_{\text{sleep-lag}}$) | $t_{\text{dispatcher-sleep-end}} - t_{\text{worker-slot-released}}$<br>(for $t_{\text{sleep-start}} \le t_{\text{slot-released}} \le t_{\text{sleep-end}}$) | Time a worker slot sat empty and available while the dispatcher thread was asleep. |
| **Redis Dequeue Duration**<br>($T_{\text{dequeue-exec}}$) | $t_{\text{redis-dequeue-complete}} - t_{\text{dispatcher-dequeue-start}}$ | Pure client-observed Redis round-trip latency for executing the `ZPOPMAX` Lua script. |
| **Loop Scheduling Overhead**<br>($T_{\text{loop-overhead}}$) | $t_{\text{dispatcher-dequeue-start}} - t_{\text{dispatcher-sleep-end}}$ | In-process Python dispatcher overhead between wake-up and initiating dequeue. |
| **Slot-to-Dispatch Latency**<br>($T_{\text{replenish-lag}}$) | $t_{\text{dispatcher-task-submitted}} - t_{\text{worker-slot-released}}$ | Total interval from worker slot release until a new task is dispatched to the thread pool. |
| **Starvation Ratio**<br>($R_{\text{starve}}$) | $\frac{\text{Time with } Q > 0 \text{ and } W < c \text{ and Dispatcher Asleep}}{\text{Total Time with } Q > 0}$ | Fraction of queue-positive time where capacity was available but unutilized due to dispatcher sleep. |

---

## 4. Hypothesis Testing & Disentanglement Criteria

The empirical data will be evaluated against the following strict decision thresholds:

```
                            Stage 2 Diagnostic Campaign
                                         │
        ┌────────────────────────────────┴────────────────────────────────┐
        ▼                                                                 ▼
Is T_sleep-lag dominant?                                          Is T_dequeue-exec dominant?
(T_sleep-lag p50 > 50ms and R_starve > 30%)                       (T_dequeue-exec > 50ms)
        │                                                                 │
        ├─ YES ──> Confirms H1 (Polling Wake-up Delay)                    ├─ YES ──> Confirms Redis Dequeue Contention
        │          Target: Event-driven wake-up                           │
        │                                                                 ▼
        └─ NO ───> Evaluate Sequential Replenishment:            Is W >= c for > 80% of Q > 0?
                   Does sum(T_replenish) scale with c?                    │
                   ├─ YES ──> Confirms H2 (Sequential Overhead)           ├─ YES ──> Confirms H3 (Service-Time Bound)
                   │          Target: Batch Dequeue                       │          Target: DB write optimizations
                   └─ NO ───> Confirms H4 (State Mismatch)                └─ NO ───> Confirms Dispatcher Inefficiency
```

1. **Confirmation of H1 (Polling Wake-up Delay):** Confirmed if median wake-up delay $T_{\text{sleep-lag}} > 50\,\text{ms}$ and starvation ratio $R_{\text{starve}} > 30\%$, demonstrating that tasks wait because the dispatcher is sleeping.
2. **Confirmation of H2 (Sequential Dequeue Overhead):** Confirmed if $T_{\text{sleep-lag}}$ is low ($< 10\,\text{ms}$) but total replenishment time for $k$ available slots scales linearly as $k \times T_{\text{dequeue-exec}}$, leaving slots empty while serial dequeues occur.
3. **Confirmation of H3 (Service-Time Ceiling):** Confirmed if active worker occupancy $W(t) \ge c$ for $> 80\%$ of queue-positive time, proving workers are genuinely saturated and not starved by the dispatcher.
4. **Confirmation of H4 (Queue/State Mismatch):** Confirmed if dispatcher logs show `_active_tasks >= concurrency` when proxy worker occupancy $W(t) < c$, indicating internal state drift or stuck tasks.

---

## 5. Experimental Campaign Design

1. **Phased Isolation (No Bundling):**
   - **Phase 2A (Diagnostic Baseline):** Execute uninstrumented vs. dispatch-instrumented trials to measure baseline $T_{\text{sleep-lag}}$, $T_{\text{dequeue-exec}}$, and $R_{\text{starve}}$ across W-01, W-02 ($n=1,000$), and W-03.
   - **Phase 2B (Wake-up Mechanism Test):** Evaluate event-driven capacity wake-up (`threading.Event` set on task completion) in isolation, without modifying dequeue batching.
   - **Phase 2C (Batch Dequeue Evaluation):** Evaluate batch prefetching in isolation only after Phase 2B is complete, preventing confounded attribution.
2. **Statistical Rigor:**
   - 3 paired interleaved trials per workload.
   - Database table truncation and Redis flush between trials.
   - Full regression suite execution (224/224) before and after each phase.

---

## 6. Scope & Architecture Invariants

The following subsystem boundaries and contracts remain frozen and strictly protected:
- `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`) remains locked.
- Task state machine transitions and retry logic in `TaskRepository` remain untouched.
- Lease timeouts, lease tokens, and fencing protections remain untouched.
- Redis sorted set priority ordering (ZADD scores $1 \to 3,999,999,999,999$) and ACK delivery token contracts remain untouched.
- Transactional outbox polling boundaries and synchronous database writes remain untouched.
