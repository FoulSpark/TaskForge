# Gate G9.3 — Stage 2 Phase 2C Diagnostic Rebaselining & Sequential Dequeue Investigation Protocol

**Milestone:** Gate G9.3 — Stage 2 Phase 2C (Diagnostic Target-Lock Investigation Proposal)  
**Author:** TaskForge Platform Hardening Team  
**Date:** 2026-11-10  
**Status:** Proposal Submitted for Architectural Review  
**Production Base Commit:** `a89858b`  
**Frozen Baseline Tag:** `v0.8.0-phase8-freeze` (`da27047f56fe2eec75acdb8502f505542cf61202`)  
**Workload Definitions Hash (SHA-256):** `04f2306538868a41514c19f0958e04bf0b0586b3a83989e115231a32e50f0110`  
**Scope Boundary:** Strictly Diagnostic Rebaselining. Zero batch dequeue implementation. Zero changes to queue contracts, lease fencing, worker concurrency, or PostgreSQL/Redis authoritative state boundaries.

---

## 1. Executive Summary & Context

With the formal approval and production adoption of Gate G9.3 Stage 2 Phase 2B (`a89858b`), the dispatcher scheduling sleep lag was reduced by $94.9\% - 98.7\%$ (dropping $T_{\text{sleep-lag}}$ from $\sim 24-26\,\text{ms}$ down to $0.35-1.21\,\text{ms}$), and queue starvation collapsed by $86.4\% - 93.5\%$ (dropping $R_{\text{starve}}$ from $25\%-31\%$ down to $1.7\%-4.0\%$).

Because dispatcher scheduling dynamics have fundamentally changed, historical Stage 1B and Phase 2A timing distributions can no longer serve as an accurate bottleneck attribution profile. 

The objective of Phase 2C is **not** to implement batch dequeue. Rather, Phase 2C is a **diagnostic target-lock investigation** to:
1. Establish a post-adoption diagnostic baseline with event-driven wake-up active.
2. Measure client-observed Redis dequeue round-trip ($T_{\text{dequeue-exec}}$) and remaining slot replenishment latency ($T_{\text{replenish-lag}}$).
3. Measure Redis command rates and dispatcher thread utilization.
4. Empirically determine whether sequential single-element dequeue limits sustained throughput under standard and elevated concurrency levels ($c = 4, 10, 20$).
5. Only then evaluate whether batch dequeue warrants an isolated experimental trial.

---

## 2. Operational Hypotheses & Pre-Registered Decision Criteria

The diagnostic investigation will test three competing operational hypotheses:

### Hypothesis H2-A: Sequential Dequeue Round-Trip Dominates Remaining Replenishment
- **Mechanism:** With polling sleep lag eliminated, client-observed Redis round-trip latency ($T_{\text{dequeue-exec}}$) becomes the primary component of replenishment delay ($T_{\text{replenish-lag}}$), delaying worker slot replenishment when multiple slots become free simultaneously.
- **Pre-Registered Decision Criteria:** Supported if:
  1. $T_{\text{dequeue-exec}}$ accounts for **$> 40\%$** of total replenishment lag ($T_{\text{replenish-lag}}$).
  2. Sequential dequeue delay demonstrably bounds sustained throughput below worker capacity limits under positive queue depth ($Q(t) > 0$).
  3. Starvation ratio $R_{\text{starve}}$ remains elevated due to dequeue serialization.

### Hypothesis H2-B: Broker Latency is Non-Dominant / Non-Bottleneck
- **Mechanism:** Redis single-element dequeue round-trip ($1.5 - 4.3\,\text{ms}$) is negligible relative to worker execution time and database lease/transaction overhead. Sequential dequeue supplies tasks faster than workers consume them, leaving dispatcher utilization low and worker occupancy high ($W \approx c$).
- **Pre-Registered Decision Criteria:** Supported if:
  1. $T_{\text{dequeue-exec}}$ accounts for **$< 20\%$** of task lifecycle or worker execution duration.
  2. Worker occupancy remains saturated ($W(t) \ge c$ for $> 75\%$ of queue-positive time).
  3. Dispatcher loop duty cycle (fraction of time spent actively executing dequeue vs. waiting on capacity/queue) remains $< 25\%$.
  4. *Conclusion if Supported:* Batch dequeue is rejected as an unnecessary optimization that adds complexity without throughput gains.

### Hypothesis H2-C: Concurrency-Scaled Serialization Bottleneck
- **Mechanism:** Sequential dequeue is efficient at low concurrency ($c=4$), but creates serialized queuing delay at higher concurrency ($c=10, 20$) because $N$ released slots require $N$ sequential network round-trips to replenish.
- **Pre-Registered Decision Criteria:** Supported if:
  1. At $c=4$, replenishment lag is low and non-blocking, but at $c=20$, replenishment lag scales linearly with $c$ ($T_{\text{replenish-lag}} \propto c$).
  2. Dispatcher loop duty cycle exceeds $> 60\%$ at $c=20$, indicating dispatcher thread saturation.

---

## 3. Post-Adoption Measurement Framework & Diagnostic Metrics

The measurement framework relies on the existing, validated 16-event monotonic profiler contract:

1. **Client-Observed Dequeue Round-Trip ($T_{\text{dequeue-exec}}$):**
   $$T_{\text{dequeue-exec}} = t_{\text{redis\_dequeue\_complete}} - t_{\text{dispatcher\_dequeue\_start}}$$
   Directly captures client round-trip latency including client Redis driver framing, network serialization, and Redis Lua execution.
2. **Remaining Slot Replenishment Lag ($T_{\text{replenish-lag}}$):**
   $$T_{\text{replenish-lag}} = t_{\text{dispatcher\_task\_submitted}} - t_{\text{worker\_slot\_released}}$$
   Measures the total latency from worker completion to the next task's dispatch into an executor slot.
3. **Dispatcher Utilization / Duty Cycle ($U_{\text{disp}}$):**
   $$U_{\text{disp}} = \frac{\sum (T_{\text{dequeue-exec}} + T_{\text{loop-overhead}})}{T_{\text{queue-positive-wall-clock}}}$$
   Quantifies the proportion of time the dispatcher thread spends actively executing work versus waiting on capacity (`_capacity_event.wait`) or broker dequeue timeout.
4. **Redis Command Rate & Broker Contention ($R_{\text{redis}}$):**
   Redis INFO / commandstats metrics capturing dequeue commands per second and command processing latency.
5. **Worker Capacity Occupancy Ratio ($S_{\text{work}}$):**
   Fraction of queue-backed duration where active worker count equals configured concurrency ($W(t) = c$).

---

## 4. Evaluation Matrix & Workload Parameters

To rigorously test both baseline throughput and concurrency scaling, the evaluation plan specifies:

| Run Series | Workload | Tasks ($n$) | Concurrency ($c$) | Condition | Primary Objective |
| :--- | :--- | :---: | :---: | :--- | :--- |
| **Series 1** | W-01 (`noop`) | 500 | 10 | Post-Adoption Baseline | Measure post-adoption $T_{\text{dequeue-exec}}$ and $U_{\text{disp}}$ |
| | W-02 (`prime`, limit 1000) | 200 | 4 | Post-Adoption Baseline | Measure compute-bound post-adoption baseline |
| | W-03 (`io`, 10 KB) | 200 | 10 | Post-Adoption Baseline | Measure I/O-bound post-adoption baseline |
| **Series 2** | W-01 (`noop`) | 500 | 4 | Concurrency Scaling | Measure scaling at low concurrency ($c=4$) |
| | W-01 (`noop`) | 500 | 10 | Concurrency Scaling | Measure scaling at medium concurrency ($c=10$) |
| | W-01 (`noop`) | 500 | 20 | Concurrency Scaling | Measure scaling at high concurrency ($c=20$) |

Each trial series will execute 3 independent trials with full database truncation and Redis flushing between every trial.

---

## 5. Decision Rules for Phase 2C Target-Lock

At the conclusion of the diagnostic campaign:
1. **If H2-B is supported:** Phase 2C will conclude with a formal finding that single-element sequential dequeue is **not** a material bottleneck, and batch dequeue will be formally dismissed to preserve code simplicity and lease safety.
2. **If H2-A or H2-C is supported:** A targeted, isolated evaluation of bounded batch dequeue (e.g. prefetching up to available free capacity $k = c - W$) will be defined and submitted for formal gate review before any implementation begins.
3. **Authoritative Invariant Lock:** Under no circumstances will batching be implemented without explicit prior gate authorization.
