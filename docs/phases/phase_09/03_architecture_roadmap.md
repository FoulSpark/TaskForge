# G9.0.4 — Phase 9 Architecture Roadmap & Dependency Order

## 1. Roadmap Principles

Phase 9 transforms TaskForge from an experimentally verified reliability core (`v0.8.0-phase8-freeze`) into a fully diagnosable, secure, and operationally credible platform.

```mermaid
flowchart TD
    G90["G9.0: Baseline & Scope Lock (da27047)\n• Provenance, Environment, Threat Model\n• Pre-hardening Latency & Throughput Baseline"]
    G91["G9.1: Security Hardening\n• API Key AuthN & Scope RBAC\n• Tenant Isolation Enforcement\n• Redis Token-Bucket Rate Limiting\n• Secret Masking & Error Sanitization"]
    G92["G9.2: Observability & Health\n• Prometheus Metrics Exporter (:9090)\n• Lifecycle Latency Histograms\n• Two-Stage /healthz (Live vs Ready)\n• Structured Tracing & Correlation IDs"]
    G93["G9.3: Deployment Reproducibility\n• Production Docker Compose Stack\n• Zero-Downtime Migration Procedures\n• Cold Bootstrap & Disaster Recovery Drill"]
    G94["G9.4: Performance Characterization\n• Post-Hardening Throughput & Latency (p50/p95/p99)\n• Sustained Load & Queue Lag Analysis\n• Saturation Limits & Pool Exhaustion Recovery"]
    G95["G9.5: Public Architecture Package\n• Engineering Whitepaper & Invariant Proofs\n• Trade-off Records & Failure Mode Analysis\n• Reproducible Public Benchmark Suite"]

    G90 --> G91
    G91 --> G92
    G92 --> G93
    G93 --> G94
    G94 --> G95
```

---

## 2. Dependency Order Justification

1. **Why G9.0 First?** Baseline measurements must be established *before* introducing auth, rate limiting, and telemetry middleware. This ensures performance regressions or latency overheads introduced by Phase 9.1–9.3 are quantitatively measurable against an immutable standard.
2. **Why Security (9.1) Before Observability (9.2)?** Establishing trust boundaries and tenant context early ensures that metrics, traces, and logs in 9.2 can be properly tagged with tenant, user, and security attributes without requiring retrofitting.
3. **Why Observability (9.2) Before Deployment & Performance (9.3 & 9.4)?** Performance profiling requires accurate instrumentation (queue depth gauges, latency histograms, worker saturation counters). Running benchmarks without telemetry leaves bottlenecks opaque.
4. **Why Deployment (9.3) Before Final Performance Characterization (9.4)?** Performance characterization must reflect the actual reproducible deployment topology (containerized networking, connection pools, CPU limits), not an ad-hoc local script.
5. **Why Architecture Presentation (9.5) Last?** Public documentation, whitepapers, and reproducible demo scripts must reflect measured and verified operational realities.

---

## 3. Scope Exclusions & Non-Goals

* **Non-Goal 1: Universal Exactly-Once Delivery:** TaskForge maintains at-least-once delivery with terminal duplicate fencing. Phase 9 does not attempt two-phase commit with external arbitrary side-effect systems.
* **Non-Goal 2: Complex Distributed Tracing Backends:** OpenTelemetry protocol will be supported, but TaskForge will not bundle distributed tracing clusters (e.g. Jaeger / Zipkin) into its default runtime footprint; standard Prometheus + structured JSON logs serve as core telemetry.
* **Non-Goal 3: Distributed Multi-Region Consensus:** TaskForge remains an active-passive / standalone cluster system backed by PostgreSQL and Redis. Multi-region cross-datacenter consensus is explicitly out of scope for Phase 9.

