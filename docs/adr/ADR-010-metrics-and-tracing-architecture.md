# ADR-010: Metrics and Tracing Architecture

## Status
Accepted

## Context
Operating a distributed task processing platform requires deep visibility into task ingestion, queue lag, worker utilization, latency percentiles, and failure root causes.

## Problem
Relying solely on unstructured stdout logs makes it impossible to detect tail latency degradation, queue backpressure bottlenecks, or correlation between an API request and background worker execution.

## Alternatives Considered
1. **Cloud-specific monitoring (AWS CloudWatch / Datadog):** Vendor lock-in, non-reproducible in local-first open-source repositories.
2. **Prometheus + OpenTelemetry + Grafana:** Vendor-neutral, open-source industry standard. Standardized pull-based scraping for metrics, W3C correlation headers for distributed tracing, and visual dashboarding.

## Decision
We implement **Prometheus pull-based metrics (`/metrics`) combined with OpenTelemetry distributed correlation IDs and Grafana dashboard visualizations**.

## Advantages
- Standard Prometheus text format accessible without third-party accounts.
- Request correlation IDs (`X-Correlation-ID`) propagated across HTTP requests, structured JSON logs, attempt audits, and worker traces.
- Complete pre-configured Grafana dashboard JSON with 12 operational panels.

## Disadvantages
- Requires running Prometheus and Grafana containers in production/Docker environments.

## Validation Evidence
Verified in `tests/integration/test_observability.py`. `/metrics` and `/api/v1/stats` validated with zero high-cardinality label explosion.
