import { useCallback, useEffect, useState } from "react";
import { ApiError } from "../api/client";
import type {
  LatencyPercentileSummary,
  MonitoringSummaryResponse,
} from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

const STAGE_LABELS: Array<{
  key: keyof MonitoringSummaryResponse["latencies_ms"];
  title: string;
  definition: string;
}> = [
  {
    key: "outbox_dwell",
    title: "Outbox Dwell",
    definition: "published_at - created_at (transactional outbox dispatch lag)",
  },
  {
    key: "queue_dwell",
    title: "Queue Dwell",
    definition: "attempt_1.started_at - published_at (broker queue residence)",
  },
  {
    key: "submission_to_start",
    title: "Submission to Start",
    definition: "attempt.started_at - task.created_at",
  },
  {
    key: "handler_duration",
    title: "Handler Duration",
    definition: "Recorded task handler execution time (completed attempts only)",
  },
  {
    key: "worker_lease_tenure",
    title: "Worker Lease Tenure",
    definition: "task.completed_at - final_attempt.started_at",
  },
  {
    key: "end_to_end",
    title: "End-to-End",
    definition: "task.completed_at - task.created_at (completed tasks only)",
  },
];

function fmtMs(value: number | null): string {
  return value === null ? "—" : `${value.toFixed(1)} ms`;
}

export function MonitoringPage() {
  const { client, isAuthenticated, sessionEpoch } = useAuth();
  const [windowMinutes, setWindowMinutes] = useState(5);
  const [summary, setSummary] = useState<MonitoringSummaryResponse | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSummary(null);
    setError(null);
  }, [sessionEpoch]);

  const fetchSummary = useCallback(async () => {
    try {
      const res = await client.getMonitoringSummary(windowMinutes);
      setSummary(res);
      setError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        setSummary(null);
        throw err;
      }
      if (err instanceof ApiError) {
        setError(err.message);
        if (err.status === 429 || err.status === 503) {
          throw err;
        }
      } else {
        setError("Failed to fetch monitoring summary.");
      }
    }
  }, [client, windowMinutes]);

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 2500,
    resetKey: `${sessionEpoch}:${windowMinutes}`,
    fetcher: fetchSummary,
  });

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">System Monitoring</h1>
          <p className="page-subtitle">
            Rolling-window lifecycle telemetry from{" "}
            <span className="mono">/api/v1/monitoring/summary</span>
          </p>
        </div>

        <div style={{ display: "flex", alignItems: "center", gap: 8 }}>
          <label htmlFor="window-select" style={{ fontSize: 13, fontWeight: 600 }}>
            Window:
          </label>
          <select
            id="window-select"
            className="select"
            style={{ width: "auto" }}
            value={windowMinutes}
            onChange={(e) => setWindowMinutes(Number(e.target.value))}
          >
            <option value={5}>Last 5 minutes</option>
            <option value={15}>Last 15 minutes</option>
            <option value={60}>Last 60 minutes</option>
          </select>
        </div>
      </div>

      {backoffSeconds ? (
        <div className="banner banner-warn" role="status">
          Telemetry polling backed off for {backoffSeconds}s.
        </div>
      ) : null}

      {error ? (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      ) : null}

      {summary ? (
        <>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Infrastructure Health</div>
              <div className="kpi-value" style={{ fontSize: 18 }}>
                <span
                  className={`pill ${
                    summary.health.overall === "healthy"
                      ? "pill-healthy"
                      : "pill-degraded"
                  }`}
                >
                  {summary.health.overall.toUpperCase()}
                </span>
              </div>
              <div className="kpi-sub">
                DB: {summary.health.database} • Redis: {summary.health.redis}
              </div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Completed In Window</div>
              <div className="kpi-value">{summary.throughput.observation_count}</div>
              <div className="kpi-sub">
                {summary.throughput.tasks_per_second.toFixed(2)} tasks/sec (
                {summary.throughput.tasks_per_minute.toFixed(1)}/min)
              </div>
            </div>

            <div className="kpi-card">
              <div className="kpi-label">Tenant Scope</div>
              <div className="kpi-value mono" style={{ fontSize: 18 }}>
                {summary.tenant_id}
              </div>
              <div className="kpi-sub">
                Window: {summary.window_minutes}m ({summary.window_seconds}s)
              </div>
            </div>
          </div>

          <section className="card">
            <h2 className="card-title">
              Separated Lifecycle Latencies (Completed Populations)
            </h2>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Lifecycle Stage</th>
                    <th>Samples</th>
                    <th>p50</th>
                    <th>p95</th>
                    <th>p99</th>
                    <th>Statistical Confidence</th>
                  </tr>
                </thead>
                <tbody>
                  {STAGE_LABELS.map((stage) => {
                    const metric: LatencyPercentileSummary =
                      summary.latencies_ms[stage.key];
                    return (
                      <tr key={stage.key}>
                        <td>
                          <strong>{stage.title}</strong>
                          <div style={{ fontSize: 11.5, color: "var(--text-muted)" }}>
                            {stage.definition}
                          </div>
                        </td>
                        <td className="tabular">{metric.sample_count}</td>
                        <td className="tabular">{fmtMs(metric.p50_ms)}</td>
                        <td className="tabular">{fmtMs(metric.p95_ms)}</td>
                        <td className="tabular">{fmtMs(metric.p99_ms)}</td>
                        <td>
                          {metric.sample_count === 0 ? (
                            <span className="pill pill-neutral">No observations</span>
                          ) : metric.sufficient_sample_size ? (
                            <span className="pill pill-healthy">
                              Sufficient (&ge;{metric.minimum_recommended_samples})
                            </span>
                          ) : (
                            <span className="pill pill-degraded">
                              Low N (&lt;{metric.minimum_recommended_samples})
                            </span>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>

          {summary.cluster ? (
            <section className="card">
              <h2 className="card-title">
                Cluster Saturation & Outbox (Operator Scope)
              </h2>
              <div className="kpi-grid">
                <div className="kpi-card">
                  <div className="kpi-label">Pending Outbox Events</div>
                  <div className="kpi-value">
                    {summary.cluster.outbox_pending_events}
                  </div>
                  <div className="kpi-sub">
                    Oldest lag: {summary.cluster.outbox_lag_seconds.toFixed(2)}s
                  </div>
                </div>
                <div className="kpi-card">
                  <div className="kpi-label">Active Workers</div>
                  <div className="kpi-value">
                    {summary.cluster.workers.active_count}
                  </div>
                  <div className="kpi-sub">
                    Running {summary.cluster.workers.active_tasks} /{" "}
                    {summary.cluster.workers.total_concurrency} slots
                  </div>
                </div>
                <div className="kpi-card">
                  <div className="kpi-label">DB Pool</div>
                  <div className="kpi-value">
                    {summary.cluster.db_pool.checked_out} out /{" "}
                    {summary.cluster.db_pool.idle} idle
                  </div>
                  <div className="kpi-sub">
                    Overflow: {summary.cluster.db_pool.overflow}
                  </div>
                </div>
                <div className="kpi-card">
                  <div className="kpi-label">Queue Depth by Priority</div>
                  <div className="kpi-sub" style={{ fontSize: 13 }}>
                    C:{summary.cluster.queue_depth_by_priority.critical ?? 0} • H:
                    {summary.cluster.queue_depth_by_priority.high ?? 0} • M:
                    {summary.cluster.queue_depth_by_priority.medium ?? 0} • L:
                    {summary.cluster.queue_depth_by_priority.low ?? 0}
                  </div>
                </div>
              </div>
            </section>
          ) : null}
        </>
      ) : null}
    </>
  );
}

