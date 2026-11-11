import { useCallback, useEffect, useRef, useState } from "react";
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
  formula: string;
  anchorNote: string;
}> = [
  {
    key: "outbox_dwell",
    title: "Outbox Dwell",
    formula: "outbox.published_at − task.created_at",
    anchorNote: "Window anchored on outbox publication timestamp (excludes queue & execution).",
  },
  {
    key: "queue_dwell",
    title: "Queue Dwell",
    formula: "attempt_1.started_at − outbox.published_at",
    anchorNote:
      "Initial attempt only; window anchored on first-attempt completion time (excludes outbox lag & RETRY_WAIT).",
  },
  {
    key: "submission_to_start",
    title: "Submission to Start",
    formula: "attempt.started_at − task.created_at",
    anchorNote:
      "Total pre-execution wait across all completed attempts (outbox dwell + broker wait).",
  },
  {
    key: "handler_duration",
    title: "Handler Duration",
    formula: "TaskAttempt.duration_ms",
    anchorNote:
      "Pure Python handler execution runtime per completed attempt (distinct from lease tenure).",
  },
  {
    key: "worker_lease_tenure",
    title: "Worker Lease Tenure",
    formula: "task.completed_at − final_attempt.started_at",
    anchorNote:
      "Attempt start to durable task completion (handler runtime + lease & PostgreSQL persistence overhead).",
  },
  {
    key: "end_to_end",
    title: "End-to-End",
    formula: "task.completed_at − task.created_at",
    anchorNote:
      "Ingestion to terminal task completion; window anchored on Task.completed_at.",
  },
];

function fmtMs(value: number | null): string {
  return value === null ? "—" : `${value.toFixed(1)} ms`;
}

export function MonitoringPage() {
  const { client, isAuthenticated, sessionEpoch, hasScope } = useAuth();
  const isOperator = hasScope("admin:operator");

  const [windowMinutes, setWindowMinutes] = useState(5);
  const windowMinutesRef = useRef(windowMinutes);
  windowMinutesRef.current = windowMinutes;

  const [summary, setSummary] = useState<MonitoringSummaryResponse | null>(null);
  const [loadedWindow, setLoadedWindow] = useState<number | null>(null);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setSummary(null);
    setLoadedWindow(null);
    setError(null);
  }, [sessionEpoch]);

  const fetchSummary = useCallback(async () => {
    const reqWindow = windowMinutes;
    setIsRefreshing(true);
    try {
      const res = await client.getMonitoringSummary(reqWindow);
      if (windowMinutesRef.current !== reqWindow) {
        return;
      }
      setSummary(res);
      setLoadedWindow(reqWindow);
      setError(null);
    } catch (err) {
      if (windowMinutesRef.current !== reqWindow) {
        return;
      }
      if (err instanceof ApiError && err.status === 401) {
        setSummary(null);
        setLoadedWindow(null);
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
    } finally {
      if (windowMinutesRef.current === reqWindow) {
        setIsRefreshing(false);
      }
    }
  }, [client, windowMinutes]);

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 2500,
    resetKey: `${sessionEpoch}:${windowMinutes}`,
    fetcher: fetchSummary,
  });

  const handleManualRefresh = () => {
    void fetchSummary().catch(() => {
      // Errors & 429/503 backoff handled safely in fetchSummary
    });
  };

  const hasCurrentWindowSummary =
    summary !== null && loadedWindow === windowMinutes;

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
          <button
            type="button"
            className="btn"
            disabled={isRefreshing}
            onClick={handleManualRefresh}
          >
            {isRefreshing ? "Refreshing..." : "Refresh"}
          </button>
        </div>
      </div>

      {backoffSeconds ? (
        <div className="banner banner-warn" role="status">
          Telemetry polling backed off for {backoffSeconds}s.
          {hasCurrentWindowSummary ? " Showing last valid snapshot." : ""}
        </div>
      ) : null}

      {error ? (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      ) : null}

      {!hasCurrentWindowSummary && error === null ? (
        <div className="card" style={{ color: "var(--text-secondary)" }}>
          Loading monitoring summary for last {windowMinutes} minutes...
        </div>
      ) : null}

      {hasCurrentWindowSummary ? (
        <>
          <section aria-label="Tenant Operational Overview">
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
                <div className="kpi-sub tabular">
                  Window: {summary.window_minutes}m ({summary.window_seconds}s) •{" "}
                  {new Date(summary.timestamp_utc).toLocaleTimeString()}
                </div>
              </div>
            </div>
          </section>

          <section
            className="card"
            aria-label="Separated Lifecycle Latencies"
          >
            <h2 className="card-title">
              Separated Lifecycle Latencies (Tenant-Scoped Completed Populations)
            </h2>
            <p
              style={{
                margin: "0 0 12px",
                fontSize: 12.5,
                color: "var(--text-secondary)",
              }}
            >
              Each row reflects its own completed observation population and window
              anchor (do not compare counts directly across stages).{" "}
              <strong>Handler Duration</strong> (pure handler time) is reported
              separately from <strong>Worker Lease Tenure</strong> (includes lease &amp;
              terminal persistence overhead).
            </p>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Lifecycle Stage &amp; Population</th>
                    <th>Samples / Min Recommended</th>
                    <th>p50</th>
                    <th>p95</th>
                    <th>p99</th>
                    <th>Evidence Sufficiency</th>
                  </tr>
                </thead>
                <tbody>
                  {STAGE_LABELS.map((stage) => {
                    const metric: LatencyPercentileSummary =
                      summary.latencies_ms[stage.key];
                    const isEmpty = metric.sample_count === 0;
                    const isLowSample =
                      !isEmpty && !metric.sufficient_sample_size;
                    const quantileStyle = isLowSample
                      ? { color: "var(--text-muted)" }
                      : undefined;

                    return (
                      <tr key={stage.key}>
                        <td>
                          <strong>{stage.title}</strong>{" "}
                          <span
                            className="mono"
                            style={{ fontSize: 11, color: "var(--text-secondary)" }}
                          >
                            ({stage.formula})
                          </span>
                          <div style={{ fontSize: 11.5, color: "var(--text-muted)" }}>
                            {stage.anchorNote}
                          </div>
                          <div
                            className="mono"
                            style={{ fontSize: 11, color: "var(--text-muted)" }}
                          >
                            Population: {metric.population} • {metric.method}
                          </div>
                        </td>
                        <td className="tabular">
                          <strong>{metric.sample_count}</strong>{" "}
                          <span style={{ color: "var(--text-muted)", fontSize: 12 }}>
                            / min {metric.minimum_recommended_samples}
                          </span>
                        </td>
                        <td className="tabular" style={quantileStyle}>
                          {fmtMs(metric.p50_ms)}
                        </td>
                        <td className="tabular" style={quantileStyle}>
                          {fmtMs(metric.p95_ms)}
                        </td>
                        <td className="tabular" style={quantileStyle}>
                          {fmtMs(metric.p99_ms)}
                        </td>
                        <td>
                          {isEmpty ? (
                            <span className="pill pill-neutral">No observations</span>
                          ) : metric.sufficient_sample_size ? (
                            <span className="pill pill-healthy">
                              Sufficient (&ge;{metric.minimum_recommended_samples})
                            </span>
                          ) : (
                            <div>
                              <span className="pill pill-degraded">
                                Low N (&lt;{metric.minimum_recommended_samples})
                              </span>
                              <div
                                style={{
                                  fontSize: 11,
                                  color: "var(--text-muted)",
                                  marginTop: 4,
                                }}
                              >
                                Valid metric; limited quantile evidence
                              </div>
                            </div>
                          )}
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </section>

          {!isOperator ? (
            <section className="card" aria-label="Operator Telemetry Boundary">
              <h2 className="card-title">
                Cluster Saturation &amp; Outbox (Operator Scope Only)
              </h2>
              <div className="banner banner-info" role="note">
                <span>
                  <strong>Authorization Boundary:</strong> Your API key is limited to
                  tenant-scoped telemetry (<span className="mono">{summary.tenant_id}</span>
                  ). Global queue depths, outbox backlog, worker slot capacity, and DB
                  pool metrics require the <span className="mono">admin:operator</span>{" "}
                  scope.
                </span>
              </div>
            </section>
          ) : summary.cluster === null ? (
            <section className="card" aria-label="Operator Cluster Telemetry">
              <h2 className="card-title">
                Cluster Saturation &amp; Outbox (Operator Scope)
              </h2>
              <p style={{ margin: 0, color: "var(--text-secondary)" }}>
                Cluster telemetry is unavailable in this snapshot (
                <span className="mono">cluster: null</span>) — no values fabricated.
              </p>
            </section>
          ) : (
            <section className="card" aria-label="Operator Cluster Telemetry">
              <h2 className="card-title">
                Cluster Saturation &amp; Outbox (Operator Scope)
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
          )}
        </>
      ) : null}
    </>
  );
}
