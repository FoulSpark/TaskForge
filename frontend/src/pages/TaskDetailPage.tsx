import { useCallback, useEffect, useState } from "react";
import { Link, useLocation, useParams } from "react-router-dom";
import { ApiError } from "../api/client";
import {
  canCancelStatus,
  canRetryStatus,
  computeElapsedMs,
  getLifecycleStateLabel,
  isImmutableTerminalStatus,
  isTerminalStatus,
  sanitizeMetadataForDisplay,
  sortAttemptsAsc,
  sortEventsChronological,
} from "../api/lifecycle";
import type { TaskItem } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { PriorityBadge, StatusBadge } from "../components/StatusBadge";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

function formatMsValue(ms: number | null): string {
  return ms !== null ? `${ms.toFixed(1)} ms` : "—";
}

export function TaskDetailPage() {
  const { taskId = "" } = useParams();
  const location = useLocation();
  const submissionNotice = (
    location.state as { submissionNotice?: string } | null
  )?.submissionNotice;
  const { client, principal, isAuthenticated, sessionEpoch, hasScope } = useAuth();
  const canWrite = hasScope("tasks:write");

  const [task, setTask] = useState<TaskItem | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [actionBusy, setActionBusy] = useState(false);

  useEffect(() => {
    setTask(null);
    setError(null);
  }, [taskId, sessionEpoch]);

  const fetchTask = useCallback(async () => {
    if (!taskId) return;
    try {
      const data = await client.getTask(taskId);
      setTask(data);
      setError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        setTask(null);
        throw err;
      }
      setError(err instanceof ApiError ? err.message : "Failed to load task details.");
      if (err instanceof ApiError && (err.status === 429 || err.status === 503)) {
        throw err;
      }
    }
  }, [client, taskId]);

  const isTerminal = task ? isTerminalStatus(task.status) : false;

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated && Boolean(taskId) && !isTerminal,
    intervalMs: 2000,
    resetKey: `${sessionEpoch}:${taskId}`,
    fetcher: fetchTask,
  });

  const handleAction = async (action: "cancel" | "retry") => {
    if (!task) return;
    setActionBusy(true);
    setError(null);
    try {
      const updated =
        action === "cancel"
          ? await client.cancelTask(task.id)
          : await client.retryTask(task.id);
      setTask(updated);
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        await fetchTask();
        setError(
          `State conflict (409): ${err.message}. Refreshed authoritative server state.`,
        );
      } else {
        setError(err instanceof ApiError ? err.message : `Failed to ${action} task.`);
      }
    } finally {
      setActionBusy(false);
    }
  };

  const attempts = task ? sortAttemptsAsc(task.attempts) : [];
  const events = task ? sortEventsChronological(task.events) : [];
  const latestAttempt = attempts.length > 0 ? attempts[attempts.length - 1] : null;
  const latestWorkerId = latestAttempt?.worker_id ?? "—";

  const handlerDurationMs =
    latestAttempt?.duration_ms !== null && latestAttempt?.duration_ms !== undefined
      ? latestAttempt.duration_ms
      : null;
  const workerLeaseTenureMs = task
    ? computeElapsedMs(latestAttempt?.started_at, task.completed_at)
    : null;
  const endToEndMs = task
    ? computeElapsedMs(task.created_at, task.completed_at)
    : null;

  const failedAttempts = attempts.filter(
    (att) => Boolean(att.error_message) || att.status === "FAILED" || att.status === "TIMEOUT",
  );
  const eventsWithDiagnostics = events.filter(
    (ev) => sanitizeMetadataForDisplay(ev.metadata_json) !== null,
  );
  const showFailureDiagnostics =
    failedAttempts.length > 0 ||
    task?.status === "FAILED" ||
    task?.status === "DEAD_LETTER";

  const sanitizedResultMetadata = sanitizeMetadataForDisplay(
    task?.result?.execution_metadata,
  );

  return (
    <>
      <div className="page-header">
        <div>
          <div style={{ marginBottom: 6 }}>
            <Link to="/tasks" style={{ fontSize: 13, color: "var(--accent-primary)" }}>
              ← Back to Task Explorer
            </Link>
          </div>
          <h1 className="page-title">{task?.task_name ?? "Task Inspection"}</h1>
          <p className="page-subtitle mono">{taskId}</p>
        </div>

        {task && canWrite ? (
          <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
            {canCancelStatus(task.status) ? (
              <button
                type="button"
                className="btn btn-danger"
                disabled={actionBusy}
                onClick={() => void handleAction("cancel")}
              >
                Cancel Task
              </button>
            ) : null}
            {canRetryStatus(task.status) ? (
              <button
                type="button"
                className="btn btn-primary"
                disabled={actionBusy}
                onClick={() => void handleAction("retry")}
              >
                Manual Retry (Re-queue)
              </button>
            ) : null}
            {isImmutableTerminalStatus(task.status) ? (
              <span className="pill pill-neutral">Immutable (Terminal)</span>
            ) : null}
          </div>
        ) : null}
      </div>

      {submissionNotice === "idempotent_replay" ? (
        <div className="banner banner-info" role="status">
          Idempotent replay (HTTP 200): Matched existing task for this idempotency key —
          no duplicate task was created.
        </div>
      ) : submissionNotice === "scheduled" ? (
        <div className="banner banner-info" role="status">
          Task persisted and scheduled for future execution (waiting until due).
        </div>
      ) : null}

      {backoffSeconds ? (
        <div className="banner banner-warn" role="status">
          Live task polling backed off for {backoffSeconds}s.
        </div>
      ) : null}

      {error ? (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      ) : null}

      {task ? (
        <>
          <section className="card" aria-label="Task-Level Summary">
            <div
              style={{
                display: "flex",
                flexWrap: "wrap",
                gap: 10,
                alignItems: "center",
                marginBottom: 16,
              }}
            >
              <StatusBadge status={task.status} />
              <PriorityBadge priority={task.priority} />
              <span className="pill pill-neutral mono">{task.task_type}</span>
              {!isImmutableTerminalStatus(task.status) && getLifecycleStateLabel(task) ? (
                <span className="pill pill-neutral">{getLifecycleStateLabel(task)}</span>
              ) : null}
              {!isTerminal ? (
                <span className="pill pill-neutral">Auto-refreshing</span>
              ) : null}
            </div>

            <div className="detail-grid">
              <div className="kv-item">
                <span className="kv-label">Tenant Scope</span>
                <span className="kv-val mono">{principal?.tenant_id ?? "—"}</span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Latest Worker ID</span>
                <span className="kv-val mono">{latestWorkerId}</span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Retries / Max</span>
                <span className="kv-val tabular">
                  {task.retry_count} / {task.max_retries}
                </span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Timeout</span>
                <span className="kv-val tabular">{task.timeout_seconds}s</span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Idempotency Key</span>
                <span className="kv-val mono">{task.idempotency_key ?? "—"}</span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Scheduled At</span>
                <span className="kv-val tabular">
                  {task.scheduled_at
                    ? new Date(task.scheduled_at).toLocaleString()
                    : "Immediate"}
                </span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Created At</span>
                <span className="kv-val tabular">
                  {new Date(task.created_at).toLocaleString()}
                </span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Started At</span>
                <span className="kv-val tabular">
                  {task.started_at ? new Date(task.started_at).toLocaleString() : "—"}
                </span>
              </div>
              <div className="kv-item">
                <span className="kv-label">Completed At</span>
                <span className="kv-val tabular">
                  {task.completed_at
                    ? new Date(task.completed_at).toLocaleString()
                    : "—"}
                </span>
              </div>
            </div>
          </section>

          <section className="card" aria-label="Execution Timing Breakdown">
            <h2 className="card-title">
              Execution Timing Breakdown (Attempt vs Task Scope)
            </h2>
            <div className="kpi-grid">
              <div className="kpi-card">
                <div className="kpi-label">Latest Handler Duration</div>
                <div className="kpi-value">{formatMsValue(handlerDurationMs)}</div>
                <div className="kpi-sub">
                  TaskAttempt.duration_ms (pure handler runtime)
                </div>
              </div>
              <div className="kpi-card">
                <div className="kpi-label">Worker Lease Tenure</div>
                <div className="kpi-value">{formatMsValue(workerLeaseTenureMs)}</div>
                <div className="kpi-sub">
                  Completed − final attempt started (incl. lease &amp; DB persistence overhead)
                </div>
              </div>
              <div className="kpi-card">
                <div className="kpi-label">End-to-End Task Elapsed</div>
                <div className="kpi-value">{formatMsValue(endToEndMs)}</div>
                <div className="kpi-sub">
                  Task completed − task ingested (full task-level lifecycle)
                </div>
              </div>
            </div>
          </section>

          {showFailureDiagnostics ? (
            <section
              className="card"
              aria-label="Failure Diagnostics"
              style={{ borderColor: "#fecaca" }}
            >
              <h2 className="card-title" style={{ color: "#991b1b" }}>
                Failure Diagnostics ({task.status})
              </h2>
              {failedAttempts.length === 0 ? (
                <p style={{ margin: 0, color: "var(--text-secondary)" }}>
                  Task is in {task.status}; check lifecycle events below for transition
                  details.
                </p>
              ) : (
                <div style={{ display: "flex", flexDirection: "column", gap: 10 }}>
                  {failedAttempts.map((att) => (
                    <div
                      key={att.id}
                      style={{
                        border: "1px solid #fecaca",
                        borderRadius: 8,
                        padding: 12,
                        background: "#fef2f2",
                      }}
                    >
                      <div
                        style={{
                          fontSize: 13,
                          fontWeight: 600,
                          color: "#991b1b",
                          marginBottom: 6,
                        }}
                      >
                        Attempt #{att.attempt_number} • Status:{" "}
                        <span className="mono">{att.status}</span> • Worker:{" "}
                        <span className="mono">{att.worker_id ?? "—"}</span>
                      </div>
                      <pre className="code-block">
                        {att.error_message ?? "No error message recorded on attempt."}
                      </pre>
                    </div>
                  ))}
                </div>
              )}
              {eventsWithDiagnostics.length > 0 ? (
                <div style={{ marginTop: 12, fontSize: 12.5, color: "var(--text-secondary)" }}>
                  Event diagnostics also recorded on {eventsWithDiagnostics.length}{" "}
                  lifecycle event(s) below.
                </div>
              ) : null}
            </section>
          ) : null}

          <section className="card" aria-label="Execution Result">
            <h2 className="card-title">Execution Result</h2>
            {task.result ? (
              <>
                <div style={{ marginBottom: 8, fontSize: 12, color: "var(--text-muted)" }}>
                  Result ID #{task.result.id} • Recorded{" "}
                  {new Date(task.result.created_at).toLocaleString()}
                </div>
                <pre className="code-block">
                  {JSON.stringify(task.result.return_value, null, 2)}
                </pre>
                {sanitizedResultMetadata ? (
                  <div style={{ marginTop: 12 }}>
                    <div
                      className="kv-label"
                      style={{ marginBottom: 6 }}
                    >
                      Execution Metadata
                    </div>
                    <pre className="code-block">
                      {JSON.stringify(sanitizedResultMetadata, null, 2)}
                    </pre>
                  </div>
                ) : null}
              </>
            ) : (
              <p style={{ margin: 0, color: "var(--text-muted)" }}>
                {isTerminal
                  ? "No result payload stored for this task."
                  : "Waiting for worker execution to produce a result..."}
              </p>
            )}
          </section>

          <section className="card" aria-label="Input Payload">
            <h2 className="card-title">Input Payload</h2>
            <pre className="code-block">{JSON.stringify(task.payload, null, 2)}</pre>
          </section>

          <section className="card" aria-label="Attempt History">
            <h2 className="card-title">Attempt History ({attempts.length})</h2>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Attempt</th>
                    <th>Worker ID</th>
                    <th>Status</th>
                    <th>Handler Duration</th>
                    <th>Started</th>
                    <th>Completed</th>
                    <th>Error</th>
                  </tr>
                </thead>
                <tbody>
                  {attempts.length === 0 ? (
                    <tr>
                      <td colSpan={7} style={{ color: "var(--text-muted)" }}>
                        No worker attempts recorded yet.
                      </td>
                    </tr>
                  ) : (
                    attempts.map((att) => (
                      <tr key={att.id}>
                        <td className="tabular">#{att.attempt_number}</td>
                        <td className="mono">{att.worker_id ?? "—"}</td>
                        <td className="mono">{att.status}</td>
                        <td className="tabular">
                          {formatMsValue(att.duration_ms)}
                        </td>
                        <td className="tabular">
                          {att.started_at
                            ? new Date(att.started_at).toLocaleString()
                            : "—"}
                        </td>
                        <td className="tabular">
                          {att.completed_at
                            ? new Date(att.completed_at).toLocaleString()
                            : "—"}
                        </td>
                        <td className="mono" style={{ color: "#b91c1c" }}>
                          {att.error_message ?? "—"}
                        </td>
                      </tr>
                    ))
                  )}
                </tbody>
              </table>
            </div>
          </section>

          <section className="card" aria-label="Lifecycle Events">
            <h2 className="card-title">Lifecycle Events ({events.length})</h2>
            <div className="table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Timestamp</th>
                    <th>Event</th>
                    <th>Transition</th>
                    <th>Metadata</th>
                  </tr>
                </thead>
                <tbody>
                  {events.length === 0 ? (
                    <tr>
                      <td colSpan={4} style={{ color: "var(--text-muted)" }}>
                        No lifecycle events recorded.
                      </td>
                    </tr>
                  ) : (
                    events.map((ev) => {
                      const cleanMeta = sanitizeMetadataForDisplay(ev.metadata_json);
                      return (
                        <tr key={ev.id}>
                          <td className="tabular">
                            {new Date(ev.created_at).toLocaleString()}
                          </td>
                          <td className="mono">{ev.event_type}</td>
                          <td className="mono">
                            {ev.from_state ?? "∅"} → {ev.to_state}
                          </td>
                          <td className="mono">
                            {cleanMeta ? JSON.stringify(cleanMeta) : "—"}
                          </td>
                        </tr>
                      );
                    })
                  )}
                </tbody>
              </table>
            </div>
          </section>
        </>
      ) : null}
    </>
  );
}

