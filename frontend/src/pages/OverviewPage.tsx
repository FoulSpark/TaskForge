import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { ApiError } from "../api/client";
import type {
  OperatorSystemStatsResponse,
  TaskItem,
  TenantStatsResponse,
} from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { PriorityBadge, StatusBadge } from "../components/StatusBadge";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

export function OverviewPage() {
  const { client, isAuthenticated, sessionEpoch, hasScope } = useAuth();
  const isOperator = hasScope("admin:operator");

  const [stats, setStats] = useState<TenantStatsResponse | null>(null);
  const [recentTasks, setRecentTasks] = useState<TaskItem[]>([]);
  const [operatorStats, setOperatorStats] = useState<OperatorSystemStatsResponse | null>(
    null,
  );
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    setStats(null);
    setRecentTasks([]);
    setOperatorStats(null);
    setError(null);
  }, [sessionEpoch]);

  const fetchOverview = useCallback(async () => {
    try {
      const [tenantStats, taskList] = await Promise.all([
        client.getStats(),
        client.listTasks({ limit: 8, offset: 0 }),
      ]);
      setStats(tenantStats);
      setRecentTasks(taskList.items);

      if (isOperator) {
        const sysStats = await client.getOperatorSystemStats();
        setOperatorStats(sysStats);
      } else {
        setOperatorStats(null);
      }
      setError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        setStats(null);
        setRecentTasks([]);
        setOperatorStats(null);
        throw err;
      }
      if (err instanceof ApiError) {
        setError(err.message);
        if (err.status === 429 || err.status === 503) {
          throw err;
        }
      } else {
        setError("Failed to load overview metrics.");
      }
    }
  }, [client, isOperator]);

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 2500,
    resetKey: sessionEpoch,
    fetcher: fetchOverview,
  });

  const t = stats?.tasks;

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Overview</h1>
          <p className="page-subtitle">
            Tenant-scoped execution counts and recent workloads for{" "}
            <strong className="mono">{stats?.tenant_id ?? "—"}</strong>
          </p>
        </div>
        <div style={{ display: "flex", gap: 8 }}>
          <Link to="/submit" className="btn btn-primary">
            + Submit Task
          </Link>
          <Link to="/tasks" className="btn">
            View All Tasks
          </Link>
        </div>
      </div>

      {backoffSeconds ? (
        <div className="banner banner-warn" role="status">
          Polling backed off for {backoffSeconds}s due to API rate limiting or degraded
          dependency state.
        </div>
      ) : null}

      {error ? (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      ) : null}

      <div className="kpi-grid">
        <div className="kpi-card">
          <div className="kpi-label">Total Tasks</div>
          <div className="kpi-value">{t?.total ?? "—"}</div>
          <div className="kpi-sub">Scoped to tenant</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Running / Queued</div>
          <div className="kpi-value">
            {t ? `${t.running} / ${t.queued + t.pending}` : "—"}
          </div>
          <div className="kpi-sub">Active execution pipeline</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Succeeded</div>
          <div className="kpi-value">{t?.succeeded ?? "—"}</div>
          <div className="kpi-sub">Completed cleanly</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Failed / DLQ</div>
          <div className="kpi-value">
            {t ? `${t.failed + t.dead_letter} (${t.retry_wait} retrying)` : "—"}
          </div>
          <div className="kpi-sub">Terminal failures & retries</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Avg Handler Duration</div>
          <div className="kpi-value">
            {stats ? `${stats.performance.avg_execution_ms.toFixed(1)} ms` : "—"}
          </div>
          <div className="kpi-sub">Successful attempts</div>
        </div>
      </div>

      {isOperator && operatorStats ? (
        <section className="card">
          <h2 className="card-title">Operator Cluster Overview (System Scope)</h2>
          <div className="kpi-grid">
            <div className="kpi-card">
              <div className="kpi-label">Global Queue Depth</div>
              <div className="kpi-value">{operatorStats.performance.queue_depth_total}</div>
              <div className="kpi-sub">
                C:{operatorStats.performance.queue_depth_by_priority.critical ?? 0} H:
                {operatorStats.performance.queue_depth_by_priority.high ?? 0} M:
                {operatorStats.performance.queue_depth_by_priority.medium ?? 0} L:
                {operatorStats.performance.queue_depth_by_priority.low ?? 0}
              </div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Active Workers</div>
              <div className="kpi-value">{operatorStats.workers.active_count}</div>
              <div className="kpi-sub">Heartbeating worker processes</div>
            </div>
            <div className="kpi-card">
              <div className="kpi-label">Global Tasks Total</div>
              <div className="kpi-value">{operatorStats.tasks.total}</div>
              <div className="kpi-sub">Across all tenants</div>
            </div>
          </div>
        </section>
      ) : null}

      <section className="card">
        <div className="page-header" style={{ marginBottom: 12 }}>
          <h2 className="card-title" style={{ margin: 0 }}>
            Recent Tenant Tasks
          </h2>
          <Link to="/tasks" className="btn btn-sm">
            Explore All
          </Link>
        </div>

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Task</th>
                <th>Type</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Retries</th>
                <th>Created</th>
              </tr>
            </thead>
            <tbody>
              {recentTasks.length === 0 ? (
                <tr>
                  <td colSpan={6} style={{ color: "var(--text-muted)" }}>
                    No tasks submitted for this tenant yet.
                  </td>
                </tr>
              ) : (
                recentTasks.map((task) => (
                  <tr key={task.id}>
                    <td>
                      <Link
                        to={`/tasks/${task.id}`}
                        style={{ fontWeight: 600, color: "var(--accent-primary)" }}
                      >
                        {task.task_name}
                      </Link>
                      <div className="mono" style={{ fontSize: 11, color: "var(--text-muted)" }}>
                        {task.id}
                      </div>
                    </td>
                    <td className="mono">{task.task_type}</td>
                    <td>
                      <StatusBadge status={task.status} />
                    </td>
                    <td>
                      <PriorityBadge priority={task.priority} />
                    </td>
                    <td className="tabular">
                      {task.retry_count} / {task.max_retries}
                    </td>
                    <td className="tabular">
                      {new Date(task.created_at).toLocaleString()}
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>
        </div>
      </section>
    </>
  );
}

