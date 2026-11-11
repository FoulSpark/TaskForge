import { useCallback, useEffect, useState } from "react";
import { Link } from "react-router-dom";
import { ApiError } from "../api/client";
import {
  canCancelStatus,
  canRetryStatus,
  isImmutableTerminalStatus,
} from "../api/lifecycle";
import type { TaskItem, TaskStatus } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { PriorityBadge, StatusBadge } from "../components/StatusBadge";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

const PAGE_LIMIT = 20;
const STATUS_OPTIONS: TaskStatus[] = [
  "PENDING",
  "QUEUED",
  "RUNNING",
  "SUCCESS",
  "RETRY_WAIT",
  "FAILED",
  "DEAD_LETTER",
  "CANCELLED",
];

export function TaskExplorerPage() {
  const { client, isAuthenticated, sessionEpoch, hasScope } = useAuth();
  const canWrite = hasScope("tasks:write");

  const [statusFilter, setStatusFilter] = useState("");
  const [taskTypeFilter, setTaskTypeFilter] = useState("");
  const [priorityFilter, setPriorityFilter] = useState("");
  const [offset, setOffset] = useState(0);

  const [items, setItems] = useState<TaskItem[]>([]);
  const [total, setTotal] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [busyTaskId, setBusyTaskId] = useState<string | null>(null);

  useEffect(() => {
    setItems([]);
    setTotal(0);
    setHasMore(false);
    setError(null);
  }, [sessionEpoch]);

  const fetchTasks = useCallback(async () => {
    try {
      const res = await client.listTasks({
        status: statusFilter || undefined,
        task_type: taskTypeFilter.trim() || undefined,
        priority: priorityFilter || undefined,
        limit: PAGE_LIMIT,
        offset,
      });
      setItems(res.items);
      setTotal(res.total);
      setHasMore(res.has_more);
      setError(null);
    } catch (err) {
      if (err instanceof ApiError && err.status === 401) {
        setItems([]);
        setTotal(0);
        throw err;
      }
      if (err instanceof ApiError) {
        setError(err.message);
        if (err.status === 429 || err.status === 503) {
          throw err;
        }
      } else {
        setError("Failed to fetch tasks.");
      }
    }
  }, [client, statusFilter, taskTypeFilter, priorityFilter, offset]);

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 2500,
    resetKey: `${sessionEpoch}:${statusFilter}:${taskTypeFilter}:${priorityFilter}:${offset}`,
    fetcher: fetchTasks,
  });

  const handleAction = async (taskId: string, action: "cancel" | "retry") => {
    setBusyTaskId(taskId);
    setError(null);
    try {
      if (action === "cancel") {
        await client.cancelTask(taskId);
      } else {
        await client.retryTask(taskId);
      }
      await fetchTasks();
    } catch (err) {
      if (err instanceof ApiError && err.status === 409) {
        await fetchTasks();
        setError(
          `State conflict (409): ${err.message}. Synchronized authoritative task list.`,
        );
      } else {
        setError(err instanceof ApiError ? err.message : `Failed to ${action} task.`);
      }
    } finally {
      setBusyTaskId(null);
    }
  };

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Task Explorer</h1>
          <p className="page-subtitle">
            Filter, inspect, cancel, and retry tasks scoped to your tenant
          </p>
        </div>
        <Link to="/submit" className="btn btn-primary">
          + Submit Task
        </Link>
      </div>

      {backoffSeconds ? (
        <div className="banner banner-warn" role="status">
          Polling paused for {backoffSeconds}s due to API backpressure.
        </div>
      ) : null}

      {error ? (
        <div className="banner banner-error" role="alert">
          {error}
        </div>
      ) : null}

      <section className="card">
        <div className="form-grid-2" style={{ marginBottom: 14 }}>
          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-status">Status</label>
            <select
              id="filter-status"
              className="select"
              value={statusFilter}
              onChange={(e) => {
                setOffset(0);
                setStatusFilter(e.target.value);
              }}
            >
              <option value="">All Statuses</option>
              {STATUS_OPTIONS.map((st) => (
                <option key={st} value={st}>
                  {st}
                </option>
              ))}
            </select>
          </div>

          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-task-type">Task Type</label>
            <input
              id="filter-task-type"
              className="input mono"
              placeholder="e.g. compute_prime"
              value={taskTypeFilter}
              onChange={(e) => {
                setOffset(0);
                setTaskTypeFilter(e.target.value);
              }}
            />
          </div>

          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-priority">Priority</label>
            <select
              id="filter-priority"
              className="select"
              value={priorityFilter}
              onChange={(e) => {
                setOffset(0);
                setPriorityFilter(e.target.value);
              }}
            >
              <option value="">All Priorities</option>
              <option value="4">4 — Critical</option>
              <option value="3">3 — High</option>
              <option value="2">2 — Medium</option>
              <option value="1">1 — Low</option>
            </select>
          </div>
        </div>

        <div className="table-wrap">
          <table className="data-table">
            <thead>
              <tr>
                <th>Task Name / ID</th>
                <th>Type</th>
                <th>Status</th>
                <th>Priority</th>
                <th>Retries</th>
                <th>Updated</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {items.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ color: "var(--text-muted)" }}>
                    No matching tasks found.
                  </td>
                </tr>
              ) : (
                items.map((task) => {
                  const isBusy = busyTaskId === task.id;
                  return (
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
                        {task.status === "RETRY_WAIT" ? (
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 3 }}>
                            Backoff wait (inactive)
                          </div>
                        ) : task.status === "PENDING" && task.scheduled_at ? (
                          <div style={{ fontSize: 11, color: "var(--text-muted)", marginTop: 3 }}>
                            Scheduled wait
                          </div>
                        ) : null}
                      </td>
                      <td>
                        <PriorityBadge priority={task.priority} />
                      </td>
                      <td className="tabular">
                        {task.retry_count} / {task.max_retries}
                      </td>
                      <td className="tabular">
                        {new Date(task.updated_at).toLocaleString()}
                      </td>
                      <td>
                        <div style={{ display: "flex", gap: 6, alignItems: "center" }}>
                          <Link to={`/tasks/${task.id}`} className="btn btn-sm">
                            Inspect
                          </Link>
                          {canWrite && canCancelStatus(task.status) ? (
                            <button
                              type="button"
                              className="btn btn-sm btn-danger"
                              disabled={isBusy}
                              onClick={() => void handleAction(task.id, "cancel")}
                            >
                              Cancel
                            </button>
                          ) : null}
                          {canWrite && canRetryStatus(task.status) ? (
                            <button
                              type="button"
                              className="btn btn-sm btn-primary"
                              disabled={isBusy}
                              onClick={() => void handleAction(task.id, "retry")}
                            >
                              Retry
                            </button>
                          ) : null}
                          {isImmutableTerminalStatus(task.status) ? (
                            <span style={{ fontSize: 11, color: "var(--text-muted)" }}>
                              Immutable
                            </span>
                          ) : null}
                        </div>
                      </td>
                    </tr>
                  );
                })
              )}
            </tbody>
          </table>
        </div>

        <div
          style={{
            marginTop: 14,
            display: "flex",
            justifyContent: "space-between",
            alignItems: "center",
          }}
        >
          <span className="tabular" style={{ fontSize: 13, color: "var(--text-secondary)" }}>
            Showing {items.length === 0 ? 0 : offset + 1}–{offset + items.length} of{" "}
            {total}
          </span>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className="btn btn-sm"
              disabled={offset === 0}
              onClick={() => setOffset((o) => Math.max(0, o - PAGE_LIMIT))}
            >
              Previous
            </button>
            <button
              type="button"
              className="btn btn-sm"
              disabled={!hasMore}
              onClick={() => setOffset((o) => o + PAGE_LIMIT)}
            >
              Next
            </button>
          </div>
        </div>
      </section>
    </>
  );
}

