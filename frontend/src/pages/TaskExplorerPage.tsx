import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ApiError } from "../api/client";
import {
  canCancelStatus,
  canRetryStatus,
  isFutureScheduled,
  isImmutableTerminalStatus,
} from "../api/lifecycle";
import type { TaskItem, TaskStatus } from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { PriorityBadge, StatusBadge } from "../components/StatusBadge";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

const DEFAULT_LIMIT = 20;
const LIMIT_OPTIONS = [10, 20, 50, 100] as const;

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

const VALID_STATUS_SET = new Set<string>(STATUS_OPTIONS);
const VALID_PRIORITY_SET = new Set<string>(["1", "2", "3", "4"]);

export interface ExplorerQueryState {
  status: string;
  taskType: string;
  priority: string;
  limit: number;
  offset: number;
}

function parseNonNegativeInt(raw: string | null, fallback: number): number {
  if (!raw || !/^\d+$/.test(raw.trim())) {
    return fallback;
  }
  const num = Number.parseInt(raw.trim(), 10);
  if (!Number.isFinite(num) || num < 0) {
    return fallback;
  }
  return num;
}

export function sanitizeExplorerParams(sp: URLSearchParams): {
  state: ExplorerQueryState;
  needsNormalization: boolean;
} {
  const rawStatus = sp.get("status");
  const status = rawStatus && VALID_STATUS_SET.has(rawStatus) ? rawStatus : "";

  const rawTaskType = sp.get("task_type");
  const taskType = rawTaskType ? rawTaskType.trim().slice(0, 64) : "";

  const rawPriority = sp.get("priority");
  const priority = rawPriority && VALID_PRIORITY_SET.has(rawPriority) ? rawPriority : "";

  const rawLimit = sp.get("limit");
  let limit = DEFAULT_LIMIT;
  if (rawLimit !== null) {
    const parsedLimit = parseNonNegativeInt(rawLimit, DEFAULT_LIMIT);
    limit = Math.min(100, Math.max(1, parsedLimit));
  }

  const rawOffset = sp.get("offset");
  const offset = parseNonNegativeInt(rawOffset, 0);

  const canonical = buildSearchParams({ status, taskType, priority, limit, offset });
  const needsNormalization = canonical.toString() !== sp.toString();

  return {
    state: { status, taskType, priority, limit, offset },
    needsNormalization,
  };
}

function buildSearchParams(q: ExplorerQueryState): URLSearchParams {
  const next = new URLSearchParams();
  if (q.status) next.set("status", q.status);
  if (q.taskType) next.set("task_type", q.taskType);
  if (q.priority) next.set("priority", q.priority);
  if (q.limit !== DEFAULT_LIMIT) next.set("limit", String(q.limit));
  if (q.offset > 0) next.set("offset", String(q.offset));
  return next;
}

type ExplorerErrorState = {
  kind: "rate_limit" | "unavailable" | "error";
  message: string;
  retryAfterSeconds?: number | null;
};

export function TaskExplorerPage() {
  const { client, isAuthenticated, sessionEpoch, hasScope } = useAuth();
  const canWrite = hasScope("tasks:write");

  const [searchParams, setSearchParams] = useSearchParams();
  const { state: query, needsNormalization } = useMemo(
    () => sanitizeExplorerParams(searchParams),
    [searchParams],
  );

  // Clean invalid URL query params immediately so deep links stay canonical
  useEffect(() => {
    if (needsNormalization) {
      setSearchParams(buildSearchParams(query), { replace: true });
    }
  }, [needsNormalization, query, setSearchParams]);

  const activeQueryKey = `${sessionEpoch}:${query.status}:${query.taskType}:${query.priority}:${query.limit}:${query.offset}`;
  const activeQueryKeyRef = useRef(activeQueryKey);
  activeQueryKeyRef.current = activeQueryKey;

  const [loadedQueryKey, setLoadedQueryKey] = useState<string | null>(null);
  const [items, setItems] = useState<TaskItem[]>([]);
  const [total, setTotal] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const [isRefreshing, setIsRefreshing] = useState(false);
  const [errorState, setErrorState] = useState<ExplorerErrorState | null>(null);
  const [busyTaskId, setBusyTaskId] = useState<string | null>(null);

  useEffect(() => {
    setLoadedQueryKey(null);
    setItems([]);
    setTotal(0);
    setHasMore(false);
    setErrorState(null);
  }, [sessionEpoch]);

  const updateQuery = useCallback(
    (patch: Partial<ExplorerQueryState>, resetOffset: boolean, replace = true) => {
      const next: ExplorerQueryState = {
        ...query,
        ...patch,
        offset: resetOffset ? 0 : (patch.offset ?? query.offset),
      };
      setErrorState(null);
      setSearchParams(buildSearchParams(next), { replace });
    },
    [query, setSearchParams],
  );

  const fetchTasks = useCallback(async () => {
    const requestKey = `${sessionEpoch}:${query.status}:${query.taskType}:${query.priority}:${query.limit}:${query.offset}`;
    setIsRefreshing(true);
    try {
      const res = await client.listTasks({
        status: query.status || undefined,
        task_type: query.taskType || undefined,
        priority: query.priority || undefined,
        limit: query.limit,
        offset: query.offset,
      });

      // Discard stale response if user switched filter/pagination while fetch was in flight
      if (activeQueryKeyRef.current !== requestKey) {
        return;
      }

      setItems(res.items);
      setTotal(res.total);
      setHasMore(res.has_more);
      setLoadedQueryKey(requestKey);
      setErrorState(null);
    } catch (err) {
      if (activeQueryKeyRef.current !== requestKey) {
        return;
      }
      if (err instanceof ApiError && err.status === 401) {
        setLoadedQueryKey(null);
        setItems([]);
        setTotal(0);
        throw err;
      }
      if (err instanceof ApiError) {
        if (err.status === 429) {
          setErrorState({
            kind: "rate_limit",
            message: err.message,
            retryAfterSeconds: err.retryAfterSeconds,
          });
          throw err;
        }
        if (err.status === 503) {
          setErrorState({
            kind: "unavailable",
            message: err.message,
            retryAfterSeconds: err.retryAfterSeconds,
          });
          throw err;
        }
        setErrorState({ kind: "error", message: err.message });
      } else {
        setErrorState({ kind: "error", message: "Failed to fetch tasks." });
      }
    } finally {
      if (activeQueryKeyRef.current === requestKey) {
        setIsRefreshing(false);
      }
    }
  }, [client, sessionEpoch, query]);

  const { backoffSeconds } = useVisibilityPolling({
    enabled: isAuthenticated,
    intervalMs: 2500,
    resetKey: activeQueryKey,
    fetcher: fetchTasks,
  });

  const handleManualRefresh = () => {
    void fetchTasks().catch(() => {
      // Backpressure (429/503) and auth (401) state already handled in fetchTasks/client
    });
  };

  const handleAction = async (taskId: string, action: "cancel" | "retry") => {
    setBusyTaskId(taskId);
    setErrorState(null);
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
        setErrorState({
          kind: "error",
          message: `State conflict (409): ${err.message}. Synchronized authoritative task list.`,
        });
      } else {
        setErrorState({
          kind: "error",
          message: err instanceof ApiError ? err.message : `Failed to ${action} task.`,
        });
      }
    } finally {
      setBusyTaskId(null);
    }
  };

  // Strict stale-row guard: never render previous filter's items under a newly chosen query
  const hasCurrentQueryRows = loadedQueryKey === activeQueryKey;
  const isTransitionLoading = !hasCurrentQueryRows && errorState === null;
  const visibleItems = hasCurrentQueryRows ? items : [];
  const hasActiveFilters = Boolean(query.status || query.taskType || query.priority);

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Task Explorer</h1>
          <p className="page-subtitle">
            Server-side filtering, URL-synchronized pagination, and live status polling
          </p>
        </div>
        <div style={{ display: "flex", gap: 8, alignItems: "center" }}>
          <button
            type="button"
            className="btn"
            disabled={isRefreshing}
            onClick={handleManualRefresh}
          >
            {isRefreshing ? "Refreshing..." : "Refresh"}
          </button>
          <Link to="/submit" className="btn btn-primary">
            + Submit Task
          </Link>
        </div>
      </div>

      {backoffSeconds || errorState?.kind === "rate_limit" || errorState?.kind === "unavailable" ? (
        <div className="banner banner-warn" role="status">
          <span>
            {errorState?.kind === "rate_limit"
              ? `Rate limited (429): Polling paused for ${backoffSeconds ?? errorState.retryAfterSeconds ?? 5}s.${hasCurrentQueryRows ? " Showing last valid rows." : ""}`
              : errorState?.kind === "unavailable"
                ? `Service unavailable (503): Retry in ${backoffSeconds ?? errorState.retryAfterSeconds ?? 5}s.${hasCurrentQueryRows ? " Showing last valid rows." : ""}`
                : `Polling paused for ${backoffSeconds}s due to API backpressure.`}
          </span>
        </div>
      ) : null}

      {errorState?.kind === "error" ? (
        <div className="banner banner-error" role="alert">
          <span>
            {errorState.message}
            {hasCurrentQueryRows ? " (Showing last valid rows.)" : ""}
          </span>
          <button
            type="button"
            className="btn btn-sm"
            onClick={handleManualRefresh}
          >
            Retry Fetch
          </button>
        </div>
      ) : null}

      <section className="card">
        <div className="form-grid-2" style={{ marginBottom: 14 }}>
          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-status">Status</label>
            <select
              id="filter-status"
              className="select"
              value={query.status}
              onChange={(e) => updateQuery({ status: e.target.value }, true)}
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
              value={query.taskType}
              onChange={(e) =>
                updateQuery({ taskType: e.target.value.slice(0, 64) }, true)
              }
            />
          </div>

          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-priority">Priority</label>
            <select
              id="filter-priority"
              className="select"
              value={query.priority}
              onChange={(e) => updateQuery({ priority: e.target.value }, true)}
            >
              <option value="">All Priorities</option>
              <option value="4">4 — Critical</option>
              <option value="3">3 — High</option>
              <option value="2">2 — Medium</option>
              <option value="1">1 — Low</option>
            </select>
          </div>

          <div className="field" style={{ marginBottom: 0 }}>
            <label htmlFor="filter-limit">Page Size</label>
            <select
              id="filter-limit"
              className="select tabular"
              value={query.limit}
              onChange={(e) =>
                updateQuery({ limit: Number(e.target.value) }, true)
              }
            >
              {LIMIT_OPTIONS.map((opt) => (
                <option key={opt} value={opt}>
                  {opt} per page
                </option>
              ))}
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
              {isTransitionLoading ? (
                <tr>
                  <td colSpan={7} style={{ color: "var(--text-secondary)" }}>
                    Loading matching tasks...
                  </td>
                </tr>
              ) : !hasCurrentQueryRows ? (
                <tr>
                  <td colSpan={7} style={{ color: "var(--text-muted)" }}>
                    Unable to load tasks for the selected filters.
                  </td>
                </tr>
              ) : visibleItems.length === 0 ? (
                <tr>
                  <td colSpan={7} style={{ color: "var(--text-muted)" }}>
                    <div
                      style={{
                        display: "flex",
                        alignItems: "center",
                        justifyContent: "space-between",
                        gap: 12,
                      }}
                    >
                      <span>
                        {hasActiveFilters
                          ? "No tasks match the current filters."
                          : "No tasks submitted for this tenant yet."}
                      </span>
                      {hasActiveFilters ? (
                        <button
                          type="button"
                          className="btn btn-sm"
                          onClick={() =>
                            updateQuery(
                              { status: "", taskType: "", priority: "" },
                              true,
                            )
                          }
                        >
                          Reset Filters
                        </button>
                      ) : null}
                    </div>
                  </td>
                </tr>
              ) : (
                visibleItems.map((task) => {
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
                        ) : isFutureScheduled(task) ? (
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
            {hasCurrentQueryRows
              ? `Showing ${visibleItems.length === 0 ? 0 : query.offset + 1}–${query.offset + visibleItems.length} of ${total}`
              : "Loading pagination metadata..."}
          </span>
          <div style={{ display: "flex", gap: 8 }}>
            <button
              type="button"
              className="btn btn-sm"
              disabled={query.offset === 0 || isTransitionLoading}
              onClick={() =>
                updateQuery(
                  { offset: Math.max(0, query.offset - query.limit) },
                  false,
                  false,
                )
              }
            >
              Previous
            </button>
            <button
              type="button"
              className="btn btn-sm"
              disabled={!hasCurrentQueryRows || !hasMore || isTransitionLoading}
              onClick={() =>
                updateQuery({ offset: query.offset + query.limit }, false, false)
              }
            >
              Next
            </button>
          </div>
        </div>
      </section>
    </>
  );
}
