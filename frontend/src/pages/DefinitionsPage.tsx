import { useCallback, useEffect, useState } from "react";
import { Link, useSearchParams } from "react-router-dom";
import { ApiError } from "../api/client";
import type {
  DefinitionAuditEventItem,
  DefinitionExecutionDetailResponse,
  DefinitionExecutionSummary,
} from "../api/types";
import { useAuth } from "../auth/AuthContext";
import { PriorityBadge, StatusBadge } from "../components/StatusBadge";
import { useVisibilityPolling } from "../hooks/useVisibilityPolling";

function formatMs(ms: number | null | undefined): string {
  return ms !== null && ms !== undefined ? `${ms.toFixed(1)} ms` : "—";
}

export function DefinitionsPage() {
  const [searchParams, setSearchParams] = useSearchParams();
  const initialDefId = searchParams.get("id") || "";

  const { client, hasScope, isAuthenticated, sessionEpoch } = useAuth();
  const canWrite = hasScope("tasks:write");

  const [inputDefId, setInputDefId] = useState(initialDefId);
  const [activeDefId, setActiveDefId] = useState(initialDefId);

  const [activeTab, setActiveTab] = useState<"executions" | "audit">("executions");

  // Executions state
  const [executions, setExecutions] = useState<DefinitionExecutionSummary[]>([]);
  const [totalExecutions, setTotalExecutions] = useState(0);
  const [statusFilter, setStatusFilter] = useState("");
  const [versionFilter, setVersionFilter] = useState<string>("");
  const [execLoading, setExecLoading] = useState(false);
  const [execError, setExecError] = useState<string | null>(null);

  // Selected execution detail inspection
  const [selectedTaskId, setSelectedTaskId] = useState<string | null>(null);
  const [detailData, setDetailData] = useState<DefinitionExecutionDetailResponse | null>(null);
  const [detailLoading, setDetailLoading] = useState(false);
  const [detailError, setDetailError] = useState<string | null>(null);
  const [cancelBusy, setCancelBusy] = useState(false);
  const [cancelNotice, setCancelNotice] = useState<string | null>(null);

  // Audit state
  const [auditEvents, setAuditEvents] = useState<DefinitionAuditEventItem[]>([]);
  const [totalAudit, setTotalAudit] = useState(0);
  const [actionFilter, setActionFilter] = useState("");
  const [auditLoading, setAuditLoading] = useState(false);
  const [auditError, setAuditError] = useState<string | null>(null);

  // Keep search params in sync
  const handleInspectDefinition = (defId: string) => {
    const trimmed = defId.trim();
    if (!trimmed) return;
    setActiveDefId(trimmed);
    setSelectedTaskId(null);
    setDetailData(null);
    setSearchParams({ id: trimmed });
  };

  const fetchExecutions = useCallback(async () => {
    if (!activeDefId) return;
    setExecLoading(true);
    setExecError(null);
    try {
      const vNum = versionFilter ? Number.parseInt(versionFilter, 10) : undefined;
      const res = await client.listDefinitionExecutions(activeDefId, {
        ...(statusFilter ? { status: statusFilter } : {}),
        ...(vNum && Number.isFinite(vNum) ? { version_number: vNum } : {}),
        limit: 50,
        offset: 0,
      });
      setExecutions(res.items);
      setTotalExecutions(res.total);
    } catch (err) {
      setExecError(err instanceof ApiError ? err.message : "Failed to load executions.");
    } finally {
      setExecLoading(false);
    }
  }, [client, activeDefId, statusFilter, versionFilter]);

  const fetchAudit = useCallback(async () => {
    if (!activeDefId) return;
    setAuditLoading(true);
    setAuditError(null);
    try {
      const res = await client.listDefinitionAuditHistory(activeDefId, {
        ...(actionFilter ? { action: actionFilter } : {}),
        limit: 50,
        offset: 0,
      });
      setAuditEvents(res.items);
      setTotalAudit(res.total);
    } catch (err) {
      setAuditError(err instanceof ApiError ? err.message : "Failed to load audit events.");
    } finally {
      setAuditLoading(false);
    }
  }, [client, activeDefId, actionFilter]);

  useEffect(() => {
    if (activeDefId) {
      if (activeTab === "executions") {
        void fetchExecutions();
      } else {
        void fetchAudit();
      }
    }
  }, [activeDefId, activeTab, fetchExecutions, fetchAudit]);

  const fetchDetail = useCallback(
    async (taskId: string) => {
      if (!activeDefId) return;
      setDetailLoading(true);
      setDetailError(null);
      setCancelNotice(null);
      try {
        const detail = await client.getDefinitionExecutionDetail(activeDefId, taskId);
        setDetailData(detail);
      } catch (err) {
        setDetailError(
          err instanceof ApiError ? err.message : "Failed to load execution detail.",
        );
      } finally {
        setDetailLoading(false);
      }
    },
    [client, activeDefId],
  );

  const handleSelectExecution = (taskId: string) => {
    setSelectedTaskId(taskId);
    void fetchDetail(taskId);
  };

  const handleCancelExecution = async () => {
    if (!activeDefId || !selectedTaskId) return;
    setCancelBusy(true);
    setCancelNotice(null);
    try {
      const outcome = await client.cancelDefinitionExecution(activeDefId, selectedTaskId);
      setCancelNotice(`Cancellation submitted: ${outcome.status} (${outcome.message})`);
      if (detailData) {
        setDetailData({ ...detailData, status: outcome.status });
      }
      setExecutions((prev) =>
        prev.map((e) => (e.id === selectedTaskId ? { ...e, status: outcome.status } : e)),
      );
    } catch (err) {
      setCancelNotice(
        err instanceof ApiError ? `Cancel failed: ${err.message}` : "Cancellation failed.",
      );
    } finally {
      setCancelBusy(false);
    }
  };

  // Optional background polling if execution is active
  useVisibilityPolling({
    enabled:
      isAuthenticated &&
      Boolean(activeDefId) &&
      Boolean(selectedTaskId) &&
      Boolean(
        detailData &&
          detailData.status !== "SUCCESS" &&
          detailData.status !== "FAILED" &&
          detailData.status !== "CANCELLED" &&
          detailData.status !== "DEAD_LETTER",
      ),
    intervalMs: 3000,
    resetKey: `${sessionEpoch}:${selectedTaskId}`,
    fetcher: () => (selectedTaskId ? fetchDetail(selectedTaskId) : Promise.resolve()),
  });

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Custom Task Definitions</h1>
          <p className="page-subtitle">
            Phase 11 Controlled Execution, Version Snapshots & Audit Trail
          </p>
        </div>
      </div>

      <div className="card" style={{ marginBottom: 20 }}>
        <form
          onSubmit={(e) => {
            e.preventDefault();
            handleInspectDefinition(inputDefId);
          }}
          style={{ display: "flex", gap: 12, alignItems: "flex-end", flexWrap: "wrap" }}
        >
          <div className="field" style={{ flex: 1, minWidth: 260, margin: 0 }}>
            <label htmlFor="def-lookup-input">Definition Identifier</label>
            <input
              id="def-lookup-input"
              className="input mono"
              placeholder="e.g. def_image_resizer or def_order_pipeline"
              value={inputDefId}
              onChange={(e) => setInputDefId(e.target.value)}
              required
            />
          </div>
          <button type="submit" className="btn btn-primary">
            Inspect Definition
          </button>
          {activeDefId ? (
            <Link
              to="/submit"
              className="btn"
              style={{ textDecoration: "none", alignSelf: "flex-end" }}
            >
              + Execute Definition
            </Link>
          ) : null}
        </form>
      </div>

      {!activeDefId ? (
        <div className="banner banner-info">
          Enter a custom task definition identifier above to inspect executions, immutable version snapshots, failure diagnoses, and sensitive-data scrubbed audit history.
        </div>
      ) : (
        <>
          <div style={{ display: "flex", gap: 10, marginBottom: 16 }}>
            <button
              type="button"
              className={`btn ${activeTab === "executions" ? "btn-primary" : ""}`}
              onClick={() => setActiveTab("executions")}
            >
              Executions ({totalExecutions})
            </button>
            <button
              type="button"
              className={`btn ${activeTab === "audit" ? "btn-primary" : ""}`}
              onClick={() => setActiveTab("audit")}
            >
              Audit Trail ({totalAudit})
            </button>
          </div>

          {activeTab === "executions" ? (
            <>
              <div
                className="card"
                style={{
                  display: "flex",
                  gap: 12,
                  alignItems: "center",
                  flexWrap: "wrap",
                  marginBottom: 16,
                  padding: 12,
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <label htmlFor="exec-status-filter" style={{ fontSize: 13, margin: 0 }}>
                    Status:
                  </label>
                  <select
                    id="exec-status-filter"
                    className="select"
                    value={statusFilter}
                    onChange={(e) => setStatusFilter(e.target.value)}
                    style={{ padding: "4px 8px" }}
                  >
                    <option value="">All Statuses</option>
                    <option value="PENDING">PENDING</option>
                    <option value="QUEUED">QUEUED</option>
                    <option value="RUNNING">RUNNING</option>
                    <option value="SUCCESS">SUCCESS</option>
                    <option value="RETRY_WAIT">RETRY_WAIT</option>
                    <option value="FAILED">FAILED</option>
                    <option value="DEAD_LETTER">DEAD_LETTER</option>
                    <option value="CANCELLED">CANCELLED</option>
                  </select>
                </div>

                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <label htmlFor="exec-version-filter" style={{ fontSize: 13, margin: 0 }}>
                    Version #:
                  </label>
                  <input
                    id="exec-version-filter"
                    type="number"
                    min={1}
                    className="input tabular"
                    style={{ width: 80, padding: "4px 8px" }}
                    placeholder="All"
                    value={versionFilter}
                    onChange={(e) => setVersionFilter(e.target.value)}
                  />
                </div>

                <button
                  type="button"
                  className="btn btn-sm"
                  onClick={() => void fetchExecutions()}
                  disabled={execLoading}
                >
                  {execLoading ? "Refreshing..." : "Refresh"}
                </button>
              </div>

              {execError ? <div className="banner banner-error">{execError}</div> : null}

              <div style={{ display: "grid", gridTemplateColumns: selectedTaskId ? "1fr 1fr" : "1fr", gap: 16 }}>
                <div className="card" style={{ overflowX: "auto" }}>
                  <h3 style={{ marginTop: 0, marginBottom: 12, fontSize: 16 }}>
                    Executions for <span className="mono">{activeDefId}</span>
                  </h3>
                  {executions.length === 0 ? (
                    <p style={{ color: "var(--text-secondary)", margin: "16px 0" }}>
                      {execLoading ? "Loading executions..." : "No executions found matching criteria."}
                    </p>
                  ) : (
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Task ID / Name</th>
                          <th>Version</th>
                          <th>Status</th>
                          <th>Duration</th>
                          <th>Submitted</th>
                          <th>Action</th>
                        </tr>
                      </thead>
                      <tbody>
                        {executions.map((item) => (
                          <tr
                            key={item.id}
                            style={{
                              background: selectedTaskId === item.id ? "rgba(56, 189, 248, 0.08)" : undefined,
                            }}
                          >
                            <td>
                              <div style={{ fontWeight: 600 }}>{item.task_name}</div>
                              <div className="mono" style={{ fontSize: 11, color: "var(--text-muted)" }}>
                                {item.id}
                              </div>
                            </td>
                            <td className="tabular">
                              {item.version_number ? `v${item.version_number}` : "—"}
                            </td>
                            <td>
                              <StatusBadge status={item.status as any} />
                            </td>
                            <td className="tabular">{formatMs(item.duration_ms)}</td>
                            <td className="tabular" style={{ fontSize: 12 }}>
                              {new Date(item.created_at).toLocaleTimeString()}
                            </td>
                            <td>
                              <button
                                type="button"
                                className="btn btn-sm"
                                onClick={() => handleSelectExecution(item.id)}
                              >
                                {selectedTaskId === item.id ? "Viewing" : "Inspect"}
                              </button>
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  )}
                </div>

                {selectedTaskId ? (
                  <div className="card">
                    <div style={{ display: "flex", justifyContent: "space-between", alignItems: "center", marginBottom: 12 }}>
                      <h3 style={{ margin: 0, fontSize: 16 }}>
                        Execution Inspection
                      </h3>
                      <button
                        type="button"
                        className="btn btn-sm"
                        onClick={() => setSelectedTaskId(null)}
                      >
                        Close
                      </button>
                    </div>

                    {cancelNotice ? (
                      <div className="banner banner-info" style={{ marginBottom: 12 }}>
                        {cancelNotice}
                      </div>
                    ) : null}

                    {detailLoading && !detailData ? (
                      <p>Loading execution inspection details...</p>
                    ) : detailError ? (
                      <div className="banner banner-error">{detailError}</div>
                    ) : detailData ? (
                      <div>
                        <div style={{ display: "flex", gap: 8, alignItems: "center", marginBottom: 12, flexWrap: "wrap" }}>
                          <StatusBadge status={detailData.status as any} />
                          <PriorityBadge priority={detailData.priority as any} />
                          <span className="pill pill-neutral mono">{detailData.task_type}</span>
                          {canWrite &&
                          (detailData.status === "QUEUED" ||
                            detailData.status === "RUNNING" ||
                            detailData.status === "PENDING" ||
                            detailData.status === "RETRY_WAIT") ? (
                            <button
                              type="button"
                              className="btn btn-sm btn-danger"
                              disabled={cancelBusy}
                              onClick={() => void handleCancelExecution()}
                            >
                              {cancelBusy ? "Cancelling..." : "Fenced Cancel"}
                            </button>
                          ) : null}
                          <Link to={`/tasks/${detailData.id}`} className="btn btn-sm">
                            Open Full Task
                          </Link>
                        </div>

                        <div className="detail-grid" style={{ marginBottom: 16 }}>
                          <div className="kv-item">
                            <span className="kv-label">Task ID</span>
                            <span className="kv-val mono">{detailData.id}</span>
                          </div>
                          <div className="kv-item">
                            <span className="kv-label">Definition ID</span>
                            <span className="kv-val mono">{detailData.definition_id}</span>
                          </div>
                          <div className="kv-item">
                            <span className="kv-label">Pinned Version ID</span>
                            <span className="kv-val mono">{detailData.definition_version_id ?? "—"}</span>
                          </div>
                          <div className="kv-item">
                            <span className="kv-label">Version Number</span>
                            <span className="kv-val tabular">
                              {detailData.version?.version_number ? `v${detailData.version.version_number}` : "—"}
                            </span>
                          </div>
                          <div className="kv-item">
                            <span className="kv-label">Version Content Hash</span>
                            <span className="kv-val mono" style={{ fontSize: 11 }}>
                              {detailData.version?.content_hash ?? "—"}
                            </span>
                          </div>
                          <div className="kv-item">
                            <span className="kv-label">Submitted At</span>
                            <span className="kv-val tabular">
                              {new Date(detailData.timeline.created_at).toLocaleString()}
                            </span>
                          </div>
                        </div>

                        {detailData.failure_diagnosis ? (
                          <div className="banner banner-error" style={{ marginBottom: 16 }}>
                            <strong>Failure Diagnosis: </strong>
                            <span className="mono">{detailData.failure_diagnosis.classification}</span>
                            {detailData.failure_diagnosis.error_message ? (
                              <div>{detailData.failure_diagnosis.error_message}</div>
                            ) : null}
                            <div style={{ marginTop: 6, fontSize: 12 }}>
                              Retryable: {detailData.failure_diagnosis.retryable ? "Yes" : "No"} |{" "}
                              Timeout: {detailData.failure_diagnosis.is_timeout ? "Yes" : "No"} |{" "}
                              Retries Exhausted: {detailData.failure_diagnosis.retries_exhausted ? "Yes" : "No"}
                            </div>
                          </div>
                        ) : null}

                        <div className="field">
                          <label>Scrubbed Input Payload</label>
                          <pre
                            className="mono"
                            style={{
                              background: "#090d16",
                              padding: 8,
                              borderRadius: 4,
                              fontSize: 12,
                              maxHeight: 140,
                              overflowY: "auto",
                            }}
                          >
                            {JSON.stringify(detailData.payload, null, 2)}
                          </pre>
                        </div>

                        {detailData.result ? (
                          <div className="field">
                            <label>Scrubbed Result</label>
                            <pre
                              className="mono"
                              style={{
                                background: "#090d16",
                                padding: 8,
                                borderRadius: 4,
                                fontSize: 12,
                                maxHeight: 140,
                                overflowY: "auto",
                              }}
                            >
                              {JSON.stringify(detailData.result, null, 2)}
                            </pre>
                          </div>
                        ) : null}

                        <h4 style={{ margin: "12px 0 6px 0", fontSize: 14 }}>
                          Attempts ({detailData.attempts.length})
                        </h4>
                        {detailData.attempts.length === 0 ? (
                          <p style={{ color: "var(--text-secondary)", fontSize: 12 }}>No attempts recorded yet.</p>
                        ) : (
                          <table className="table" style={{ fontSize: 12 }}>
                            <thead>
                              <tr>
                                <th>#</th>
                                <th>Status</th>
                                <th>Worker</th>
                                <th>Duration</th>
                                <th>Error</th>
                              </tr>
                            </thead>
                            <tbody>
                              {detailData.attempts.map((att) => (
                                <tr key={att.id}>
                                  <td>{att.attempt_number}</td>
                                  <td>{att.status}</td>
                                  <td className="mono">{att.worker_id ?? "—"}</td>
                                  <td className="tabular">{formatMs(att.duration_ms)}</td>
                                  <td style={{ color: "#f87171" }}>{att.error_message ?? "—"}</td>
                                </tr>
                              ))}
                            </tbody>
                          </table>
                        )}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </div>
            </>
          ) : (
            <div className="card">
              <div
                style={{
                  display: "flex",
                  gap: 12,
                  alignItems: "center",
                  flexWrap: "wrap",
                  marginBottom: 16,
                  padding: 12,
                }}
              >
                <div style={{ display: "flex", alignItems: "center", gap: 6 }}>
                  <label htmlFor="audit-action-filter" style={{ fontSize: 13, margin: 0 }}>
                    Action:
                  </label>
                  <input
                    id="audit-action-filter"
                    className="input mono"
                    style={{ width: 160, padding: "4px 8px" }}
                    placeholder="e.g. EXECUTE or CANCEL"
                    value={actionFilter}
                    onChange={(e) => setActionFilter(e.target.value)}
                  />
                </div>

                <button
                  type="button"
                  className="btn btn-sm"
                  onClick={() => void fetchAudit()}
                  disabled={auditLoading}
                >
                  {auditLoading ? "Refreshing..." : "Refresh"}
                </button>
              </div>

              {auditError ? <div className="banner banner-error">{auditError}</div> : null}

              {auditEvents.length === 0 ? (
                <p style={{ color: "var(--text-secondary)", margin: "16px 0" }}>
                  {auditLoading ? "Loading audit events..." : "No audit trail events found."}
                </p>
              ) : (
                <table className="table">
                  <thead>
                    <tr>
                      <th>Action</th>
                      <th>Actor ID</th>
                      <th>Version ID</th>
                      <th>Scrubbed Metadata</th>
                      <th>Timestamp</th>
                    </tr>
                  </thead>
                  <tbody>
                    {auditEvents.map((evt) => (
                      <tr key={evt.id}>
                        <td>
                          <span
                            className="pill"
                            style={{
                              background:
                                evt.action.includes("CANCEL")
                                  ? "#7f1d1d"
                                  : evt.action.includes("EXECUTE")
                                    ? "#065f46"
                                    : "#1e293b",
                              color: "#f8fafc",
                            }}
                          >
                            {evt.action}
                          </span>
                        </td>
                        <td className="mono">{evt.actor_id}</td>
                        <td className="mono">{evt.version_id ?? "—"}</td>
                        <td>
                          <pre
                            className="mono"
                            style={{
                              background: "#090d16",
                              padding: 6,
                              borderRadius: 4,
                              fontSize: 11,
                              margin: 0,
                              maxHeight: 100,
                              overflowY: "auto",
                            }}
                          >
                            {JSON.stringify(evt.metadata, null, 2)}
                          </pre>
                        </td>
                        <td className="tabular" style={{ fontSize: 12 }}>
                          {new Date(evt.created_at).toLocaleString()}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}
        </>
      )}
    </>
  );
}
