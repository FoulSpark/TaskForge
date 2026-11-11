import { FormEvent, useEffect, useState } from "react";
import { useNavigate } from "react-router-dom";
import { ApiError } from "../api/client";
import type {
  SubmissionConstraints,
  TaskPriority,
  TaskTypeCatalogItem,
} from "../api/types";
import { useAuth } from "../auth/AuthContext";

const FALLBACK_CONSTRAINTS: SubmissionConstraints = {
  priority: { min: 1, max: 4, default: 2 },
  timeout_seconds: { min: 1, max: 3600, default: 60 },
  max_retries: { min: 0, max: 10, default: 3 },
  task_name: { min_length: 1, max_length: 128 },
  idempotency_key: { max_length: 128 },
};

export function SubmitTaskPage() {
  const { client, hasScope } = useAuth();
  const navigate = useNavigate();

  const [catalog, setCatalog] = useState<TaskTypeCatalogItem[]>([]);
  const [constraints, setConstraints] =
    useState<SubmissionConstraints>(FALLBACK_CONSTRAINTS);
  const [loadingCatalog, setLoadingCatalog] = useState(true);
  const [catalogError, setCatalogError] = useState<string | null>(null);

  const [taskName, setTaskName] = useState("Web Console Job");
  const [taskType, setTaskType] = useState("");
  const [payloadText, setPayloadText] = useState("{}");
  const [priority, setPriority] = useState<TaskPriority>(2);
  const [timeoutSeconds, setTimeoutSeconds] = useState(60);
  const [maxRetries, setMaxRetries] = useState(3);
  const [idempotencyKey, setIdempotencyKey] = useState("");

  const [submitting, setSubmitting] = useState(false);
  const [submitError, setSubmitError] = useState<string | null>(null);

  const canWrite = hasScope("tasks:write");

  useEffect(() => {
    let active = true;
    setLoadingCatalog(true);
    client
      .getTaskTypes()
      .then((res) => {
        if (!active) return;
        setCatalog(res.items);
        setConstraints(res.submission_constraints ?? FALLBACK_CONSTRAINTS);
        const firstAvailable = res.items.find((i) => i.available) ?? res.items[0];
        if (firstAvailable) {
          setTaskType(firstAvailable.task_type);
          setPayloadText(JSON.stringify(firstAvailable.default_payload ?? {}, null, 2));
        }
        setCatalogError(null);
      })
      .catch((err) => {
        if (!active) return;
        setCatalogError(
          err instanceof ApiError ? err.message : "Failed to load task catalog.",
        );
      })
      .finally(() => {
        if (active) setLoadingCatalog(false);
      });

    return () => {
      active = false;
    };
  }, [client]);

  const selectedMeta = catalog.find((c) => c.task_type === taskType);

  const handleTaskTypeChange = (nextType: string) => {
    setTaskType(nextType);
    const meta = catalog.find((c) => c.task_type === nextType);
    if (meta) {
      setPayloadText(JSON.stringify(meta.default_payload ?? {}, null, 2));
    }
  };

  const handleSubmit = async (e: FormEvent) => {
    e.preventDefault();
    setSubmitError(null);

    let parsedPayload: unknown;
    try {
      parsedPayload = JSON.parse(payloadText || "{}");
    } catch {
      setSubmitError("Payload must be valid JSON.");
      return;
    }

    if (
      typeof parsedPayload !== "object" ||
      parsedPayload === null ||
      Array.isArray(parsedPayload)
    ) {
      setSubmitError("Payload must be a JSON object (e.g. {\"n\": 1000}).");
      return;
    }

    setSubmitting(true);
    try {
      const created = await client.createTask({
        task_name: taskName.trim(),
        task_type: taskType,
        payload: parsedPayload as Record<string, unknown>,
        priority,
        timeout_seconds: timeoutSeconds,
        max_retries: maxRetries,
        ...(idempotencyKey.trim() ? { idempotency_key: idempotencyKey.trim() } : {}),
      });
      navigate(`/tasks/${created.id}`);
    } catch (err) {
      if (err instanceof ApiError) {
        setSubmitError(`${err.code}: ${err.message}`);
      } else {
        setSubmitError("Failed to submit task.");
      }
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <>
      <div className="page-header">
        <div>
          <h1 className="page-title">Submit Task</h1>
          <p className="page-subtitle">
            Runtime-registered task handlers from{" "}
            <span className="mono">/api/v1/task-types</span>
          </p>
        </div>
      </div>

      {!canWrite ? (
        <div className="banner banner-warn" role="status">
          Current API key lacks <span className="mono">tasks:write</span> scope.
          Submitting will return HTTP 403 while keeping your session signed in.
        </div>
      ) : null}

      {catalogError ? (
        <div className="banner banner-error" role="alert">
          {catalogError}
        </div>
      ) : null}

      {submitError ? (
        <div className="banner banner-error" role="alert">
          {submitError}
        </div>
      ) : null}

      <div className="card">
        {loadingCatalog ? (
          <p style={{ margin: 0, color: "var(--text-secondary)" }}>
            Loading registered task handlers...
          </p>
        ) : (
          <form onSubmit={handleSubmit}>
            <div className="form-grid-2">
              <div className="field">
                <label htmlFor="task-name-input">Task Name</label>
                <input
                  id="task-name-input"
                  className="input"
                  value={taskName}
                  onChange={(e) => setTaskName(e.target.value)}
                  minLength={constraints.task_name.min_length}
                  maxLength={constraints.task_name.max_length}
                  required
                />
              </div>

              <div className="field">
                <label htmlFor="task-type-select">Task Type</label>
                <select
                  id="task-type-select"
                  className="select mono"
                  value={taskType}
                  onChange={(e) => handleTaskTypeChange(e.target.value)}
                  required
                >
                  {catalog.map((item) => (
                    <option
                      key={item.task_type}
                      value={item.task_type}
                      disabled={!item.available}
                    >
                      {item.display_name} ({item.task_type})
                      {!item.available ? " [unavailable]" : ""}
                    </option>
                  ))}
                </select>
                {selectedMeta ? (
                  <span className="field-hint">{selectedMeta.description}</span>
                ) : null}
              </div>
            </div>

            <div className="form-grid-2">
              <div className="field">
                <label htmlFor="priority-select">Priority</label>
                <select
                  id="priority-select"
                  className="select"
                  value={priority}
                  onChange={(e) => setPriority(Number(e.target.value) as TaskPriority)}
                >
                  <option value={1}>1 — Low</option>
                  <option value={2}>2 — Medium (Default)</option>
                  <option value={3}>3 — High</option>
                  <option value={4}>4 — Critical</option>
                </select>
              </div>

              <div className="field">
                <label htmlFor="timeout-input">Timeout (seconds)</label>
                <input
                  id="timeout-input"
                  type="number"
                  className="input tabular"
                  value={timeoutSeconds}
                  min={constraints.timeout_seconds.min}
                  max={constraints.timeout_seconds.max}
                  onChange={(e) => setTimeoutSeconds(Number(e.target.value))}
                  required
                />
              </div>

              <div className="field">
                <label htmlFor="retries-input">Max Retries</label>
                <input
                  id="retries-input"
                  type="number"
                  className="input tabular"
                  value={maxRetries}
                  min={constraints.max_retries.min}
                  max={constraints.max_retries.max}
                  onChange={(e) => setMaxRetries(Number(e.target.value))}
                  required
                />
              </div>

              <div className="field">
                <label htmlFor="idempotency-input">Idempotency Key (Optional)</label>
                <input
                  id="idempotency-input"
                  className="input mono"
                  value={idempotencyKey}
                  maxLength={constraints.idempotency_key.max_length}
                  onChange={(e) => setIdempotencyKey(e.target.value)}
                  placeholder="req-20261112-001"
                />
              </div>
            </div>

            <div className="field">
              <div
                style={{
                  display: "flex",
                  justifyContent: "space-between",
                  alignItems: "center",
                }}
              >
                <label htmlFor="payload-textarea">JSON Payload</label>
                {selectedMeta ? (
                  <button
                    type="button"
                    className="btn btn-sm"
                    onClick={() =>
                      setPayloadText(
                        JSON.stringify(selectedMeta.default_payload ?? {}, null, 2),
                      )
                    }
                  >
                    Reset Default Payload
                  </button>
                ) : null}
              </div>
              <textarea
                id="payload-textarea"
                rows={7}
                className="textarea"
                value={payloadText}
                onChange={(e) => setPayloadText(e.target.value)}
              />
            </div>

            <div style={{ display: "flex", gap: 10 }}>
              <button
                type="submit"
                className="btn btn-primary"
                disabled={submitting || !taskType}
              >
                {submitting ? "Submitting Task..." : "Submit Task"}
              </button>
            </div>
          </form>
        )}
      </div>
    </>
  );
}

