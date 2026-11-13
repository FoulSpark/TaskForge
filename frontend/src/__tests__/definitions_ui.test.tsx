import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("Phase 11 UI — Custom Task Definitions, Controlled Execution & Audit Views", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("displays definition attribution with link on Task Detail for definition-originated tasks", async () => {
    const taskId = "task-def-9999";
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k11",
          tenant_id: "tenant-delta",
          name: "dev-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith(`/api/v1/tasks/${taskId}`)) {
        return jsonResponse({
          id: taskId,
          task_name: "Order Processing Execution",
          task_type: "def_order_pipeline",
          status: "SUCCESS",
          priority: 2,
          payload: { order_id: 12345 },
          retry_count: 0,
          max_retries: 3,
          timeout_seconds: 60,
          scheduled_at: null,
          started_at: "2026-11-13T10:00:00Z",
          completed_at: "2026-11-13T10:00:02Z",
          created_at: "2026-11-13T09:59:59Z",
          updated_at: "2026-11-13T10:00:02Z",
          definition_id: "def_orders",
          definition_version_id: "ver-orders-v1-uuid",
          attempts: [],
          result: {
            id: 1,
            return_value: { status: "processed" },
            execution_metadata: null,
            created_at: "2026-11-13T10:00:02Z",
          },
          events: [],
        });
      }
      return jsonResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", `/tasks/${taskId}`);
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_key_11");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Definition ID")).toBeInTheDocument();
      expect(screen.getByText("def_orders")).toBeInTheDocument();
      expect(screen.getByText("ver-orders-v1-uuid")).toBeInTheDocument();
    });
  });

  it("supports executing custom definitions from Submit Task page", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const method = init?.method ?? "GET";
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k11",
          tenant_id: "tenant-delta",
          name: "dev-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.includes("/api/v1/task-types")) {
        return jsonResponse({
          items: [
            {
              task_type: "math_compute",
              display_name: "Math Compute",
              category: "compute",
              description: "Math job",
              default_payload: {},
              payload_schema: {},
              available: true,
            },
          ],
          submission_constraints: {
            priority: { min: 1, max: 4, default: 2 },
            timeout_seconds: { min: 1, max: 3600, default: 60 },
            max_retries: { min: 0, max: 10, default: 3 },
            task_name: { min_length: 1, max_length: 128 },
            idempotency_key: { max_length: 128 },
          },
        });
      }
      if (url.includes("/api/v1/definitions/def_image_resizer/execute") && method === "POST") {
        return jsonResponse(
          {
            id: "task-exec-8888",
            task_name: "Custom Resizer Run",
            task_type: "image_resizer",
            payload: { width: 400 },
            status: "QUEUED",
            priority: 3,
            retry_count: 0,
            max_retries: 3,
            timeout_seconds: 120,
            created_at: "2026-11-13T10:00:00Z",
            updated_at: "2026-11-13T10:00:00Z",
            definition_id: "def_image_resizer",
            definition_version_id: "ver-img-v2-uuid",
            attempts: [],
            events: [],
          },
          201,
        );
      }
      if (url.includes("/api/v1/tasks/task-exec-8888")) {
        return jsonResponse({
          id: "task-exec-8888",
          task_name: "Custom Resizer Run",
          task_type: "image_resizer",
          payload: { width: 400 },
          status: "QUEUED",
          priority: 3,
          retry_count: 0,
          max_retries: 3,
          timeout_seconds: 120,
          created_at: "2026-11-13T10:00:00Z",
          updated_at: "2026-11-13T10:00:00Z",
          definition_id: "def_image_resizer",
          definition_version_id: "ver-img-v2-uuid",
          attempts: [],
          events: [],
        });
      }
      return jsonResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", "/submit");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_key_11");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Phase 11 Custom Definition")).toBeInTheDocument();
    });

    // Switch to definition mode
    await user.click(screen.getByRole("button", { name: "Phase 11 Custom Definition" }));

    // Fill form
    const defIdInput = screen.getByLabelText(/Definition ID/i);
    await user.type(defIdInput, "def_image_resizer");

    const taskNameInput = screen.getByLabelText(/Task Name \(Optional\)/i);
    await user.type(taskNameInput, "Custom Resizer Run");

    const execBtn = screen.getByRole("button", { name: /Execute Definition/i });
    await user.click(execBtn);

    await waitFor(() => {
      expect(screen.getByText("task-exec-8888")).toBeInTheDocument();
    });
  });

  it("inspects definition executions, immutable snapshot, and audit history on Definitions page", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);

      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k11",
          tenant_id: "tenant-delta",
          name: "dev-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }

      if (url.includes("/api/v1/definitions/def_pipeline/executions/task-1001/cancel")) {
        return jsonResponse({
          id: "task-1001",
          status: "CANCELLED",
          message: "Execution cancelled under active lease fencing.",
        });
      }

      if (url.includes("/api/v1/definitions/def_pipeline/executions/task-1001")) {
        return jsonResponse({
          id: "task-1001",
          task_name: "Pipeline Execution A",
          task_type: "data_pipeline",
          status: "RUNNING",
          priority: 2,
          definition_id: "def_pipeline",
          definition_version_id: "ver-snap-v1",
          version: {
            id: "ver-snap-v1",
            version_number: 1,
            task_type: "data_pipeline",
            content_hash: "sha256_mock_abc123",
          },
          payload: { dataset: "sales_q4" },
          result: null,
          failure_diagnosis: null,
          attempts: [
            {
              id: 1,
              attempt_number: 1,
              worker_id: "worker-node-1",
              status: "RUNNING",
              started_at: "2026-11-13T10:00:00Z",
              completed_at: null,
              duration_ms: 1540.2,
              error_message: null,
            },
          ],
          timeline: {
            created_at: "2026-11-13T09:59:50Z",
            started_at: "2026-11-13T10:00:00Z",
            completed_at: null,
          },
        });
      }

      if (url.includes("/api/v1/definitions/def_pipeline/executions")) {
        return jsonResponse({
          items: [
            {
              id: "task-1001",
              task_name: "Pipeline Execution A",
              task_type: "data_pipeline",
              status: "RUNNING",
              priority: 2,
              definition_id: "def_pipeline",
              definition_version_id: "ver-snap-v1",
              version_number: 1,
              created_at: "2026-11-13T09:59:50Z",
              started_at: "2026-11-13T10:00:00Z",
              completed_at: null,
              duration_ms: 1540.2,
            },
          ],
          total: 1,
          limit: 50,
          offset: 0,
        });
      }

      if (url.includes("/api/v1/definitions/def_pipeline/audit")) {
        return jsonResponse({
          items: [
            {
              id: "aud-001",
              tenant_id: "tenant-delta",
              definition_id: "def_pipeline",
              version_id: "ver-snap-v1",
              actor_id: "key-11",
              action: "DEFINITION_EXECUTE",
              metadata: { task_id: "task-1001", sanitized: true },
              created_at: "2026-11-13T09:59:50Z",
            },
          ],
          total: 1,
          limit: 50,
          offset: 0,
        });
      }

      return jsonResponse({});
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", "/definitions");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_key_11");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByLabelText(/Definition Identifier/i)).toBeInTheDocument();
    });

    const defInput = screen.getByLabelText(/Definition Identifier/i);
    await user.type(defInput, "def_pipeline");
    await user.click(screen.getByRole("button", { name: /Inspect Definition/i }));

    // Verify executions list table
    await waitFor(() => {
      expect(screen.getByText("Pipeline Execution A")).toBeInTheDocument();
      expect(screen.getByText("task-1001")).toBeInTheDocument();
      expect(screen.getByText("v1")).toBeInTheDocument();
    });

    // Inspect execution detail
    await user.click(screen.getByRole("button", { name: "Inspect" }));

    await waitFor(() => {
      expect(screen.getByText("Execution Inspection")).toBeInTheDocument();
      expect(screen.getByText("sha256_mock_abc123")).toBeInTheDocument();
      expect(screen.getByRole("button", { name: /Fenced Cancel/i })).toBeInTheDocument();
    });

    // Trigger fenced cancellation
    const cancelBtn = screen.getByRole("button", { name: /Fenced Cancel/i });
    await user.click(cancelBtn);
    await waitFor(() => {
      expect(screen.getByText(/Cancellation submitted:/i)).toBeInTheDocument();
    });

    // Switch to audit tab
    await user.click(screen.getByRole("button", { name: /Audit Trail/i }));
    await waitFor(() => {
      expect(screen.getByText("DEFINITION_EXECUTE")).toBeInTheDocument();
      expect(screen.getByText("key-11")).toBeInTheDocument();
    });
  });
});
