import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function makeTask(overrides: Record<string, unknown> = {}) {
  return {
    id: "11111111-1111-1111-1111-111111111111",
    task_name: "Lifecycle Task",
    task_type: "compute_prime",
    payload: { n: 50 },
    status: "QUEUED",
    priority: 2,
    tenant_id: "tenant-f2a",
    idempotency_key: null,
    retry_count: 0,
    max_retries: 3,
    timeout_seconds: 60,
    scheduled_at: null,
    started_at: null,
    completed_at: null,
    created_at: "2026-11-12T02:00:00Z",
    updated_at: "2026-11-12T02:00:00Z",
    error_message: null,
    error_type: null,
    attempts: [],
    result: null,
    events: [],
    ...overrides,
  };
}

const CATALOG_RESPONSE = {
  items: [
    {
      task_type: "compute_prime",
      display_name: "Compute Primes",
      category: "workload",
      description: "Prime computation",
      default_payload: { n: 50 },
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
};

describe("Gate F2-A — Task Lifecycle Workflows", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("handles 200 idempotent replay and 409 conflict without losing edits or claiming duplicates", async () => {
    const existingTask = makeTask({
      id: "task-replay-200",
      task_name: "Existing Idempotent Job",
      status: "QUEUED",
      idempotency_key: "idem-abc-123",
    });

    let postCount = 0;
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2a",
          name: "f2a-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          timestamp: 1,
          scope: "tenant",
          tenant_id: "tenant-f2a",
          tasks: {
            total: 1,
            pending: 0,
            queued: 1,
            running: 0,
            succeeded: 0,
            failed: 0,
            retry_wait: 0,
            dead_letter: 0,
            cancelled: 0,
          },
          performance: { avg_execution_ms: 0, queue_depth: null },
          workers: null,
        });
      }
      if (url.endsWith("/api/v1/task-types")) {
        return jsonResponse(CATALOG_RESPONSE);
      }
      if (url.endsWith("/api/v1/tasks") && init?.method === "POST") {
        postCount += 1;
        if (postCount === 1) {
          return jsonResponse(
            {
              error: {
                code: "IDEMPOTENCY_CONFLICT",
                message: "Idempotency key collision with conflicting payload",
              },
            },
            409,
          );
        }
        return jsonResponse(existingTask, 200);
      }
      if (url.includes(`/api/v1/tasks/${existingTask.id}`)) {
        return jsonResponse(existingTask);
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({ items: [], total: 0, limit: 8, offset: 0, has_more: false });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_f2a_key");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByRole("link", { name: /^Submit Task$/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("link", { name: /^Submit Task$/i }));

    await waitFor(() => {
      expect(screen.getByText(/At-least-once delivery:/i)).toBeInTheDocument();
    });

    await user.clear(screen.getByLabelText(/Task Name/i));
    await user.type(screen.getByLabelText(/Task Name/i), "Conflicting Retry Submit");
    await user.type(screen.getByLabelText(/Idempotency Key/i), "idem-abc-123");
    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));

    // 409 conflict stays on Submit form with edits preserved
    await waitFor(() => {
      expect(screen.getByText(/Idempotency conflict \(409\)/i)).toBeInTheDocument();
    });
    expect(screen.getByLabelText<HTMLInputElement>(/Task Name/i).value).toBe(
      "Conflicting Retry Submit",
    );
    expect(screen.getByLabelText<HTMLInputElement>(/Idempotency Key/i).value).toBe(
      "idem-abc-123",
    );

    // Matching replay returns HTTP 200 -> opens existing task with explicit replay banner
    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));
    await waitFor(() => {
      expect(screen.getByText(/Idempotent replay \(HTTP 200\)/i)).toBeInTheDocument();
    });
    expect(screen.getByText("Existing Idempotent Job")).toBeInTheDocument();
  });

  it("validates future scheduled_at, rejects past timestamps, and renders scheduled PENDING wait state", async () => {
    const capturedBodies: Array<Record<string, unknown>> = [];
    const scheduledTask = makeTask({
      id: "task-sched-1",
      task_name: "Nightly Scheduled Job",
      status: "PENDING",
      scheduled_at: "2030-01-01T12:00:00.000Z",
    });

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2a",
          name: "f2a-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          timestamp: 1,
          scope: "tenant",
          tenant_id: "tenant-f2a",
          tasks: {
            total: 0,
            pending: 0,
            queued: 0,
            running: 0,
            succeeded: 0,
            failed: 0,
            retry_wait: 0,
            dead_letter: 0,
            cancelled: 0,
          },
          performance: { avg_execution_ms: 0, queue_depth: null },
          workers: null,
        });
      }
      if (url.endsWith("/api/v1/task-types")) {
        return jsonResponse(CATALOG_RESPONSE);
      }
      if (url.endsWith("/api/v1/tasks") && init?.method === "POST") {
        capturedBodies.push(JSON.parse(String(init.body)));
        return jsonResponse(scheduledTask, 201);
      }
      if (url.includes(`/api/v1/tasks/${scheduledTask.id}`)) {
        return jsonResponse(scheduledTask);
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({ items: [], total: 0, limit: 8, offset: 0, has_more: false });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_sched");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));
    await waitFor(() => {
      expect(screen.getByRole("link", { name: /^Submit Task$/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("link", { name: /^Submit Task$/i }));

    await waitFor(() => {
      expect(screen.getByLabelText(/Execution Schedule/i)).toBeInTheDocument();
    });
    await user.selectOptions(screen.getByLabelText(/Execution Schedule/i), "scheduled");

    const dtInput = screen.getByLabelText(/Scheduled Time/i);
    // Past date rejected client-side without POSTing
    fireEvent.change(dtInput, { target: { value: "2020-01-01T10:00" } });
    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));
    await waitFor(() => {
      expect(screen.getByText(/must be in the future/i)).toBeInTheDocument();
    });
    expect(capturedBodies.length).toBe(0);

    // Future date accepted and sent as UTC ISO scheduled_at
    fireEvent.change(dtInput, { target: { value: "2030-01-01T12:00" } });
    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));

    await waitFor(() => {
      expect(screen.getByText("Nightly Scheduled Job")).toBeInTheDocument();
    });
    expect(capturedBodies.length).toBe(1);
    expect(typeof capturedBodies[0].scheduled_at).toBe("string");
    expect(
      screen.getByText(/Waiting — Scheduled Future Execution \(Inactive\)/i),
    ).toBeInTheDocument();
  });

  it("enforces retry/cancel state rules and reconciles 409 conflicts without optimistic state updates", async () => {
    const queuedTask = makeTask({ id: "task-conflict", status: "QUEUED" });
    const nowSucceededTask = makeTask({
      id: "task-conflict",
      status: "SUCCESS",
      completed_at: "2026-11-12T02:01:00Z",
    });
    const explorerItems = [
      makeTask({ id: "t-canc", task_name: "Cancelled Job", status: "CANCELLED" }),
      makeTask({ id: "t-rwait", task_name: "Backoff Job", status: "RETRY_WAIT" }),
      makeTask({ id: "t-dlq", task_name: "DLQ Job", status: "DEAD_LETTER" }),
      makeTask({ id: "t-fail", task_name: "Failed Job", status: "FAILED" }),
    ];

    let conflictedOnce = false;

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2a",
          name: "f2a-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          timestamp: 1,
          scope: "tenant",
          tenant_id: "tenant-f2a",
          tasks: {
            total: 4,
            pending: 0,
            queued: 0,
            running: 0,
            succeeded: 0,
            failed: 1,
            retry_wait: 1,
            dead_letter: 1,
            cancelled: 1,
          },
          performance: { avg_execution_ms: 0, queue_depth: null },
          workers: null,
        });
      }
      if (url.endsWith("/api/v1/tasks/task-conflict/cancel") && init?.method === "POST") {
        conflictedOnce = true;
        return jsonResponse(
          {
            error: {
              code: "HTTP_409",
              message: "Cannot transition task from 'SUCCESS' to 'CANCELLED'",
            },
          },
          409,
        );
      }
      if (url.endsWith("/api/v1/tasks/task-conflict")) {
        return jsonResponse(conflictedOnce ? nowSucceededTask : queuedTask);
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({
          items: explorerItems,
          total: explorerItems.length,
          limit: 20,
          offset: 0,
          has_more: false,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_f2a_conflict");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    // Go to Task Explorer and check exact action button matrix per state
    await waitFor(() => {
      expect(screen.getByRole("link", { name: /Task Explorer/i })).toBeInTheDocument();
    });
    await user.click(screen.getByRole("link", { name: /Task Explorer/i }));

    await waitFor(() => {
      expect(screen.getByText("Cancelled Job")).toBeInTheDocument();
    });

    // CANCELLED -> Immutable label, zero Cancel/Retry buttons
    // RETRY_WAIT -> Backoff wait indicator + Cancel only (zero Retry)
    // DEAD_LETTER + FAILED -> 2 Retry buttons total
    expect(screen.getAllByRole("button", { name: /^Retry$/i })).toHaveLength(2);
    expect(screen.getAllByRole("button", { name: /^Cancel$/i })).toHaveLength(1);
    expect(screen.getByText(/Backoff wait \(inactive\)/i)).toBeInTheDocument();
    expect(screen.getByText("Immutable")).toBeInTheDocument();

    // Navigate directly to detail page of QUEUED task that races to SUCCESS before Cancel
    await act(async () => {
      window.history.pushState({}, "", "/tasks/task-conflict");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /Cancel Task/i })).toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: /Cancel Task/i }));

    // 409 triggers authoritative refresh -> now renders SUCCESS & Immutable (Terminal)
    await waitFor(() => {
      expect(screen.getByText(/State conflict \(409\)/i)).toBeInTheDocument();
    });
    expect(screen.getByText("Immutable (Terminal)")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Cancel Task/i })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Manual Retry/i })).not.toBeInTheDocument();
  });
});

