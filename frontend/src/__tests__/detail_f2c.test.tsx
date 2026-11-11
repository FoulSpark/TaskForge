import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";
import {
  canCancelStatus,
  canRetryStatus,
  isImmutableTerminalStatus,
  isTerminalStatus,
} from "../api/lifecycle";
import type { TaskStatus } from "../api/types";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

describe("Gate F2-C — Task Detail, Diagnostics & Full 8-State Lifecycle Matrix", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("enforces positive and negative action eligibility across all 8 TaskStatus values", () => {
    const matrix: Array<{
      status: TaskStatus;
      terminal: boolean;
      immutable: boolean;
      canCancel: boolean;
      canRetry: boolean;
    }> = [
      { status: "PENDING", terminal: false, immutable: false, canCancel: true, canRetry: false },
      { status: "QUEUED", terminal: false, immutable: false, canCancel: true, canRetry: false },
      { status: "RUNNING", terminal: false, immutable: false, canCancel: true, canRetry: false },
      { status: "RETRY_WAIT", terminal: false, immutable: false, canCancel: true, canRetry: false },
      { status: "FAILED", terminal: false, immutable: false, canCancel: false, canRetry: true },
      { status: "DEAD_LETTER", terminal: true, immutable: false, canCancel: false, canRetry: true },
      { status: "SUCCESS", terminal: true, immutable: true, canCancel: false, canRetry: false },
      { status: "CANCELLED", terminal: true, immutable: true, canCancel: false, canRetry: false },
    ];

    for (const row of matrix) {
      expect(isTerminalStatus(row.status)).toBe(row.terminal);
      expect(isImmutableTerminalStatus(row.status)).toBe(row.immutable);
      expect(canCancelStatus(row.status)).toBe(row.canCancel);
      expect(canRetryStatus(row.status)).toBe(row.canRetry);
    }
  });

  it("renders sorted attempts, chronological events, separated handler vs lease tenure, diagnostics, result metadata, and redacts any secret-like keys", async () => {
    const taskId = "22222222-3333-4444-5555-666666666666";
    // Out-of-order attempts & events matching exact backend TaskResponse schema
    const richTask = {
      id: taskId,
      task_name: "Retry Recovery Task",
      task_type: "db_process",
      payload: { batch_size: 25 },
      status: "DEAD_LETTER",
      priority: 4,
      idempotency_key: "batch-25",
      retry_count: 2,
      max_retries: 2,
      timeout_seconds: 45,
      scheduled_at: null,
      created_at: "2026-11-12T01:00:00.000Z",
      started_at: "2026-11-12T01:00:01.000Z",
      completed_at: "2026-11-12T01:00:03.500Z",
      updated_at: "2026-11-12T01:00:03.500Z",
      attempts: [
        {
          id: 22,
          attempt_number: 2,
          worker_id: "worker-beta",
          status: "FAILED",
          started_at: "2026-11-12T01:00:03.000Z",
          completed_at: "2026-11-12T01:00:03.400Z",
          duration_ms: 42.5,
          error_message: "ConnectionError: upstream replica timeout",
        },
        {
          id: 21,
          attempt_number: 1,
          worker_id: "worker-alpha",
          status: "FAILED",
          started_at: "2026-11-12T01:00:01.000Z",
          completed_at: "2026-11-12T01:00:01.200Z",
          duration_ms: 19.0,
          error_message: "DeadlockDetected on shard 3",
        },
      ],
      result: {
        id: 88,
        return_value: { partial_rows: 12 },
        execution_metadata: {
          rows_scanned: 25,
          lease_token: "should-be-redacted-if-ever-present",
        },
        created_at: "2026-11-12T01:00:03.450Z",
      },
      events: [
        {
          id: 303,
          event_type: "MOVED_TO_DLQ",
          from_state: "FAILED",
          to_state: "DEAD_LETTER",
          metadata_json: {
            reason: "max_retries_exhausted",
            fencing_token: "secret-fence-val",
          },
          created_at: "2026-11-12T01:00:03.500Z",
        },
        {
          id: 302,
          event_type: "TASK_DISPATCHED",
          from_state: "PENDING",
          to_state: "QUEUED",
          metadata_json: { outbox_id: "ob-1" },
          created_at: "2026-11-12T01:00:00.500Z",
        },
        {
          id: 301,
          event_type: "TASK_CREATED",
          from_state: null,
          to_state: "PENDING",
          metadata_json: { actor: "api-key" },
          created_at: "2026-11-12T01:00:00.500Z",
        },
      ],
    };

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2c",
          name: "f2c-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith(`/api/v1/tasks/${taskId}`)) {
        return jsonResponse(richTask);
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", `/tasks/${taskId}`);
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_f2c_key");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Retry Recovery Task")).toBeInTheDocument();
    });

    // DEAD_LETTER is terminal yet NOT immutable -> Manual Retry must be offered
    expect(
      screen.getByRole("button", { name: /Manual Retry \(Re-queue\)/i }),
    ).toBeInTheDocument();
    expect(screen.queryByText("Immutable (Terminal)")).not.toBeInTheDocument();

    // Separated Timing Breakdown checks:
    // Final attempt duration_ms = 42.5 ms
    // Worker lease tenure = completed_at (03.500) - final attempt started_at (03.000) = 500.0 ms
    // End-to-End elapsed = completed_at (03.500) - created_at (00.000) = 3500.0 ms
    const timingSection = screen.getByRole("region", {
      name: /Execution Timing Breakdown/i,
    });
    expect(within(timingSection).getByText("42.5 ms")).toBeInTheDocument();
    expect(within(timingSection).getByText("500.0 ms")).toBeInTheDocument();
    expect(within(timingSection).getByText("3500.0 ms")).toBeInTheDocument();

    // Failure Diagnostics card surfaces both failed attempts cleanly
    const diagSection = screen.getByRole("region", { name: /Failure Diagnostics/i });
    expect(
      within(diagSection).getByText(/DeadlockDetected on shard 3/i),
    ).toBeInTheDocument();
    expect(
      within(diagSection).getByText(/ConnectionError: upstream replica timeout/i),
    ).toBeInTheDocument();

    // Result inspection renders return_value & execution_metadata cleanly without result_size_bytes
    const resultSection = screen.getByRole("region", { name: /Execution Result/i });
    expect(within(resultSection).getByText(/partial_rows/i)).toBeInTheDocument();
    expect(within(resultSection).getByText(/rows_scanned/i)).toBeInTheDocument();
    expect(within(resultSection).queryByText(/bytes/i)).not.toBeInTheDocument();

    // Attempt History is sorted ascending (#1 before #2)
    const attemptsSection = screen.getByRole("region", { name: /Attempt History/i });
    const attemptRows = within(attemptsSection).getAllByRole("row").slice(1);
    expect(attemptRows).toHaveLength(2);
    expect(attemptRows[0]).toHaveTextContent("#1");
    expect(attemptRows[0]).toHaveTextContent("worker-alpha");
    expect(attemptRows[1]).toHaveTextContent("#2");
    expect(attemptRows[1]).toHaveTextContent("worker-beta");

    // Lifecycle Events sorted deterministically by (created_at, id): 301 -> 302 -> 303
    const eventsSection = screen.getByRole("region", { name: /Lifecycle Events/i });
    const eventRows = within(eventsSection).getAllByRole("row").slice(1);
    expect(eventRows).toHaveLength(3);
    expect(eventRows[0]).toHaveTextContent("TASK_CREATED");
    expect(eventRows[1]).toHaveTextContent("TASK_DISPATCHED");
    expect(eventRows[1]).toHaveTextContent("PENDING → QUEUED");
    expect(eventRows[2]).toHaveTextContent("MOVED_TO_DLQ");
    expect(eventRows[2]).toHaveTextContent("max_retries_exhausted");

    // Secret/fencing values redacted cleanly
    expect(document.body.textContent ?? "").not.toContain(
      "should-be-redacted-if-ever-present",
    );
    expect(document.body.textContent ?? "").not.toContain("secret-fence-val");
  });
});
