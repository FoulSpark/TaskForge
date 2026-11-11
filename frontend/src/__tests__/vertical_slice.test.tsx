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

describe("TaskForge Vertical Slice UI Workflow & Monitoring", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("supports sign in -> submit task -> view task details & result -> monitoring", async () => {
    const createdTask = {
      id: "11111111-2222-3333-4444-555555555555",
      task_name: "Prime Sieve Job",
      task_type: "compute_prime",
      payload: { n: 25 },
      status: "SUCCESS",
      priority: 3,
      idempotency_key: null,
      retry_count: 0,
      max_retries: 3,
      timeout_seconds: 60,
      scheduled_at: null,
      started_at: "2026-11-12T01:00:01Z",
      completed_at: "2026-11-12T01:00:02Z",
      created_at: "2026-11-12T01:00:00Z",
      updated_at: "2026-11-12T01:00:02Z",
      attempts: [
        {
          id: 1,
          attempt_number: 1,
          worker_id: "worker-test-1",
          status: "SUCCESS",
          started_at: "2026-11-12T01:00:01Z",
          completed_at: "2026-11-12T01:00:02Z",
          duration_ms: 4.2,
          error_message: null,
        },
      ],
      result: {
        id: 10,
        return_value: { count: 9, primes_found: 9 },
        execution_metadata: { worker_id: "worker-test-1" },
        created_at: "2026-11-12T01:00:02Z",
      },
      events: [
        {
          id: 100,
          event_type: "TASK_SUBMITTED",
          from_state: null,
          to_state: "QUEUED",
          metadata_json: { source: "api" },
          created_at: "2026-11-12T01:00:00Z",
        },
      ],
    };

    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k-prod",
          tenant_id: "tenant-prod",
          name: "prod-admin",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          scope: "tenant",
          tenant_id: "tenant-prod",
          tasks: {
            total: 1,
            pending: 0,
            queued: 0,
            running: 0,
            succeeded: 1,
            failed: 0,
            retry_wait: 0,
            dead_letter: 0,
            cancelled: 0,
          },
          performance: { avg_execution_ms: 4.2, queue_depth: null },
          workers: null,
        });
      }
      if (url.endsWith("/api/v1/task-types")) {
        return jsonResponse({
          items: [
            {
              task_type: "compute_prime",
              display_name: "Compute Primes",
              category: "workload",
              description: "Prime computation",
              default_payload: { n: 25 },
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
      if (url.endsWith("/api/v1/tasks") && init?.method === "POST") {
        return jsonResponse(createdTask, 201);
      }
      if (url.includes(`/api/v1/tasks/${createdTask.id}`)) {
        return jsonResponse(createdTask);
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({
          items: [createdTask],
          total: 1,
          limit: 8,
          offset: 0,
          has_more: false,
        });
      }
      if (url.includes("/api/v1/monitoring/summary")) {
        return jsonResponse({
          timestamp_utc: "2026-11-12T01:00:05Z",
          window_minutes: 5,
          window_seconds: 300,
          tenant_id: "tenant-prod",
          health: { overall: "healthy", database: "healthy", redis: "healthy" },
          throughput: {
            observation_count: 1,
            tasks_per_second: 0.0033,
            tasks_per_minute: 0.2,
          },
          latencies_ms: {
            outbox_dwell: {
              sample_count: 1,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "published_submitted_outbox_events",
              p50_ms: 12.0,
              p95_ms: 12.0,
              p99_ms: 12.0,
            },
            queue_dwell: {
              sample_count: 1,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "completed_initial_attempts_with_outbox",
              p50_ms: 40.0,
              p95_ms: 40.0,
              p99_ms: 40.0,
            },
            submission_to_start: {
              sample_count: 1,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "completed_attempts_only",
              p50_ms: 52.0,
              p95_ms: 52.0,
              p99_ms: 52.0,
            },
            handler_duration: {
              sample_count: 1,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "completed_attempts_only",
              p50_ms: 4.2,
              p95_ms: 4.2,
              p99_ms: 4.2,
            },
            worker_lease_tenure: {
              sample_count: 1,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "completed_tasks_with_final_attempt",
              p50_ms: 100.0,
              p95_ms: 100.0,
              p99_ms: 100.0,
            },
            end_to_end: {
              sample_count: 0,
              sufficient_sample_size: false,
              minimum_recommended_samples: 20,
              method: "exact_sql_observations",
              population: "completed_tasks_only",
              p50_ms: null,
              p95_ms: null,
              p99_ms: null,
            },
          },
          cluster: null,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_prod_key");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByRole("link", { name: /\+ Submit Task/i })).toBeInTheDocument();
    });

    await user.click(screen.getByRole("link", { name: /\+ Submit Task/i }));
    await waitFor(() => {
      expect(screen.getByLabelText(/Task Name/i)).toBeInTheDocument();
    });

    await user.clear(screen.getByLabelText(/Task Name/i));
    await user.type(screen.getByLabelText(/Task Name/i), "Prime Sieve Job");
    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));

    // Detail view shows worker ID, attempts, and execution result JSON (no lease token)
    await waitFor(() => {
      expect(screen.getByText(/primes_found/i)).toBeInTheDocument();
    });
    expect(screen.getAllByText(/worker-test-1/i).length).toBeGreaterThanOrEqual(1);

    // Navigate to Monitoring -> verify low-sample confidence + no observations indicators
    await user.click(screen.getByRole("link", { name: /Monitoring/i }));
    await waitFor(() => {
      expect(screen.getByText(/Outbox Dwell/i)).toBeInTheDocument();
    });
    expect(screen.getAllByText(/Low N \(<20\)/i).length).toBeGreaterThanOrEqual(1);
    expect(screen.getByText(/No observations/i)).toBeInTheDocument();
  });
});

