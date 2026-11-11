import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}

function makeMetric(
  population: string,
  sampleCount: number,
  p50: number | null,
  p95: number | null,
  p99: number | null,
  minRecommended = 20,
) {
  return {
    sample_count: sampleCount,
    sufficient_sample_size: sampleCount >= minRecommended,
    minimum_recommended_samples: minRecommended,
    method: "exact_sql_observations",
    population,
    p50_ms: p50,
    p95_ms: p95,
    p99_ms: p99,
  };
}

describe("Gate F2-D — Operational Monitoring UX, Sample Sufficiency & Scope Boundaries", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("renders population labels, 0/low/exact-min/above-min sample states, independent null quantiles, and non-operator cluster suppression", async () => {
    // Non-operator key receiving a malicious/unexpected cluster payload MUST still hide cluster data!
    const monitoringResponse = {
      timestamp_utc: "2026-11-12T02:40:00Z",
      window_minutes: 15,
      window_seconds: 900,
      tenant_id: "tenant-std",
      health: { overall: "healthy", database: "healthy", redis: "healthy" },
      throughput: {
        observation_count: 25,
        tasks_per_second: 0.0278,
        tasks_per_minute: 1.6667,
      },
      latencies_ms: {
        outbox_dwell: makeMetric("published_submitted_outbox_events", 0, null, null, null),
        queue_dwell: makeMetric("completed_initial_attempts_with_outbox", 5, 11.2, null, 44.8),
        submission_to_start: makeMetric("completed_attempts_only", 19, 15.0, 29.0, 48.0),
        handler_duration: makeMetric("completed_attempts_only", 20, 4.1, 8.2, 12.3),
        worker_lease_tenure: makeMetric("completed_tasks_with_final_attempt", 25, 9.5, 18.0, 31.4),
        end_to_end: makeMetric("completed_tasks_only", 25, 28.0, 49.0, 77.2),
      },
      // Inject cluster object to prove frontend enforces admin:operator boundary defensively
      cluster: {
        queue_depth_by_priority: { critical: 99, high: 0, medium: 0, low: 0 },
        outbox_pending_events: 999,
        outbox_lag_seconds: 12.5,
        workers: { active_count: 8, total_concurrency: 32, active_tasks: 4 },
        db_pool: { checked_out: 3, idle: 7, overflow: 0 },
      },
    };

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k-std",
          tenant_id: "tenant-std",
          name: "std-reader",
          scopes: ["tasks:read"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.includes("/api/v1/monitoring/summary")) {
        return jsonResponse(monitoringResponse);
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", "/monitoring");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_std_mon");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Outbox Dwell")).toBeInTheDocument();
    });

    const table = screen.getByRole("table");
    const rows = within(table).getAllByRole("row").slice(1);
    expect(rows).toHaveLength(6);

    // 1. outbox_dwell: sample_count=0 -> "No observations", all quantiles "—"
    expect(rows[0]).toHaveTextContent("published_submitted_outbox_events");
    expect(rows[0]).toHaveTextContent("0 / min 20");
    expect(rows[0]).toHaveTextContent("No observations");
    expect(within(rows[0]).getAllByText("—")).toHaveLength(3);

    // 2. queue_dwell: sample_count=5 -> "Low N (<20)", independent null p95="—" while p50 & p99 show
    expect(rows[1]).toHaveTextContent("completed_initial_attempts_with_outbox");
    expect(rows[1]).toHaveTextContent("5 / min 20");
    expect(rows[1]).toHaveTextContent("Low N (<20)");
    expect(rows[1]).toHaveTextContent("Valid metric; limited quantile evidence");
    expect(rows[1]).toHaveTextContent("11.2 ms");
    expect(within(rows[1]).getAllByText("—")).toHaveLength(1);
    expect(rows[1]).toHaveTextContent("44.8 ms");

    // 3. submission_to_start: sample_count=19 (below min=20) -> Low N (<20)
    expect(rows[2]).toHaveTextContent("19 / min 20");
    expect(rows[2]).toHaveTextContent("Low N (<20)");

    // 4. handler_duration: sample_count=20 (exact min=20) -> Sufficient (>=20)
    expect(rows[3]).toHaveTextContent("20 / min 20");
    expect(rows[3]).toHaveTextContent("Sufficient (≥20)");

    // 5. worker_lease_tenure: sample_count=25 (above min=20) -> Sufficient (>=20), distinct from handler_duration
    expect(rows[4]).toHaveTextContent("25 / min 20");
    expect(rows[4]).toHaveTextContent("9.5 ms");
    expect(rows[4]).toHaveTextContent("Sufficient (≥20)");

    // Operator cluster section MUST NOT render for non-operator even when mocked JSON includes cluster
    expect(screen.queryByText("999")).not.toBeInTheDocument();
    const boundarySection = screen.getByRole("region", {
      name: /Operator Telemetry Boundary/i,
    });
    expect(boundarySection).toBeInTheDocument();
    expect(boundarySection).toHaveTextContent(/require the admin:operator scope/i);

    // Window switching issues request with ?window_minutes=60
    await user.selectOptions(screen.getByLabelText(/Window:/i), "60");
    await waitFor(() => {
      expect(
        fetchMock.mock.calls.some(([u]) =>
          String(u).includes("/api/v1/monitoring/summary?window_minutes=60"),
        ),
      ).toBe(true);
    });
  });

  it("handles operator with null cluster cleanly without fabricating metrics, then renders live cluster when non-null", async () => {
    let returnCluster = false;

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k-op",
          tenant_id: "tenant-op",
          name: "op-admin",
          scopes: ["tasks:read", "tasks:write", "admin:operator"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.includes("/api/v1/monitoring/summary")) {
        return jsonResponse({
          timestamp_utc: "2026-11-12T02:40:00Z",
          window_minutes: 5,
          window_seconds: 300,
          tenant_id: "tenant-op",
          health: { overall: "healthy", database: "healthy", redis: "healthy" },
          throughput: {
            observation_count: 0,
            tasks_per_second: 0,
            tasks_per_minute: 0,
          },
          latencies_ms: {
            outbox_dwell: makeMetric("published_submitted_outbox_events", 0, null, null, null),
            queue_dwell: makeMetric("completed_initial_attempts_with_outbox", 0, null, null, null),
            submission_to_start: makeMetric("completed_attempts_only", 0, null, null, null),
            handler_duration: makeMetric("completed_attempts_only", 0, null, null, null),
            worker_lease_tenure: makeMetric("completed_tasks_with_final_attempt", 0, null, null, null),
            end_to_end: makeMetric("completed_tasks_only", 0, null, null, null),
          },
          cluster: returnCluster
            ? {
                queue_depth_by_priority: { critical: 1, high: 2, medium: 3, low: 4 },
                outbox_pending_events: 7,
                outbox_lag_seconds: 0.42,
                workers: { active_count: 3, total_concurrency: 12, active_tasks: 2 },
                db_pool: { checked_out: 1, idle: 9, overflow: 0 },
              }
            : null,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", "/monitoring");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_op_mon");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    // Initially cluster === null -> explicit unavailable message, zero fabricated metrics
    await waitFor(() => {
      expect(screen.getByText(/cluster: null/i)).toBeInTheDocument();
    });
    expect(screen.queryByText(/Pending Outbox Events/i)).not.toBeInTheDocument();

    // Now operator receives real cluster snapshot on Refresh
    returnCluster = true;
    await user.click(screen.getByRole("button", { name: /^Refresh$/i }));

    await waitFor(() => {
      expect(screen.getByText(/Pending Outbox Events/i)).toBeInTheDocument();
    });
    expect(screen.getByText("0.42s", { exact: false })).toBeInTheDocument();
    expect(screen.getByText("C:1 • H:2 • M:3 • L:4")).toBeInTheDocument();
  });
});
