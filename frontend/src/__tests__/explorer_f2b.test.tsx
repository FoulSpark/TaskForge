import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";
import { sanitizeExplorerParams } from "../pages/TaskExplorerPage";

function jsonResponse(body: unknown, status = 200, headers?: Record<string, string>) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...(headers ?? {}),
    },
  });
}

function makeTask(id: string, name: string, status = "SUCCESS", priority = 2) {
  return {
    id,
    task_name: name,
    task_type: "compute_prime",
    payload: { n: 10 },
    status,
    priority,
    idempotency_key: null,
    retry_count: 0,
    max_retries: 3,
    timeout_seconds: 60,
    scheduled_at: null,
    started_at: null,
    completed_at: null,
    created_at: "2026-11-12T02:00:00Z",
    updated_at: "2026-11-12T02:00:01Z",
    attempts: [],
    result: null,
    events: [],
  };
}

describe("Gate F2-B — Task Explorer Server-Side Filtering, Pagination & URL Sync", () => {
  beforeEach(() => {
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
  });

  it("sanitizes invalid URL query parameters cleanly", () => {
    const { state, needsNormalization } = sanitizeExplorerParams(
      new URLSearchParams("status=BOGUS&priority=99&limit=500&offset=-10"),
    );
    expect(needsNormalization).toBe(true);
    expect(state).toEqual({
      status: "",
      taskType: "",
      priority: "",
      limit: 100,
      offset: 0,
    });
  });

  it("restores deep-linked filter and pagination params and syncs URL cleanly", async () => {
    const requestedUrls: string[] = [];

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requestedUrls.push(url);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2b",
          name: "f2b-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.includes("/api/v1/tasks?")) {
        return jsonResponse({
          items: [makeTask("deep-1", "Deep Linked Failed Task", "FAILED", 3)],
          total: 25,
          limit: 10,
          offset: 20,
          has_more: true,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState(
      {},
      "",
      "/tasks?status=FAILED&task_type=compute_prime&priority=3&limit=10&offset=20",
    );

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_f2b_deep");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Deep Linked Failed Task")).toBeInTheDocument();
    });

    expect(screen.getByLabelText<HTMLSelectElement>(/Status/i).value).toBe("FAILED");
    expect(screen.getByLabelText<HTMLInputElement>(/Task Type/i).value).toBe(
      "compute_prime",
    );
    expect(screen.getByLabelText<HTMLSelectElement>(/Priority/i).value).toBe("3");
    expect(screen.getByLabelText<HTMLSelectElement>(/Page Size/i).value).toBe("10");
    expect(screen.getByText("Showing 21–21 of 25")).toBeInTheDocument();

    const taskQuery = requestedUrls.find((u) => u.includes("/api/v1/tasks?"));
    expect(taskQuery).toContain("status=FAILED");
    expect(taskQuery).toContain("task_type=compute_prime");
    expect(taskQuery).toContain("priority=3");
    expect(taskQuery).toContain("limit=10");
    expect(taskQuery).toContain("offset=20");
  });

  it("resets offset to zero when filters change, prevents stale rows during empty transitions, and preserves valid rows on 429/503 refresh failures", async () => {
    let nextRefreshError: "429" | "503" | null = null;
    const requestedUrls: string[] = [];

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      requestedUrls.push(url);

      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "k1",
          tenant_id: "tenant-f2b",
          name: "f2b-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.includes("/api/v1/tasks?")) {
        if (nextRefreshError === "429") {
          return jsonResponse(
            { error: { code: "RATE_LIMIT_EXCEEDED", message: "Too many requests" } },
            429,
            { "Retry-After": "7" },
          );
        }
        if (nextRefreshError === "503") {
          return jsonResponse(
            { error: { code: "SERVICE_UNAVAILABLE", message: "Redis unavailable" } },
            503,
            { "Retry-After": "9" },
          );
        }

        const sp = new URLSearchParams(url.split("?")[1] ?? "");
        const status = sp.get("status");
        const offset = Number(sp.get("offset") ?? "0");

        if (status === "DEAD_LETTER") {
          return jsonResponse({
            items: [],
            total: 0,
            limit: 20,
            offset: 0,
            has_more: false,
          });
        }
        if (offset === 20) {
          return jsonResponse({
            items: [makeTask("p2-1", "Page Two Job", "QUEUED", 2)],
            total: 25,
            limit: 20,
            offset: 20,
            has_more: false,
          });
        }
        return jsonResponse({
          items: [makeTask("p1-1", "Page One Job", "SUCCESS", 2)],
          total: 25,
          limit: 20,
          offset: 0,
          has_more: true,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    window.history.pushState({}, "", "/tasks");
    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_f2b_nav");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Page One Job")).toBeInTheDocument();
    });

    // Advance to page 2 -> offset=20 reflected in URL and table
    await user.click(screen.getByRole("button", { name: /^Next$/i }));
    await waitFor(() => {
      expect(screen.getByText("Page Two Job")).toBeInTheDocument();
    });
    expect(window.location.search).toContain("offset=20");

    // 429 on Refresh preserves last valid rows ("Page Two Job") + displays Rate Limit banner
    nextRefreshError = "429";
    await user.click(screen.getByRole("button", { name: /^Refresh$/i }));
    await waitFor(() => {
      expect(screen.getByText(/Rate limited \(429\): Polling paused for 7s/i)).toBeInTheDocument();
    });
    expect(screen.getByText("Page Two Job")).toBeInTheDocument();

    // 503 on Refresh preserves last valid rows + displays 503 unavailable banner
    nextRefreshError = "503";
    await user.click(screen.getByRole("button", { name: /^Refresh$/i }));
    await waitFor(() => {
      expect(screen.getByText(/Service unavailable \(503\): Retry in 9s/i)).toBeInTheDocument();
    });
    expect(screen.getByText("Page Two Job")).toBeInTheDocument();

    // Recover and switch filter to DEAD_LETTER -> resets offset=0, clears stale Page Two Job, shows Reset Filters CTA
    nextRefreshError = null;
    await user.selectOptions(screen.getByLabelText(/Status/i), "DEAD_LETTER");

    await waitFor(() => {
      expect(screen.getByText(/No tasks match the current filters/i)).toBeInTheDocument();
    });
    expect(window.location.search).toBe("?status=DEAD_LETTER");
    expect(screen.queryByText("Page Two Job")).not.toBeInTheDocument();

    const dlqRequest = requestedUrls.find((u) => u.includes("status=DEAD_LETTER"));
    expect(dlqRequest).toContain("offset=0");

    // Clicking Reset Filters restores unfiltered Page One cleanly
    await user.click(screen.getByRole("button", { name: /Reset Filters/i }));
    await waitFor(() => {
      expect(screen.getByText("Page One Job")).toBeInTheDocument();
    });
    expect(window.location.search).toBe("");
  });
});

