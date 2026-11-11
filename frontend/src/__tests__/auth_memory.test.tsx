import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { App } from "../App";

function jsonResponse(body: unknown, status = 200, headers?: Record<string, string>) {
  return new Response(JSON.stringify(body), {
    status,
    headers: {
      "Content-Type": "application/json",
      ...(headers ?? {}),
    },
  });
}

describe("In-memory auth lifecycle & R3 polling isolation", () => {
  beforeEach(() => {
    window.localStorage.clear();
    window.sessionStorage.clear();
    window.history.pushState({}, "", "/");
  });

  afterEach(() => {
    vi.restoreAllMocks();
    vi.useRealTimers();
  });

  it("never writes API keys to localStorage, sessionStorage, or cookies", async () => {
    const setItemSpy = vi.spyOn(Storage.prototype, "setItem");

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "key-1",
          tenant_id: "tenant-alpha",
          name: "alpha-rw",
          scopes: ["tasks:read", "tasks:write"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          scope: "tenant",
          tenant_id: "tenant-alpha",
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
          performance: { avg_execution_ms: 12.5, queue_depth: null },
          workers: null,
        });
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({ items: [], total: 0, limit: 8, offset: 0, has_more: false });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_supersecret_key");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getAllByText(/tenant-alpha/i).length).toBeGreaterThanOrEqual(1);
    });

    await user.click(screen.getByRole("button", { name: /Sign Out/i }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /Sign In/i })).toBeInTheDocument();
    });

    expect(setItemSpy).not.toHaveBeenCalled();
    expect(window.localStorage.length).toBe(0);
    expect(window.sessionStorage.length).toBe(0);
    expect(document.cookie).toBe("");
  });

  it("stops polling immediately and clears cached tenant data when 401 arrives mid-polling", async () => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
    let statsCalls = 0;

    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "key-1",
          tenant_id: "tenant-revoke",
          name: "revoke-me",
          scopes: ["tasks:read"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        statsCalls += 1;
        if (statsCalls === 1) {
          return jsonResponse({
            scope: "tenant",
            tenant_id: "tenant-revoke",
            tasks: {
              total: 7,
              pending: 0,
              queued: 0,
              running: 0,
              succeeded: 7,
              failed: 0,
              retry_wait: 0,
              dead_letter: 0,
              cancelled: 0,
            },
            performance: { avg_execution_ms: 15.0, queue_depth: null },
            workers: null,
          });
        }
        return jsonResponse(
          { error: { code: "HTTP_401", message: "API key revoked" } },
          401,
        );
      }
      if (url.includes("/api/v1/tasks")) {
        if (statsCalls > 1) {
          return jsonResponse(
            { error: { code: "HTTP_401", message: "API key revoked" } },
            401,
          );
        }
        return jsonResponse({
          items: [
            {
              id: "task-secret-1",
              task_name: "Sensitive Tenant Job",
              task_type: "noop",
              payload: {},
              status: "SUCCESS",
              priority: 2,
              tenant_id: "tenant-revoke",
              idempotency_key: null,
              retry_count: 0,
              max_retries: 3,
              timeout_seconds: 60,
              scheduled_at: null,
              started_at: null,
              completed_at: null,
              created_at: "2026-11-12T00:00:00Z",
              updated_at: "2026-11-12T00:00:01Z",
              error_message: null,
              error_type: null,
            },
          ],
          total: 1,
          limit: 8,
          offset: 0,
          has_more: false,
        });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup({ advanceTimers: vi.advanceTimersByTime });
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_will_be_revoked");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByText("Sensitive Tenant Job")).toBeInTheDocument();
    });

    // Advance clock for second poll cycle -> triggers 401
    await act(async () => {
      vi.advanceTimersByTime(2600);
    });

    await waitFor(() => {
      expect(screen.getByRole("button", { name: /Sign In/i })).toBeInTheDocument();
    });

    // Cached tenant row must no longer be mounted in DOM
    expect(screen.queryByText("Sensitive Tenant Job")).not.toBeInTheDocument();
    expect(screen.getByText(/expired, revoked, or invalid/i)).toBeInTheDocument();

    const callsAfterRevoke = fetchMock.mock.calls.length;

    // Advance additional polling intervals -> ZERO further requests allowed
    await act(async () => {
      vi.advanceTimersByTime(10000);
    });

    expect(fetchMock.mock.calls.length).toBe(callsAfterRevoke);
  });

  it("preserves authenticated session when API returns 403 Forbidden", async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      if (url.endsWith("/api/v1/auth/me")) {
        return jsonResponse({
          key_id: "key-ro",
          tenant_id: "tenant-ro",
          name: "readonly-key",
          scopes: ["tasks:read"],
        });
      }
      if (url.endsWith("/health/ready")) {
        return jsonResponse({ status: "ready" });
      }
      if (url.endsWith("/api/v1/stats")) {
        return jsonResponse({
          scope: "tenant",
          tenant_id: "tenant-ro",
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
        return jsonResponse({
          items: [
            {
              task_type: "noop",
              display_name: "No-op",
              category: "test",
              description: "No-op handler",
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
      if (url.endsWith("/api/v1/tasks") && init?.method === "POST") {
        return jsonResponse(
          { error: { code: "HTTP_403", message: "Missing required scope: tasks:write" } },
          403,
        );
      }
      if (url.includes("/api/v1/tasks")) {
        return jsonResponse({ items: [], total: 0, limit: 8, offset: 0, has_more: false });
      }
      return jsonResponse({}, 404);
    });
    vi.stubGlobal("fetch", fetchMock);

    const user = userEvent.setup();
    render(<App />);

    await user.type(screen.getByLabelText(/API Key/i), "tf_live_readonly");
    await user.click(screen.getByRole("button", { name: /Sign In/i }));

    await waitFor(() => {
      expect(screen.getByRole("link", { name: /^Submit Task$/i })).toBeInTheDocument();
    });

    await user.click(screen.getByRole("link", { name: /^Submit Task$/i }));
    await waitFor(() => {
      expect(screen.getByRole("button", { name: /^Submit Task$/i })).toBeInTheDocument();
    });

    await user.click(screen.getByRole("button", { name: /^Submit Task$/i }));

    await waitFor(() => {
      expect(screen.getByText(/Authorization error \(403\)/i)).toBeInTheDocument();
    });
    // Still signed in!
    expect(screen.getByRole("button", { name: /Sign Out/i })).toBeInTheDocument();
  });
});
