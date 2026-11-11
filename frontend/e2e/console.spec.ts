import { execFileSync } from "node:child_process";
import path from "node:path";
import { expect, test, type Page } from "@playwright/test";

const PYTHON_EXE = path.resolve("..", ".venv", "Scripts", "python.exe");
const DB_HELPER = path.resolve("e2e", "db_helper.py");

interface TenantsFixture {
  tenant_a: string;
  tenant_b: string;
  key_a_rw: string;
  key_a_rw_id: string;
  key_a_ro: string;
  key_a_rev: string;
  key_a_rev_id: string;
  key_op: string;
  key_b_rw: string;
}

function runDbHelper<T>(...args: string[]): T {
  const out = execFileSync(PYTHON_EXE, [DB_HELPER, ...args], {
    cwd: path.resolve(".."),
    encoding: "utf-8",
  });
  return JSON.parse(out.trim()) as T;
}

async function signInViaUi(page: Page, apiKey: string) {
  const input = page.getByLabel("API Key");
  await expect(input).toBeVisible();
  await input.fill(apiKey);
  await page.getByRole("button", { name: "Sign In" }).click();
  await expect(page.getByRole("button", { name: "Sign Out" })).toBeVisible();
}

let tenants: TenantsFixture;

test.beforeAll(() => {
  tenants = runDbHelper<TenantsFixture>("init-tenants");
});

test.afterAll(() => {
  if (tenants) {
    runDbHelper("cleanup", tenants.tenant_a, tenants.tenant_b);
  }
});

test("1. Auth gate, keyboard sign-in, live submit->execution->detail, 200 replay, and 409 conflict", async ({
  page,
}) => {
  const runtimeErrors: string[] = [];
  page.on("pageerror", (err) => runtimeErrors.push(err.message));

  // Direct visit to protected route must show Sign-In Gate (never bypass auth)
  await page.goto("/monitoring");
  await expect(page.getByRole("button", { name: "Sign In" })).toBeVisible();

  // Keyboard sign-in (fill + Enter key) preserves requested /monitoring deep route
  const keyInput = page.getByLabel("API Key");
  await keyInput.focus();
  await expect(keyInput).toBeFocused();
  await keyInput.fill(tenants.key_a_rw);
  await keyInput.press("Enter");
  await expect(page.getByRole("heading", { name: "System Monitoring" })).toBeVisible();

  // Navigate to Submit Task via keyboard Tab/Enter or nav link
  await page.getByRole("link", { name: "Submit Task" }).click();
  await expect(page.getByText(/At-least-once delivery:/i)).toBeVisible();

  const idemKey = `e2e-idem-${Date.now()}`;
  await page.getByLabel("Task Name").fill("Edge E2E Prime Job");
  await page.getByLabel("Task Type").selectOption("compute_prime");
  await page.getByLabel("Idempotency Key (Optional)").fill(idemKey);
  await page.getByLabel("JSON Payload").fill(JSON.stringify({ n: 30 }));
  await page.getByRole("button", { name: "Submit Task" }).click();

  // Live worker + Redis ZSET + Postgres execution completes to SUCCESS & Immutable
  await expect(page.locator("[data-status='SUCCESS']")).toBeVisible({ timeout: 20_000 });
  await expect(page.getByText("Immutable (Terminal)")).toBeVisible();

  // Result, attempts, events, and separated timing checks
  const resultRegion = page.getByRole("region", { name: "Execution Result" });
  await expect(resultRegion).toContainText('"prime_count": 10');

  const timingRegion = page.getByRole("region", { name: "Execution Timing Breakdown" });
  await expect(timingRegion).toContainText("Latest Handler Duration");
  await expect(timingRegion).toContainText("Worker Lease Tenure");
  await expect(timingRegion).toContainText("End-to-End Task Elapsed");

  const attemptsRegion = page.getByRole("region", { name: "Attempt History" });
  await expect(attemptsRegion).toContainText("#1");

  const eventsRegion = page.getByRole("region", { name: "Lifecycle Events" });
  await expect(eventsRegion).toContainText("→");

  // Capture created task URL for replay check
  const createdUrl = page.url();

  // Confirm no secret key or lease_token leaks into storage, cookies, or DOM
  const bodyText = (await page.locator("body").textContent()) ?? "";
  expect(bodyText).not.toContain(tenants.key_a_rw);
  expect(bodyText).not.toContain("lease_token");
  const storageCounts = await page.evaluate(() => ({
    ls: window.localStorage.length,
    ss: window.sessionStorage.length,
    cookies: document.cookie,
  }));
  expect(storageCounts).toEqual({ ls: 0, ss: 0, cookies: "" });

  // 200 Idempotent Replay with identical payload navigates back to same task with replay notice
  await page.getByRole("link", { name: "Submit Task" }).click();
  await page.getByLabel("Task Name").fill("Edge E2E Prime Job");
  await page.getByLabel("Task Type").selectOption("compute_prime");
  await page.getByLabel("Idempotency Key (Optional)").fill(idemKey);
  await page.getByLabel("JSON Payload").fill(JSON.stringify({ n: 30 }));
  await page.getByRole("button", { name: "Submit Task" }).click();
  await expect(page.getByText(/Idempotent replay \(HTTP 200\)/i)).toBeVisible();
  expect(page.url()).toBe(createdUrl);

  // 409 Conflict with modified payload stays on Submit form with edits preserved
  await page.getByRole("link", { name: "Submit Task" }).click();
  await page.getByLabel("Task Name").fill("Edge E2E Prime Job");
  await page.getByLabel("Task Type").selectOption("compute_prime");
  await page.getByLabel("Idempotency Key (Optional)").fill(idemKey);
  await page.getByLabel("JSON Payload").fill(JSON.stringify({ n: 45 }));
  await page.getByRole("button", { name: "Submit Task" }).click();
  await expect(page.getByText(/Idempotency conflict \(409\)/i)).toBeVisible();
  await expect(page.getByLabel("Idempotency Key (Optional)")).toHaveValue(idemKey);

  expect(runtimeErrors).toEqual([]);
});

test("2. Future scheduling cancel, manual retry execution, and real 409 server reconciliation", async ({
  page,
}) => {
  await page.goto("/submit");
  await signInViaUi(page, tenants.key_a_rw);

  // Scheduled future task -> QUEUED with future scheduled_at wait -> Cancel -> CANCELLED + Immutable
  await page.getByLabel("Task Name").fill("Edge E2E Scheduled Job");
  await page.getByLabel("Task Type").selectOption("compute_prime");
  await page.getByLabel("Execution Schedule").selectOption("scheduled");
  await page.getByLabel("Scheduled Time (Local, sent as UTC)").fill("2030-01-01T12:00");
  await page.getByRole("button", { name: "Submit Task" }).click();

  await expect(page.locator("[data-status='QUEUED']")).toBeVisible();
  await expect(
    page.getByText("Waiting — Scheduled Future Execution (Inactive)"),
  ).toBeVisible();

  await page.getByRole("button", { name: "Cancel Task" }).click();
  await expect(page.locator("[data-status='CANCELLED']")).toBeVisible();
  await expect(page.getByText("Immutable (Terminal)")).toBeVisible();
  await expect(page.getByRole("button", { name: /Manual Retry/i })).toHaveCount(0);

  // Seed a FAILED task -> verify Failure Diagnostics -> Manual Retry -> live worker completes to SUCCESS
  const { task_id: failedId } = runDbHelper<{ task_id: string }>(
    "seed-failed",
    tenants.tenant_a,
  );
  await page.goto(`/tasks/${failedId}`);
  // Note: full page.goto clears React memory, so re-authenticate cleanly while preserving deep link!
  await signInViaUi(page, tenants.key_a_rw);

  const diagRegion = page.getByRole("region", { name: "Failure Diagnostics" });
  await expect(diagRegion).toContainText("Simulated transient fault before manual retry");

  await page.getByRole("button", { name: "Manual Retry (Re-queue)" }).click();
  await expect(page.locator("[data-status='SUCCESS']")).toBeVisible({ timeout: 20_000 });

  // Deterministic live backend 409 reconciliation:
  // Submit a scheduled task sitting in QUEUED wait, force it to SUCCESS in Postgres, then click Cancel Task
  await page.getByRole("link", { name: "Submit Task" }).click();
  await page.getByLabel("Task Name").fill("Edge E2E Race Conflict Job");
  await page.getByLabel("Task Type").selectOption("compute_prime");
  await page.getByLabel("Execution Schedule").selectOption("scheduled");
  await page.getByLabel("Scheduled Time (Local, sent as UTC)").fill("2030-01-01T12:00");
  await page.getByRole("button", { name: "Submit Task" }).click();

  await expect(page.locator("[data-status='QUEUED']")).toBeVisible();
  const raceTaskId = page.url().split("/tasks/")[1];
  runDbHelper("force-success", raceTaskId);

  await page.getByRole("button", { name: "Cancel Task" }).click();
  await expect(page.getByText(/State conflict \(409\)/i)).toBeVisible();
  await expect(page.locator("[data-status='SUCCESS']")).toBeVisible();
  await expect(page.getByText("Immutable (Terminal)")).toBeVisible();
});

test("3. Explorer server-side filtering, pagination, deep-link reload, and tenant isolation", async ({
  page,
}) => {
  const { tenant_b_task_id: bTaskId } = runDbHelper<{
    seeded_a: number;
    tenant_b_task_id: string;
  }>("seed-explorer", tenants.tenant_a, tenants.tenant_b);

  await page.goto("/tasks?limit=10");
  await signInViaUi(page, tenants.key_a_rw);

  await expect(page.locator("table.data-table tbody tr")).toHaveCount(10);
  await expect(page.getByText("Tenant B Private Secret Job")).toHaveCount(0);

  // Page 2 -> offset=10
  await page.getByRole("button", { name: "Next" }).click();
  await expect(page).toHaveURL(/offset=10/);

  // Filter Status -> FAILED resets offset=0 and loads matching items cleanly
  await page.getByLabel("Status").selectOption("FAILED");
  await expect(page).toHaveURL(/\?status=FAILED&limit=10$/);
  await expect(page.locator("[data-status='FAILED']").first()).toBeVisible();

  // Reloading clears in-memory credential back to Sign-In while keeping deep-link URL params;
  // signing back in restores the filtered explorer state
  await page.reload();
  await signInViaUi(page, tenants.key_a_rw);
  await expect(page.getByLabel("Status")).toHaveValue("FAILED");
  await expect(page.getByLabel("Page Size")).toHaveValue("10");

  // Direct cross-tenant task access returns backend 404 isolation error
  await page.goto(`/tasks/${bTaskId}`);
  await signInViaUi(page, tenants.key_a_rw);
  await expect(page.getByText(/404|not found/i)).toBeVisible();
  await expect(page.getByText("Tenant B Private Secret Job")).toHaveCount(0);
});

test("4. Monitoring boundaries, 403 preservation, concurrent 401 DB revocation, and 429/503 backoff", async ({
  page,
}) => {
  // Non-operator sees boundary notice & no cluster telemetry
  await page.goto("/monitoring");
  await signInViaUi(page, tenants.key_a_rw);
  await expect(page.getByRole("region", { name: /Operator Telemetry Boundary/i })).toBeVisible();
  await page.getByLabel("Window:").selectOption("15");
  await expect(page.getByText(/Window: 15m/i)).toBeVisible();

  // Operator session reveals live Cluster Saturation & Outbox
  await page.getByRole("button", { name: "Sign Out" }).click();
  await signInViaUi(page, tenants.key_op);
  await expect(page.getByRole("region", { name: "Operator Cluster Telemetry" })).toBeVisible();

  // Read-only key gets real 403 while staying authenticated
  await page.getByRole("button", { name: "Sign Out" }).click();
  await signInViaUi(page, tenants.key_a_ro);
  await page.getByRole("link", { name: "Submit Task" }).click();
  await expect(page.getByText(/lacks tasks:write scope/i)).toBeVisible();
  await page.getByRole("button", { name: "Submit Task" }).click();
  await expect(page.getByText(/Authorization error \(403\)/i)).toBeVisible();
  await expect(page.getByRole("button", { name: "Sign Out" })).toBeVisible();

  // Concurrent 401 mid-polling via real DB revocation clears session & ignores stale /tasks response
  await page.getByRole("button", { name: "Sign Out" }).click();
  await page.goto("/");
  await signInViaUi(page, tenants.key_a_rev);
  await expect(page.getByRole("heading", { name: "Overview" })).toBeVisible();

  let holdTasksResponse = false;
  let heldAbortedBy401 = false;
  let releaseLateTasks: (() => void) | null = null;

  const failedReqWatcher = (req: { url: () => string }) => {
    if (holdTasksResponse && req.url().includes("/api/v1/tasks")) {
      heldAbortedBy401 = true;
      releaseLateTasks?.();
    }
  };
  page.on("requestfailed", failedReqWatcher);

  await page.route("**/api/v1/tasks*", async (route) => {
    if (!holdTasksResponse) {
      await route.continue();
      return;
    }
    await new Promise<void>((resolve) => {
      releaseLateTasks = resolve;
    });
    if (heldAbortedBy401) {
      return;
    }
    try {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({
          items: [
            {
              id: "stale-after-401",
              task_name: "Stale Late Task Should Never Appear",
              task_type: "noop",
              payload: {},
              status: "SUCCESS",
              priority: 2,
              idempotency_key: null,
              retry_count: 0,
              max_retries: 3,
              timeout_seconds: 60,
              scheduled_at: null,
              started_at: null,
              completed_at: null,
              created_at: "2026-11-12T00:00:00Z",
              updated_at: "2026-11-12T00:00:00Z",
              attempts: [],
              result: null,
              events: [],
            },
          ],
          total: 1,
          limit: 8,
          offset: 0,
          has_more: false,
        }),
      });
    } catch {
      // Browser aborted the route concurrently on 401 session invalidation
    }
  });

  // Wait briefly until overview initial cycle settles, then revoke key in live Postgres & arm stale trap
  await page.waitForTimeout(300);
  holdTasksResponse = true;
  runDbHelper("revoke-key", tenants.key_a_rev_id);

  // Next poll (2.5s cadence) hits real 401 on /stats -> aborts /tasks & boots to Sign-In Gate
  await expect(page.getByText(/expired, revoked, or invalid/i)).toBeVisible({ timeout: 8_000 });
  releaseLateTasks?.();
  page.off("requestfailed", failedReqWatcher);
  await page.unroute("**/api/v1/tasks*");

  // Verify stale payload never mounted and polling stopped completely
  await expect(page.getByText("Stale Late Task Should Never Appear")).toHaveCount(0);
  let extraPollRequests = 0;
  const pollWatcher = (req: { url: () => string }) => {
    if (req.url().includes("/api/v1/")) extraPollRequests += 1;
  };
  page.on("request", pollWatcher);
  await page.waitForTimeout(3_000);
  page.off("request", pollWatcher);
  expect(extraPollRequests).toBe(0);

  // 429 and 503 backpressure on Explorer preserves valid rows
  await page.goto("/tasks");
  await signInViaUi(page, tenants.key_a_rw);
  await expect(page.locator("table.data-table tbody tr").first()).toBeVisible();

  let injectedStatus = 429;
  await page.route("**/api/v1/tasks?*", async (route) => {
    await route.fulfill({
      status: injectedStatus,
      headers: { "Content-Type": "application/json", "Retry-After": "4" },
      body: JSON.stringify({
        error: { code: `HTTP_${injectedStatus}`, message: `Injected ${injectedStatus}` },
      }),
    });
  });

  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText(/Rate limited \(429\): Polling paused for 4s/i)).toBeVisible();

  injectedStatus = 503;
  await page.getByRole("button", { name: "Refresh" }).click();
  await expect(page.getByText(/Service unavailable \(503\): Retry in 4s/i)).toBeVisible();
  await page.unroute("**/api/v1/tasks?*");
});

test("5. Responsive (1280x800 desktop & 390x844 mobile) and keyboard focus accessibility", async ({
  page,
}) => {
  const viewports = [
    { name: "desktop", width: 1280, height: 800 },
    { name: "mobile", width: 390, height: 844 },
  ];

  for (const vp of viewports) {
    await page.setViewportSize({ width: vp.width, height: vp.height });
    await page.goto("/");
    await signInViaUi(page, tenants.key_a_rw);

    for (const route of ["/", "/submit", "/tasks", "/monitoring"]) {
      await page.locator(`a.nav-link[href='${route}']`).click();
      await page.waitForLoadState("networkidle");
      const dims = await page.evaluate(() => ({
        scrollW: document.documentElement.scrollWidth,
        innerW: window.innerWidth,
      }));
      expect(dims.scrollW, `${vp.name} overflow on ${route}`).toBeLessThanOrEqual(dims.innerW);
    }

    // Verify Tab traverses interactive nav controls cleanly
    await page.keyboard.press("Tab");
    const focusedTag = await page.evaluate(() => document.activeElement?.tagName ?? "");
    expect(["A", "BUTTON", "INPUT", "SELECT"]).toContain(focusedTag);

    await page.getByRole("button", { name: "Sign Out" }).click();
  }
});

