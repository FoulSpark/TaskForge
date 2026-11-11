import path from "node:path";
import { defineConfig } from "@playwright/test";

const PYTHON_BIN = path.resolve("..", ".venv", "Scripts", "python.exe");

export default defineConfig({
  testDir: "./e2e",
  retries: 0,
  workers: 1,
  timeout: 45_000,
  expect: {
    timeout: 10_000,
  },
  reporter: [["list"]],
  use: {
    baseURL: "http://127.0.0.1:8012",
    channel: "msedge",
    headless: true,
    trace: "retain-on-failure",
  },
  webServer: {
    command: `"${PYTHON_BIN}" -m uvicorn taskforge.main:app --host 127.0.0.1 --port 8012`,
    cwd: "..",
    url: "http://127.0.0.1:8012/health/ready",
    reuseExistingServer: false,
    timeout: 30_000,
    env: {
      TASKFORGE_STRICT_AUTH: "1",
      START_EMBEDDED_WORKERS: "true",
      RATE_LIMIT_PER_MINUTE: "1000",
    },
  },
});

