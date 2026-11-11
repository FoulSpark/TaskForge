import type { TaskAttemptItem, TaskEventItem, TaskItem, TaskStatus } from "./types";

export const TERMINAL_STATES: ReadonlySet<TaskStatus> = new Set([
  "SUCCESS",
  "DEAD_LETTER",
  "CANCELLED",
]);

export const IMMUTABLE_TERMINAL_STATES: ReadonlySet<TaskStatus> = new Set([
  "SUCCESS",
  "CANCELLED",
]);

export const CANCELLABLE_STATES: ReadonlySet<TaskStatus> = new Set([
  "PENDING",
  "QUEUED",
  "RUNNING",
  "RETRY_WAIT",
]);

// Backend TaskService.manual_retry() allows strictly FAILED and DEAD_LETTER.
export const RETRYABLE_STATES: ReadonlySet<TaskStatus> = new Set([
  "FAILED",
  "DEAD_LETTER",
]);

export function isTerminalStatus(status: TaskStatus): boolean {
  return TERMINAL_STATES.has(status);
}

export function isImmutableTerminalStatus(status: TaskStatus): boolean {
  return IMMUTABLE_TERMINAL_STATES.has(status);
}

export function canCancelStatus(status: TaskStatus): boolean {
  return CANCELLABLE_STATES.has(status);
}

export function canRetryStatus(status: TaskStatus): boolean {
  return RETRYABLE_STATES.has(status);
}

export function isFutureScheduled(task: Pick<TaskItem, "status" | "scheduled_at">): boolean {
  if ((task.status !== "PENDING" && task.status !== "QUEUED") || !task.scheduled_at) {
    return false;
  }
  const ts = Date.parse(task.scheduled_at);
  return Number.isFinite(ts) && ts > Date.now();
}

export function getLifecycleStateLabel(
  task: Pick<TaskItem, "status" | "scheduled_at">,
): string | null {
  switch (task.status) {
    case "SUCCESS":
    case "CANCELLED":
      return "Immutable (Terminal)";
    case "DEAD_LETTER":
      return "Terminal Dead-Letter (Manual Re-queue Eligible)";
    case "RETRY_WAIT":
      return "Waiting — Auto-Retry Backoff (Inactive)";
    case "PENDING":
    case "QUEUED":
      return isFutureScheduled(task)
        ? "Waiting — Scheduled Future Execution (Inactive)"
        : task.status === "PENDING"
          ? "Waiting — Outbox Dispatch"
          : null;
    case "FAILED":
      return "Failed — Manual Retry Eligible";
    default:
      return null;
  }
}

export function sortAttemptsAsc(attempts: TaskAttemptItem[] | undefined): TaskAttemptItem[] {
  return [...(attempts ?? [])].sort((a, b) => a.attempt_number - b.attempt_number);
}

export function sortEventsChronological(events: TaskEventItem[] | undefined): TaskEventItem[] {
  return [...(events ?? [])].sort((a, b) => {
    const timeDiff = Date.parse(a.created_at) - Date.parse(b.created_at);
    if (Number.isFinite(timeDiff) && timeDiff !== 0) {
      return timeDiff;
    }
    if (a.created_at !== b.created_at) {
      return a.created_at < b.created_at ? -1 : 1;
    }
    return a.id - b.id;
  });
}

export function computeElapsedMs(
  startIso: string | null | undefined,
  endIso: string | null | undefined,
): number | null {
  if (!startIso || !endIso) {
    return null;
  }
  const start = Date.parse(startIso);
  const end = Date.parse(endIso);
  if (!Number.isFinite(start) || !Number.isFinite(end)) {
    return null;
  }
  return Math.max(0, end - start);
}

const SENSITIVE_KEY_PATTERN = /(lease_token|owner_token|fence_token|fencing|api_key|secret|password|authorization)/i;

export function sanitizeMetadataForDisplay(
  raw: Record<string, unknown> | null | undefined,
): Record<string, unknown> | null {
  if (!raw || typeof raw !== "object") {
    return null;
  }
  const cleaned: Record<string, unknown> = {};
  for (const [key, val] of Object.entries(raw)) {
    if (SENSITIVE_KEY_PATTERN.test(key)) {
      cleaned[key] = "[REDACTED]";
    } else if (val && typeof val === "object" && !Array.isArray(val)) {
      cleaned[key] = sanitizeMetadataForDisplay(val as Record<string, unknown>);
    } else {
      cleaned[key] = val;
    }
  }
  return Object.keys(cleaned).length > 0 ? cleaned : null;
}

