import type { TaskItem, TaskStatus } from "./types";

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
  if (task.status !== "PENDING" || !task.scheduled_at) {
    return false;
  }
  const ts = Date.parse(task.scheduled_at);
  return Number.isFinite(ts);
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
      return task.scheduled_at
        ? "Waiting — Scheduled Future Execution (Inactive)"
        : "Waiting — Outbox Dispatch";
    case "FAILED":
      return "Failed — Manual Retry Eligible";
    default:
      return null;
  }
}

