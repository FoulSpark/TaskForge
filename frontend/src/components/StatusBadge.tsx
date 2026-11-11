import type { TaskPriority, TaskStatus } from "../api/types";

const STATUS_META: Record<
  TaskStatus,
  { label: string; icon: string; bg: string; fg: string }
> = {
  PENDING: {
    label: "PENDING",
    icon: "○",
    bg: "var(--status-pending-bg)",
    fg: "var(--status-pending-fg)",
  },
  QUEUED: {
    label: "QUEUED",
    icon: "◔",
    bg: "var(--status-queued-bg)",
    fg: "var(--status-queued-fg)",
  },
  RUNNING: {
    label: "RUNNING",
    icon: "▶",
    bg: "var(--status-running-bg)",
    fg: "var(--status-running-fg)",
  },
  SUCCESS: {
    label: "SUCCESS",
    icon: "✓",
    bg: "var(--status-success-bg)",
    fg: "var(--status-success-fg)",
  },
  RETRY_WAIT: {
    label: "RETRY_WAIT",
    icon: "↻",
    bg: "var(--status-retry-bg)",
    fg: "var(--status-retry-fg)",
  },
  FAILED: {
    label: "FAILED",
    icon: "✕",
    bg: "var(--status-failed-bg)",
    fg: "var(--status-failed-fg)",
  },
  DEAD_LETTER: {
    label: "DEAD_LETTER",
    icon: "☠",
    bg: "var(--status-dlq-bg)",
    fg: "var(--status-dlq-fg)",
  },
  CANCELLED: {
    label: "CANCELLED",
    icon: "■",
    bg: "var(--status-cancelled-bg)",
    fg: "var(--status-cancelled-fg)",
  },
};

const PRIORITY_META: Record<
  TaskPriority,
  { label: string; bg: string; fg: string }
> = {
  4: {
    label: "P4 CRITICAL",
    bg: "var(--prio-critical-bg)",
    fg: "var(--prio-critical-fg)",
  },
  3: {
    label: "P3 HIGH",
    bg: "var(--prio-high-bg)",
    fg: "var(--prio-high-fg)",
  },
  2: {
    label: "P2 MEDIUM",
    bg: "var(--prio-medium-bg)",
    fg: "var(--prio-medium-fg)",
  },
  1: {
    label: "P1 LOW",
    bg: "var(--prio-low-bg)",
    fg: "var(--prio-low-fg)",
  },
};

export function StatusBadge({ status }: { status: TaskStatus }) {
  const meta = STATUS_META[status] ?? {
    label: status,
    icon: "•",
    bg: "var(--bg-subtle)",
    fg: "var(--text-secondary)",
  };
  return (
    <span
      className="pill tabular"
      style={{ background: meta.bg, color: meta.fg }}
      data-status={status}
    >
      <span aria-hidden="true">{meta.icon}</span>
      <span>{meta.label}</span>
    </span>
  );
}

export function PriorityBadge({ priority }: { priority: TaskPriority }) {
  const meta = PRIORITY_META[priority] ?? PRIORITY_META[2];
  return (
    <span
      className="pill tabular"
      style={{ background: meta.bg, color: meta.fg }}
    >
      {meta.label}
    </span>
  );
}

