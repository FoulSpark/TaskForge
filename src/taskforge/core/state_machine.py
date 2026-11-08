from enum import Enum, StrEnum


class TaskStatus(StrEnum):
    PENDING = "PENDING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    SUCCESS = "SUCCESS"
    RETRY_WAIT = "RETRY_WAIT"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"
    CANCELLED = "CANCELLED"


class TaskPriority(int, Enum):
    LOW = 1
    MEDIUM = 2
    HIGH = 3
    CRITICAL = 4


TERMINAL_STATES: set[TaskStatus] = {
    TaskStatus.SUCCESS,
    TaskStatus.DEAD_LETTER,
    TaskStatus.CANCELLED,
}

CANCELLABLE_STATES: set[TaskStatus] = {
    TaskStatus.PENDING,
    TaskStatus.QUEUED,
    TaskStatus.RUNNING,
    TaskStatus.RETRY_WAIT,
}

ALLOWED_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.QUEUED, TaskStatus.CANCELLED},
    TaskStatus.QUEUED: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCESS,
        TaskStatus.RETRY_WAIT,
        TaskStatus.FAILED,
        TaskStatus.CANCELLED,
    },
    TaskStatus.RETRY_WAIT: {TaskStatus.QUEUED, TaskStatus.CANCELLED},
    TaskStatus.FAILED: {TaskStatus.DEAD_LETTER},
    TaskStatus.SUCCESS: set(),
    TaskStatus.DEAD_LETTER: set(),
    TaskStatus.CANCELLED: set(),
}


class InvalidStateTransitionError(ValueError):
    """Raised when an illegal task state transition is attempted."""

    def __init__(self, current_status: TaskStatus | str, new_status: TaskStatus | str):
        self.current_status = TaskStatus(current_status)
        self.new_status = TaskStatus(new_status)
        super().__init__(
            f"Cannot transition task from '{self.current_status.value}' to '{self.new_status.value}'. "
            f"Allowed next states: {[s.value for s in ALLOWED_TRANSITIONS.get(self.current_status, set())]}"
        )


def validate_transition(current_status: TaskStatus | str, new_status: TaskStatus | str) -> bool:
    """Validate whether transitioning from current_status to new_status is legal."""
    curr = TaskStatus(current_status)
    nxt = TaskStatus(new_status)

    if nxt not in ALLOWED_TRANSITIONS.get(curr, set()):
        raise InvalidStateTransitionError(curr, nxt)
    return True


def is_terminal_state(status: TaskStatus | str) -> bool:
    """Check if task has reached a terminal state."""
    return TaskStatus(status) in TERMINAL_STATES


def can_cancel_task(status: TaskStatus | str) -> bool:
    """Check if task is in a state that can be cancelled."""
    return TaskStatus(status) in CANCELLABLE_STATES
