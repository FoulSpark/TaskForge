import pytest
from taskforge.core.state_machine import (
    ALLOWED_TRANSITIONS,
    TERMINAL_STATES,
    InvalidStateTransitionError,
    TaskPriority,
    TaskStatus,
    can_cancel_task,
    is_terminal_state,
    validate_transition,
)


def test_task_status_values():
    assert TaskStatus.PENDING == "PENDING"
    assert TaskStatus.QUEUED == "QUEUED"
    assert TaskStatus.RUNNING == "RUNNING"
    assert TaskStatus.SUCCESS == "SUCCESS"
    assert TaskStatus.RETRY_WAIT == "RETRY_WAIT"
    assert TaskStatus.FAILED == "FAILED"
    assert TaskStatus.DEAD_LETTER == "DEAD_LETTER"
    assert TaskStatus.CANCELLED == "CANCELLED"


def test_legal_transitions():
    assert validate_transition(TaskStatus.PENDING, TaskStatus.QUEUED) is True
    assert validate_transition(TaskStatus.PENDING, TaskStatus.CANCELLED) is True
    assert validate_transition(TaskStatus.QUEUED, TaskStatus.RUNNING) is True
    assert validate_transition(TaskStatus.QUEUED, TaskStatus.CANCELLED) is True
    assert validate_transition(TaskStatus.RUNNING, TaskStatus.SUCCESS) is True
    assert validate_transition(TaskStatus.RUNNING, TaskStatus.RETRY_WAIT) is True
    assert validate_transition(TaskStatus.RUNNING, TaskStatus.FAILED) is True
    assert validate_transition(TaskStatus.RUNNING, TaskStatus.CANCELLED) is True
    assert validate_transition(TaskStatus.RETRY_WAIT, TaskStatus.QUEUED) is True
    assert validate_transition(TaskStatus.RETRY_WAIT, TaskStatus.CANCELLED) is True
    assert validate_transition(TaskStatus.FAILED, TaskStatus.DEAD_LETTER) is True


def test_illegal_transitions():
    # Direct PENDING -> SUCCESS is illegal
    with pytest.raises(InvalidStateTransitionError):
        validate_transition(TaskStatus.PENDING, TaskStatus.SUCCESS)

    # Terminal state transitions are illegal
    for term in TERMINAL_STATES:
        for target in TaskStatus:
            if target != term:
                with pytest.raises(InvalidStateTransitionError):
                    validate_transition(term, target)

    # QUEUED directly to SUCCESS is illegal
    with pytest.raises(InvalidStateTransitionError):
        validate_transition(TaskStatus.QUEUED, TaskStatus.SUCCESS)


def test_terminal_states():
    assert is_terminal_state(TaskStatus.SUCCESS) is True
    assert is_terminal_state(TaskStatus.DEAD_LETTER) is True
    assert is_terminal_state(TaskStatus.CANCELLED) is True
    assert is_terminal_state(TaskStatus.RUNNING) is False
    assert is_terminal_state(TaskStatus.PENDING) is False


def test_cancellable_states():
    assert can_cancel_task(TaskStatus.PENDING) is True
    assert can_cancel_task(TaskStatus.QUEUED) is True
    assert can_cancel_task(TaskStatus.RUNNING) is True
    assert can_cancel_task(TaskStatus.RETRY_WAIT) is True
    assert can_cancel_task(TaskStatus.SUCCESS) is False
    assert can_cancel_task(TaskStatus.DEAD_LETTER) is False
    assert can_cancel_task(TaskStatus.CANCELLED) is False

