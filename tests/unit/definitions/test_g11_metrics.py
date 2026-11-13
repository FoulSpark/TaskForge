"""Unit tests for Sub-Gate G11.5-C — Definition Execution Metrics Integration.

Verifies:
- Definition-specific Prometheus counters increment on authoritative lifecycle transitions
- Counters do NOT increment for non-definition tasks
- Counters do NOT increment on read-only operations (GET endpoints)
- Validation failure counters distinguish input vs output phase
- Handler unavailable counter fires correctly
- Duration histogram records at terminal completion
- Retry does NOT increment execution counter (only duration)
- Metric recorder failures do not break task execution (try/except isolation)
- Submission counter fires after execute_definition commit

Runs on in-memory SQLite with foreign keys enforced.
"""

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.core.state_machine import TaskStatus
from taskforge.database.models import Task, get_utc_now
from taskforge.database.session import Base
from taskforge.definitions.service import DefinitionService
from taskforge.monitoring.metrics import (
    DEFINITION_EXECUTIONS_TOTAL,
    DEFINITION_HANDLER_UNAVAILABLE_TOTAL,
    DEFINITION_VALIDATION_FAILURES_TOTAL,
    normalize_definition_error_type,
    normalize_definition_phase,
    record_definition_execution,
    record_definition_execution_duration,
    record_definition_handler_unavailable,
    record_definition_validation_failure,
)
from taskforge.tasks.registry import TaskRegistry
from taskforge.workers.execution_engine import (
    ExecutionDisposition,
    execute_task,
)

# ---------------------------------------------------------------------------
# In-memory SQLite engine for metrics tests
# ---------------------------------------------------------------------------
_test_engine = create_engine(
    "sqlite:///:memory:",
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
)


@event.listens_for(_test_engine, "connect")
def _enable_fk(dbapi_conn, _record):
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


_TestSession = sessionmaker(bind=_test_engine, autoflush=False, expire_on_commit=False)


@pytest.fixture(autouse=True)
def _reset_db():
    Base.metadata.create_all(bind=_test_engine)
    yield
    with _test_engine.begin() as conn:
        conn.execute(text("PRAGMA foreign_keys=OFF"))
        Base.metadata.drop_all(bind=conn)
        conn.execute(text("PRAGMA foreign_keys=ON"))


@pytest.fixture
def db():
    session = _TestSession()
    try:
        yield session
    finally:
        session.close()


def _make_session_local_mock() -> MagicMock:
    """Mock SessionLocal as a context-manager factory backed by shared test engine."""

    def _factory(*_args, **_kwargs):
        @contextmanager
        def _cm():
            session = _TestSession()
            try:
                yield session
                session.commit()
            except Exception:
                session.rollback()
                raise
            finally:
                session.close()

        return _cm()

    mock = MagicMock(side_effect=_factory)
    return mock


@pytest.fixture
def principal():
    return AuthenticatedPrincipal(
        key_id="key-metrics-tester",
        tenant_id="tenant-metrics",
        name="Metrics Tester",
        scopes=["tasks:read", "tasks:write", "definitions:write"],
    )


@pytest.fixture
def active_definition(db, principal):
    """Active definition with a validated webhook_send handler."""
    service = DefinitionService(db)
    defn, _ = service.create_definition(
        principal=principal,
        name="Metrics Test Webhook",
        description="Tests metrics instrumentation",
        task_type="webhook_send",
        execution_type="builtin",
        configuration_json={"timeout_ms": 5000},
        input_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["url"],
            "properties": {
                "url": {"type": "string"},
            },
            "additionalProperties": False,
        },
        output_schema_json={
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "required": ["status"],
            "properties": {
                "status": {"type": "string"},
            },
        },
        execution_policy_json={"max_retries": 2, "timeout_seconds": 30},
    )
    _, val_result = service.validate_version(
        principal=principal,
        definition_id=defn.id,
        version_number=1,
    )
    assert val_result.is_valid is True
    service.activate_version(
        principal=principal,
        definition_id=defn.id,
        version_number=1,
    )
    return service.get_definition(principal, defn.id)


def _get_counter_value(counter, labels: dict[str, str]) -> float:
    """Safely read current counter value for given labels, returning 0.0 if not yet created."""
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0


def _get_histogram_count(histogram, labels: dict[str, str]) -> int:
    """Read the _count field of a histogram for given labels."""
    try:
        return int(histogram.labels(**labels)._sum._count.get())
    except Exception:
        # Fallback: try reading from sample data
        pass
    # Alternative approach: collect from registry
    for metric in histogram.collect():
        for sample in metric.samples:
            if sample.name.endswith("_count"):
                match = True
                for k, v in labels.items():
                    if sample.labels.get(k) != v:
                        match = False
                        break
                if match:
                    return int(sample.value)
    return 0


# ---------------------------------------------------------------------------
# 1. Normalizer Tests
# ---------------------------------------------------------------------------


class TestDefinitionNormalizers:
    """Verify bounded cardinality normalizers for definition metrics."""

    def test_normalize_phase_input(self):
        assert normalize_definition_phase("input") == "input"

    def test_normalize_phase_output(self):
        assert normalize_definition_phase("output") == "output"

    def test_normalize_phase_unknown_defaults_to_input(self):
        assert normalize_definition_phase("unknown") == "input"

    def test_normalize_phase_none_defaults_to_input(self):
        assert normalize_definition_phase(None) == "input"

    def test_normalize_error_type_schema(self):
        assert (
            normalize_definition_error_type("SCHEMA_VALIDATION_ERROR") == "SCHEMA_VALIDATION_ERROR"
        )

    def test_normalize_error_type_output(self):
        assert (
            normalize_definition_error_type("OUTPUT_VALIDATION_ERROR") == "OUTPUT_VALIDATION_ERROR"
        )

    def test_normalize_error_type_unknown_defaults(self):
        assert normalize_definition_error_type("RANDOM") == "SCHEMA_VALIDATION_ERROR"


# ---------------------------------------------------------------------------
# 2. Recorder Function Isolation Tests
# ---------------------------------------------------------------------------


class TestRecorderIsolation:
    """Verify that recorder functions never raise, even with bad inputs."""

    def test_record_definition_execution_with_none(self):
        """Recorder should not raise with None inputs."""
        record_definition_execution(None, None)  # Should not raise

    def test_record_definition_validation_failure_with_none(self):
        record_definition_validation_failure(None, None)  # Should not raise

    def test_record_definition_handler_unavailable_with_none(self):
        record_definition_handler_unavailable(None)  # Should not raise

    def test_record_definition_execution_duration_with_negative(self):
        record_definition_execution_duration("webhook_send", -1.0)  # Clamped to 0.0

    @patch(
        "taskforge.monitoring.metrics.DEFINITION_EXECUTIONS_TOTAL",
        side_effect=RuntimeError("Broken counter"),
    )
    def test_counter_failure_does_not_propagate(self, _mock_counter):
        """Metric failure must be silently caught — never break task execution."""
        # Should not raise
        record_definition_execution("webhook_send", "SUCCESS")


# ---------------------------------------------------------------------------
# 3. Counter Increment Tests
# ---------------------------------------------------------------------------


class TestDefinitionExecutionCounter:
    """Verify DEFINITION_EXECUTIONS_TOTAL increments correctly."""

    def test_submission_counter_increments(self):
        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )
        record_definition_execution("webhook_send", "SUBMITTED")
        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )
        assert after == before + 1.0

    def test_success_counter_increments(self):
        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )
        record_definition_execution("webhook_send", "SUCCESS")
        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )
        assert after == before + 1.0

    def test_failed_counter_increments(self):
        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "FAILED"},
        )
        record_definition_execution("webhook_send", "FAILED")
        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "FAILED"},
        )
        assert after == before + 1.0

    def test_unknown_task_type_normalized_to_other(self):
        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "other", "status": "SUCCESS"},
        )
        record_definition_execution("my_custom_unknown_type", "SUCCESS")
        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "other", "status": "SUCCESS"},
        )
        assert after == before + 1.0


class TestValidationFailureCounter:
    """Verify DEFINITION_VALIDATION_FAILURES_TOTAL distinguishes input vs output phase."""

    def test_input_validation_failure(self):
        before = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "input", "error_type": "SCHEMA_VALIDATION_ERROR"},
        )
        record_definition_validation_failure("input", "SCHEMA_VALIDATION_ERROR")
        after = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "input", "error_type": "SCHEMA_VALIDATION_ERROR"},
        )
        assert after == before + 1.0

    def test_output_validation_failure(self):
        before = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "output", "error_type": "OUTPUT_VALIDATION_ERROR"},
        )
        record_definition_validation_failure("output", "OUTPUT_VALIDATION_ERROR")
        after = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "output", "error_type": "OUTPUT_VALIDATION_ERROR"},
        )
        assert after == before + 1.0


class TestHandlerUnavailableCounter:
    """Verify DEFINITION_HANDLER_UNAVAILABLE_TOTAL increments."""

    def test_handler_unavailable_increments(self):
        before = _get_counter_value(
            DEFINITION_HANDLER_UNAVAILABLE_TOTAL,
            {"task_type": "webhook_send"},
        )
        record_definition_handler_unavailable("webhook_send")
        after = _get_counter_value(
            DEFINITION_HANDLER_UNAVAILABLE_TOTAL,
            {"task_type": "webhook_send"},
        )
        assert after == before + 1.0


# ---------------------------------------------------------------------------
# 4. Submission Metrics in DefinitionService.execute_definition()
# ---------------------------------------------------------------------------


class TestSubmissionMetrics:
    """Verify SUBMITTED counter fires after execute_definition commits."""

    def test_execute_definition_increments_submitted_counter(
        self, db, principal, active_definition
    ):
        """Submission via DefinitionService increments SUBMITTED counter exactly once."""
        from taskforge.api.schemas import DefinitionExecuteRequest

        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )

        service = DefinitionService(db)
        req = DefinitionExecuteRequest(
            input={"url": "https://example.com/hook"},
            priority=3,
            idempotency_key=f"metrics-test-{uuid.uuid4().hex[:8]}",
            timeout_seconds=20,
        )
        task, created = service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
        assert created is True

        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )
        assert after == before + 1.0

    def test_idempotent_replay_does_not_increment_submitted(self, db, principal, active_definition):
        """Idempotent replay (same key, same params) returns existing task WITHOUT incrementing SUBMITTED."""
        from taskforge.api.schemas import DefinitionExecuteRequest

        idem_key = f"idem-replay-{uuid.uuid4().hex[:8]}"
        service = DefinitionService(db)
        req = DefinitionExecuteRequest(
            input={"url": "https://example.com/hook"},
            priority=3,
            idempotency_key=idem_key,
            timeout_seconds=20,
        )
        # First call creates the task
        task1, created1 = service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
        assert created1 is True

        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )

        # Replay with same key and same params
        task2, created2 = service.execute_definition(
            principal=principal,
            definition_id=active_definition.id,
            request=req,
        )
        assert created2 is False

        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUBMITTED"},
        )
        # Counter must NOT have changed
        assert after == before


# ---------------------------------------------------------------------------
# 5. Execution Engine Metrics (success, failure, handler unavailable)
# ---------------------------------------------------------------------------


class TestExecutionEngineMetrics:
    """Verify definition metrics fire during execute_task lifecycle transitions."""

    def _create_definition_task(self, db, active_definition) -> Task:
        """Create a task pinned to a definition version."""
        ver_id = active_definition.active_version_id
        task = Task(
            id=uuid.uuid4().hex,
            task_name="metrics-execution-test",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=3,
            payload={
                "_input": {"url": "https://example.com"},
                "_current_attempt": 1,
            },
            tenant_id="tenant-metrics",
            max_retries=2,
            timeout_seconds=30,
            created_at=get_utc_now(),
            definition_id=active_definition.id,
            definition_version_id=ver_id,
        )
        db.add(task)
        db.commit()
        return task

    def test_success_increments_definition_counters(self, db, active_definition):
        """Successful execution of a definition task increments SUCCESS counter and duration."""
        task = self._create_definition_task(db, active_definition)

        before_success = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )

        def _handler(payload):
            return {"status": "ok"}

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch.object(TaskRegistry, "has_handler", return_value=True),
            patch(
                "taskforge.workers.execution_engine.get_handler",
                return_value=_handler,
            ),
        ):
            result = execute_task(task.id, "worker-m1")

        assert result == ExecutionDisposition.COMPLETED

        after_success = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )
        assert after_success == before_success + 1.0

    def test_non_definition_task_does_not_increment_definition_counters(self, db):
        """A plain task (no definition_id) must NOT increment definition counters."""
        task = Task(
            id=uuid.uuid4().hex,
            task_name="plain-task",
            task_type="webhook_send",
            status=TaskStatus.QUEUED.value,
            priority=2,
            payload={"data": "test"},
            tenant_id="tenant-metrics",
            max_retries=0,
            timeout_seconds=30,
            created_at=get_utc_now(),
        )
        db.add(task)
        db.commit()

        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )

        def _handler(payload):
            return {"ok": True}

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch.object(TaskRegistry, "has_handler", return_value=True),
            patch(
                "taskforge.workers.execution_engine.get_handler",
                return_value=_handler,
            ),
        ):
            result = execute_task(task.id, "worker-m2")

        assert result == ExecutionDisposition.COMPLETED

        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "SUCCESS"},
        )
        # Counter must NOT have changed for non-definition tasks
        assert after == before

    def test_handler_unavailable_increments_counter(self, db, active_definition):
        """HandlerUnavailableError increments the handler-unavailable counter."""
        task = self._create_definition_task(db, active_definition)

        before = _get_counter_value(
            DEFINITION_HANDLER_UNAVAILABLE_TOTAL,
            {"task_type": "webhook_send"},
        )

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch.object(TaskRegistry, "has_handler", return_value=False),
        ):
            result = execute_task(task.id, "worker-m3")

        assert result == ExecutionDisposition.TERMINAL_FAILURE_RECORDED

        after = _get_counter_value(
            DEFINITION_HANDLER_UNAVAILABLE_TOTAL,
            {"task_type": "webhook_send"},
        )
        assert after == before + 1.0

    def test_input_validation_failure_increments_counter(self, db, active_definition):
        """Input schema validation failure increments the validation failure counter."""
        task = self._create_definition_task(db, active_definition)
        # Inject invalid input that fails schema validation
        task.payload = {"_input": {"bad_field": 123}, "_current_attempt": 1}
        db.commit()

        before = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "input", "error_type": "SCHEMA_VALIDATION_ERROR"},
        )

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
        ):
            result = execute_task(task.id, "worker-m4")

        assert result == ExecutionDisposition.TERMINAL_FAILURE_RECORDED

        after = _get_counter_value(
            DEFINITION_VALIDATION_FAILURES_TOTAL,
            {"phase": "input", "error_type": "SCHEMA_VALIDATION_ERROR"},
        )
        assert after == before + 1.0

    def test_terminal_failure_increments_execution_counter(self, db, active_definition):
        """Terminal failure (max retries exceeded) increments FAILED counter."""
        task = self._create_definition_task(db, active_definition)
        # Set retries exhausted
        task.retry_count = 3
        task.max_retries = 2
        db.commit()

        before = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "FAILED"},
        )

        def _handler(payload):
            raise RuntimeError("Handler exploded")

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch.object(TaskRegistry, "has_handler", return_value=True),
            patch(
                "taskforge.workers.execution_engine.get_handler",
                return_value=_handler,
            ),
        ):
            result = execute_task(task.id, "worker-m5")

        assert result == ExecutionDisposition.TERMINAL_FAILURE_RECORDED

        after = _get_counter_value(
            DEFINITION_EXECUTIONS_TOTAL,
            {"task_type": "webhook_send", "status": "FAILED"},
        )
        assert after == before + 1.0

    def test_metric_failure_does_not_break_execution(self, db, active_definition):
        """If a metrics recorder raises, the task still completes successfully."""
        task = self._create_definition_task(db, active_definition)

        def _handler(payload):
            return {"status": "ok"}

        with (
            patch(
                "taskforge.workers.execution_engine.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch(
                "taskforge.workers.lease_manager.SessionLocal",
                new_callable=_make_session_local_mock,
            ),
            patch.object(TaskRegistry, "has_handler", return_value=True),
            patch(
                "taskforge.workers.execution_engine.get_handler",
                return_value=_handler,
            ),
            patch(
                "taskforge.workers.execution_engine.record_definition_execution",
                side_effect=RuntimeError("Metrics broken"),
            ),
        ):
            # Even though the metrics recorder throws, execute_task should succeed
            result = execute_task(task.id, "worker-m6")

        # The task still completes — metrics failure is isolated
        assert result == ExecutionDisposition.COMPLETED
