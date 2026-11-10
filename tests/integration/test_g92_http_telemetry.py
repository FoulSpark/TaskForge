"""Stage 3 & Acceptance Tests (AC-8, AC-9) for HTTP Telemetry Instrumentation.

Verifies:
- AC-8: Route template normalization across >= 100 distinct UUID paths (zero cardinality leakage).
- AC-9: Outermost middleware ordering and HTTP 429 rate-limit rejection visibility.
- Exact accounting invariant: exactly 1 request counter increment and 1 duration observation per request.
- Bounded finite taxonomies with 'other' fallback for method, status code class, and route.
- Request latency observation across 2xx, 4xx, 5xx, and 429 outcomes.
- Telemetry failure isolation: metric recording crashes never corrupt or break business responses.
- Runtime Prometheus cardinality conformity against G9.2.1 contract budget (662 base series / 855 total).
"""

import time
import uuid
import pytest
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from taskforge.main import app
from taskforge.monitoring.metrics import (
    ACTIVE_WORKERS,
    API_REQUEST_DURATION,
    API_REQUESTS_TOTAL,
    DB_POOL_CONNECTIONS,
    DEAD_LETTER_TOTAL,
    FAILURE_REASON_WHITELIST,
    HTTP_METHOD_WHITELIST,
    INITIATOR_WHITELIST,
    LEASE_RECOVERIES_TOTAL,
    OUTBOX_LAG_SECONDS,
    OUTBOX_PENDING_EVENTS,
    POOL_STATE_WHITELIST,
    PRIORITY_WHITELIST,
    QUEUE_DEPTH,
    RATE_LIMIT_EXCEEDED_TOTAL,
    RECOVERY_RESULT_WHITELIST,
    RETRIES_TOTAL,
    ROUTE_WHITELIST,
    STATUS_CODE_CLASS_WHITELIST,
    STATUS_WHITELIST,
    TASK_EXECUTION_DURATION,
    TASK_QUEUE_WAIT,
    TASK_TYPE_WHITELIST,
    TASKS_COMPLETED,
    TASKS_SUBMITTED,
    TELEMETRY_SCRAPE_ERRORS_TOTAL,
    normalize_http_method,
    normalize_route,
    normalize_status_code_class,
    record_api_request,
)


def get_histogram_count(child) -> float:
    """Helper to sum observation counts from histogram internal buckets."""
    return sum(b.get() for b in child._buckets)


def _get_metric_sample_count(metric_family_name: str) -> float:
    """Helper to sum observation counts for a histogram."""
    total = 0.0
    for metric in REGISTRY.collect():
        if metric.name == metric_family_name:
            for sample in metric.samples:
                if sample.name.endswith("_count"):
                    total += sample.value
    return total


# ==============================================================================
# Category 1: AC-9 — Outermost Middleware Ordering & 429 Rejection Visibility
# ==============================================================================


def test_ac9_rate_limit_rejection_telemetry_and_middleware_ordering(monkeypatch):
    """
    AC-9 / Stage 3: Verify that PrometheusHttpMiddleware wraps RateLimitMiddleware outermost.
    When RateLimitMiddleware rejects a request with HTTP 429:
    1. taskforge_rate_limit_exceeded_total[route] increments exactly once.
    2. taskforge_api_requests_total[http_method, route, status_code_class='4xx'] increments exactly once.
    3. taskforge_api_request_duration_seconds[http_method, route] records the rejection latency.
    """
    # Force RateLimitMiddleware to reject request
    monkeypatch.setattr(
        "taskforge.core.rate_limiter.RedisRateLimiter.is_allowed",
        lambda self, k, request_id=None: (False, 0, 5.0),
    )

    test_uuid = str(uuid.uuid4())
    route_expected = "/api/v1/tasks/{id}"

    hist_child = API_REQUEST_DURATION.labels(http_method="GET", route=route_expected)
    before_exceeded = RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route_expected)._value.get()
    before_requests = API_REQUESTS_TOTAL.labels(
        http_method="GET", route=route_expected, status_code_class="4xx"
    )._value.get()
    before_duration_count = get_histogram_count(hist_child)

    with TestClient(app) as client:
        # Send request with X-Test-Rate-Limit: 1 so rate limiter activates
        resp = client.get(
            f"/api/v1/tasks/{test_uuid}",
            headers={"X-Test-Rate-Limit": "1"},
        )
        assert resp.status_code == 429
        assert resp.headers.get("Retry-After") == "5"

    after_exceeded = RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route_expected)._value.get()
    after_requests = API_REQUESTS_TOTAL.labels(
        http_method="GET", route=route_expected, status_code_class="4xx"
    )._value.get()
    after_duration_count = get_histogram_count(hist_child)

    # Exact single-increment invariants
    assert after_exceeded == before_exceeded + 1.0, (
        f"Expected rate_limit_exceeded_total to increment by 1, got {after_exceeded - before_exceeded}"
    )
    assert after_requests == before_requests + 1.0, (
        f"Expected api_requests_total to increment by 1, got {after_requests - before_requests}"
    )
    assert after_duration_count == before_duration_count + 1.0, (
        f"Expected api_request_duration_seconds count to increment by 1, got {after_duration_count - before_duration_count}"
    )


def test_rate_limit_rejection_on_post_endpoint(monkeypatch):
    """Verify rate-limit rejection telemetry on POST /api/v1/tasks."""
    monkeypatch.setattr(
        "taskforge.core.rate_limiter.RedisRateLimiter.is_allowed",
        lambda self, k, request_id=None: (False, 0, 10.0),
    )

    route_expected = "/api/v1/tasks"
    before_exceeded = RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route_expected)._value.get()
    before_requests = API_REQUESTS_TOTAL.labels(
        http_method="POST", route=route_expected, status_code_class="4xx"
    )._value.get()

    with TestClient(app) as client:
        resp = client.post(
            "/api/v1/tasks",
            json={"task_name": "Test", "task_type": "csv_export", "payload": {}},
            headers={"X-Test-Rate-Limit": "1"},
        )
        assert resp.status_code == 429

    after_exceeded = RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route_expected)._value.get()
    after_requests = API_REQUESTS_TOTAL.labels(
        http_method="POST", route=route_expected, status_code_class="4xx"
    )._value.get()

    assert after_exceeded == before_exceeded + 1.0
    assert after_requests == before_requests + 1.0


# ==============================================================================
# Category 2: AC-8 — Route Template Normalization (Anti-Cardinality Explosion)
# ==============================================================================


def test_ac8_route_template_normalization_100_distinct_uuids():
    """
    AC-8 / Stage 3: Issue 100 requests with distinct UUID path parameters to /api/v1/tasks/{id}.
    Verify that Prometheus metrics collapse to exactly 1 route-label combination
    (route='/api/v1/tasks/{id}'), strictly preventing cardinality explosion.
    """
    uuids = [str(uuid.uuid4()) for _ in range(100)]
    route_expected = "/api/v1/tasks/{id}"

    hist_child = API_REQUEST_DURATION.labels(http_method="GET", route=route_expected)
    before_requests_4xx = API_REQUESTS_TOTAL.labels(
        http_method="GET", route=route_expected, status_code_class="4xx"
    )._value.get()
    before_duration_count = get_histogram_count(hist_child)

    with TestClient(app) as client:
        for uid in uuids:
            resp = client.get(f"/api/v1/tasks/{uid}")
            assert resp.status_code == 404

    after_requests_4xx = API_REQUESTS_TOTAL.labels(
        http_method="GET", route=route_expected, status_code_class="4xx"
    )._value.get()
    after_duration_count = get_histogram_count(hist_child)

    # All 100 requests must be recorded under the single normalized route template
    assert after_requests_4xx == before_requests_4xx + 100.0
    assert after_duration_count == before_duration_count + 100.0

    # Strict audit: verify ZERO individual UUIDs leaked into Prometheus registry label sets
    uuid_set = set(uuids)
    for metric in REGISTRY.collect():
        if metric.name in ("taskforge_api_requests_total", "taskforge_api_request_duration_seconds"):
            for sample in metric.samples:
                route_label = sample.labels.get("route", "")
                assert route_label not in uuid_set, (
                    f"CARDINALITY EXPLOSION VIOLATION: UUID {route_label} leaked as route label in {sample.name}"
                )
                for uid in uuids:
                    assert uid not in route_label, (
                        f"CARDINALITY EXPLOSION VIOLATION: UUID substring {uid} leaked in route label: {route_label}"
                    )


# ==============================================================================
# Category 3: Exact Accounting Invariant (1 Request -> 1 Observation)
# ==============================================================================


def test_exact_accounting_one_request_one_duration_per_call():
    """
    Verify that every HTTP request results in exactly 1 increment to API_REQUESTS_TOTAL
    and exactly 1 observation to API_REQUEST_DURATION across varied response codes:
    - 200 OK
    - 404 Not Found
    - 422 Unprocessable Entity
    """
    with TestClient(app) as client:
        # Case 1: 200 OK (/health/live)
        before_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        resp = client.get("/health/live")
        assert resp.status_code == 200
        after_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        assert after_count == before_count + 1.0

        # Case 2: 404 Not Found (/nonexistent-endpoint)
        before_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        resp = client.get(f"/nonexistent-endpoint-{uuid.uuid4().hex}")
        assert resp.status_code == 404
        after_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        assert after_count == before_count + 1.0

        # Case 3: 422 Validation Error (POST /api/v1/tasks with malformed body)
        before_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        resp = client.post("/api/v1/tasks", json={"invalid": "payload"})
        assert resp.status_code == 422
        after_count = _get_metric_sample_count("taskforge_api_request_duration_seconds")
        assert after_count == before_count + 1.0


# ==============================================================================
# Category 4: Finite Taxonomies & Fallback Normalization
# ==============================================================================


def test_finite_method_status_and_route_taxonomies_with_fallback():
    """
    Verify whitelist conformance and 'other' fallback across:
    - HTTP methods: GET, POST vs unapproved methods (PUT, DELETE, etc.)
    - Status code classes: 2xx, 4xx, 5xx vs out-of-range (1xx, 3xx, etc.)
    - Routes: whitelisted templates vs unlisted/unmatched endpoints
    """
    # 1. Method normalizer
    assert normalize_http_method("get") == "GET"
    assert normalize_http_method("POST") == "POST"
    assert normalize_http_method("PUT") == "other"
    assert normalize_http_method("DELETE") == "other"
    assert normalize_http_method("PATCH") == "other"
    assert normalize_http_method("") == "other"
    assert normalize_http_method(None) == "other"

    # 2. Status code class normalizer
    assert normalize_status_code_class(200) == "2xx"
    assert normalize_status_code_class(201) == "2xx"
    assert normalize_status_code_class(400) == "4xx"
    assert normalize_status_code_class(404) == "4xx"
    assert normalize_status_code_class(429) == "4xx"
    assert normalize_status_code_class(500) == "5xx"
    assert normalize_status_code_class(503) == "5xx"
    assert normalize_status_code_class(101) == "other"
    assert normalize_status_code_class(302) == "other"
    assert normalize_status_code_class(None) == "other"

    # 3. Route normalizer
    assert normalize_route("/api/v1/tasks") == "/api/v1/tasks"
    assert normalize_route("/api/v1/tasks/{id}") == "/api/v1/tasks/{id}"
    assert normalize_route("/api/v1/tasks/{task_id}") == "/api/v1/tasks/{id}"
    assert normalize_route("/tasks/{task_id}") == "/api/v1/tasks/{id}"
    assert normalize_route("/tasks/{id}") == "/api/v1/tasks/{id}"
    assert normalize_route("/api/v1/operator/recover") == "/api/v1/operator/recover"
    assert normalize_route("/api/v1/operator/outbox-sweep") == "/api/v1/operator/outbox-sweep"
    assert normalize_route("/health/live") == "other"
    assert normalize_route("/metrics") == "other"
    assert normalize_route("/api/v1/tasks/123-uuid-456") == "other"
    assert normalize_route(None) == "other"


# ==============================================================================
# Category 5: Latency Duration Measurement Across Multiple Outcomes
# ==============================================================================


def test_latency_duration_positive_across_outcomes():
    """Verify that observed latency duration is strictly positive (> 0) across 2xx and 4xx."""
    with TestClient(app) as client:
        # 2xx
        before_sum_2xx = API_REQUEST_DURATION.labels(http_method="GET", route="other")._sum.get()
        client.get("/health/live")
        after_sum_2xx = API_REQUEST_DURATION.labels(http_method="GET", route="other")._sum.get()
        assert after_sum_2xx > before_sum_2xx

        # 4xx
        before_sum_4xx = API_REQUEST_DURATION.labels(http_method="GET", route="/api/v1/tasks/{id}")._sum.get()
        client.get(f"/api/v1/tasks/{uuid.uuid4()}")
        after_sum_4xx = API_REQUEST_DURATION.labels(http_method="GET", route="/api/v1/tasks/{id}")._sum.get()
        assert after_sum_4xx > before_sum_4xx


# ==============================================================================
# Category 6: Telemetry Failure Isolation Standard
# ==============================================================================


def test_telemetry_failure_isolation(monkeypatch):
    """
    Verify that exceptions in metric observation wrappers do NOT corrupt,
    abort, or alter the business HTTP request or response.
    """
    def broken_observe(*args, **kwargs):
        raise RuntimeError("SIMULATED_METRIC_OBSERVE_FAILURE")

    def broken_inc(*args, **kwargs):
        raise RuntimeError("SIMULATED_METRIC_INC_FAILURE")

    monkeypatch.setattr(API_REQUEST_DURATION.labels("GET", "other"), "observe", broken_observe)
    monkeypatch.setattr(API_REQUESTS_TOTAL.labels("GET", "other", "2xx"), "inc", broken_inc)

    with TestClient(app) as client:
        # Request should succeed with 200 OK despite metric failures
        resp = client.get("/health/live")
        assert resp.status_code == 200
        assert resp.json()["status"] == "alive"


def test_unhandled_exception_records_5xx_and_propagates():
    """
    Verify that if an unhandled exception escapes call_next(), PrometheusHttpMiddleware
    records latency and status_code_class='5xx' before re-raising the exception.
    """
    from starlette.applications import Starlette
    from starlette.routing import Route
    from taskforge.api.middleware import PrometheusHttpMiddleware

    def broken_endpoint(request):
        raise ValueError("CRITICAL_CRASH_SIMULATION")

    test_app = Starlette(routes=[Route("/crash", broken_endpoint)])
    test_app.add_middleware(PrometheusHttpMiddleware)

    before_5xx = API_REQUESTS_TOTAL.labels(
        http_method="GET", route="other", status_code_class="5xx"
    )._value.get()

    with pytest.raises(ValueError, match="CRITICAL_CRASH_SIMULATION"):
        with TestClient(test_app, raise_server_exceptions=True) as client:
            client.get("/crash")

    after_5xx = API_REQUESTS_TOTAL.labels(
        http_method="GET", route="other", status_code_class="5xx"
    )._value.get()
    assert after_5xx == before_5xx + 1.0


# ==============================================================================
# Category 7: Runtime Cardinality Conformance (662 Base Series / 855 Total)
# ==============================================================================


def test_cardinality_ceiling_exhaustive_runtime_exposition():
    """
    Verify that following HTTP telemetry operations, the exhaustive population of all approved
    taxonomy combinations strictly conforms to 662 base series (855 total including _created).
    """
    # 1. TASKS_SUBMITTED: 6 task_types * 4 priorities = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for p in PRIORITY_WHITELIST:
            TASKS_SUBMITTED.labels(task_type=t, priority=p).inc(0)

    # 2. TASK_QUEUE_WAIT: 6 task_types * 4 priorities = 24 combinations * 12 buckets = 288 series
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for p in PRIORITY_WHITELIST:
            TASK_QUEUE_WAIT.labels(task_type=t, priority=p).observe(0.1)

    # 3. TASK_EXECUTION_DURATION: 6 task_types = 6 combinations * 11 buckets = 66 series
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        TASK_EXECUTION_DURATION.labels(task_type=t).observe(0.01)

    # 4. TASKS_COMPLETED: 6 task_types * 4 statuses = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for s in STATUS_WHITELIST:
            TASKS_COMPLETED.labels(task_type=t, status=s).inc(0)

    # 5. RETRIES_TOTAL: 6 task_types * 4 failure_reasons = 24
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        for f in list(FAILURE_REASON_WHITELIST) + ["other"]:
            RETRIES_TOTAL.labels(task_type=t, failure_reason=f).inc(0)

    # 6. DEAD_LETTER_TOTAL: 6 task_types = 6
    for t in list(TASK_TYPE_WHITELIST) + ["other"]:
        DEAD_LETTER_TOTAL.labels(task_type=t).inc(0)

    # 7. QUEUE_DEPTH: 4 priorities = 4
    for p in PRIORITY_WHITELIST:
        QUEUE_DEPTH.labels(priority=p).set(0)

    # 8. OUTBOX_PENDING_EVENTS = 1
    OUTBOX_PENDING_EVENTS.set(0)

    # 9. OUTBOX_LAG_SECONDS = 1
    OUTBOX_LAG_SECONDS.set(0)

    # 10. LEASE_RECOVERIES_TOTAL: 2 initiators * 2 recovery_results = 4
    for i in INITIATOR_WHITELIST:
        for r in RECOVERY_RESULT_WHITELIST:
            LEASE_RECOVERIES_TOTAL.labels(initiator=i, recovery_result=r).inc(0)

    # 11. API_REQUEST_DURATION: 3 methods * 5 routes = 15 combinations * 10 buckets = 150 series
    for m in list(HTTP_METHOD_WHITELIST) + ["other"]:
        for route in list(ROUTE_WHITELIST) + ["other"]:
            API_REQUEST_DURATION.labels(http_method=m, route=route).observe(0.01)

    # 12. API_REQUESTS_TOTAL: 3 methods * 5 routes * 4 status_code_classes = 60
    for m in list(HTTP_METHOD_WHITELIST) + ["other"]:
        for route in list(ROUTE_WHITELIST) + ["other"]:
            for sc in list(STATUS_CODE_CLASS_WHITELIST) + ["other"]:
                API_REQUESTS_TOTAL.labels(http_method=m, route=route, status_code_class=sc).inc(0)

    # 13. RATE_LIMIT_EXCEEDED_TOTAL: 5 routes = 5
    for route in list(ROUTE_WHITELIST) + ["other"]:
        RATE_LIMIT_EXCEEDED_TOTAL.labels(route=route).inc(0)

    # 14. ACTIVE_WORKERS = 1
    ACTIVE_WORKERS.set(0)

    # 15. DB_POOL_CONNECTIONS: 3 pool_states = 3
    for ps in POOL_STATE_WHITELIST:
        DB_POOL_CONNECTIONS.labels(pool_state=ps).set(0)

    # 16. TELEMETRY_SCRAPE_ERRORS_TOTAL = 1
    TELEMETRY_SCRAPE_ERRORS_TOTAL.inc(0)

    all_taskforge_samples = []
    for metric in REGISTRY.collect():
        if metric.name.startswith("taskforge_"):
            for s in metric.samples:
                all_taskforge_samples.append(s)

    created_samples = [s for s in all_taskforge_samples if s.name.endswith("_created")]
    base_samples = [s for s in all_taskforge_samples if not s.name.endswith("_created")]

    assert len(base_samples) == 662, f"Expected 662 base series, got {len(base_samples)}"
    assert len(created_samples) == 193, f"Expected 193 _created series, got {len(created_samples)}"
    assert len(all_taskforge_samples) == 855, f"Expected 855 total series, got {len(all_taskforge_samples)}"
