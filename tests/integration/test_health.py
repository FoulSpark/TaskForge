
from fastapi.testclient import TestClient

from taskforge.main import app


class FakeRedis:
    def __init__(self, healthy=True):
        self.healthy = healthy

    def ping(self):
        if not self.healthy:
            from redis.exceptions import ConnectionError

            raise ConnectionError("Redis unavailable")

        return True

    def close(self):
        pass


def test_root_endpoint():
    with TestClient(app) as client:
        response = client.get("/")

    assert response.status_code == 200
    assert response.json()["application"] == "TaskForge"


def test_liveness_endpoint():
    with TestClient(app) as client:
        response = client.get("/health/live")

    assert response.status_code == 200
    assert response.json()["status"] == "alive"


def test_readiness_when_dependencies_are_healthy(monkeypatch):
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)

    with TestClient(app) as client:
        app.state.redis = FakeRedis(healthy=True)

        response = client.get("/health/ready")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"


def test_readiness_when_database_is_unavailable(monkeypatch):
    monkeypatch.setattr("taskforge.main.check_database", lambda: False)

    with TestClient(app) as client:
        app.state.redis = FakeRedis(healthy=True)

        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["database"] == "unavailable"


def test_readiness_when_redis_is_unavailable(monkeypatch):
    monkeypatch.setattr("taskforge.main.check_database", lambda: True)

    with TestClient(app) as client:
        app.state.redis = FakeRedis(healthy=False)

        response = client.get("/health/ready")

    assert response.status_code == 503
    assert response.json()["checks"]["redis"] == "unavailable"
