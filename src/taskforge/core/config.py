from functools import lru_cache
from typing import Any

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    app_name: str = "TaskForge"
    app_env: str = "development"
    debug: bool = True

    api_host: str = "127.0.0.1"
    api_port: int = Field(default=8000, ge=1, le=65535)

    database_url: str = "postgresql+psycopg://taskforge:taskforge_dev@localhost:5432/taskforge"
    redis_url: str = "redis://localhost:6379/0"
    broker_type: str = "redis"

    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30

    default_task_timeout: int = 60
    default_max_retries: int = 3
    worker_lease_duration_seconds: int = 30
    outbox_poll_interval_seconds: float = 0.5

    rate_limit_per_minute: int = 300
    rate_limit_enabled: bool = True
    start_embedded_workers: bool = True
    cache_enabled: bool = True
    cache_ttl_seconds: int = 30
    allow_sqlite_fallback: bool = False

    log_level: str = "INFO"

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("debug", mode="before")
    @classmethod
    def parse_debug(cls, v: Any) -> bool:
        if isinstance(v, str):
            return v.lower() in ("true", "1", "yes", "on", "t", "dev", "development")
        return bool(v)

    @model_validator(mode="after")
    def enforce_production_rules(self) -> "Settings":
        if self.app_env.lower() in ("prod", "production") and self.allow_sqlite_fallback:
            raise ValueError(
                "allow_sqlite_fallback is strictly prohibited in production environment."
            )
        return self


@lru_cache
def get_settings() -> Settings:
    return Settings()
