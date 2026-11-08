from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.exceptions import HTTPException as StarletteHTTPException

from taskforge.api.schemas import APIErrorResponse, ErrorDetail
from taskforge.core.logging import get_logger
from taskforge.core.state_machine import InvalidStateTransitionError

logger = get_logger(__name__)


class IdempotencyConflictError(Exception):
    """Raised when an idempotency key is submitted with a conflicting payload or configuration."""

    def __init__(
        self,
        key: str,
        message: str = "Idempotency key collision with conflicting payload.",
    ):
        self.key = key
        self.message = message
        super().__init__(message)


def get_correlation_id(request: Request) -> str:
    return getattr(request.state, "correlation_id", "unknown")


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(InvalidStateTransitionError)
    async def invalid_state_transition_handler(
        request: Request, exc: InvalidStateTransitionError
    ) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.warning(
            "invalid_state_transition",
            current_status=exc.current_status.value,
            new_status=exc.new_status.value,
            correlation_id=cid,
        )
        return JSONResponse(
            status_code=409,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="INVALID_STATE_TRANSITION",
                    message=str(exc),
                    details={
                        "current_status": exc.current_status.value,
                        "attempted_status": exc.new_status.value,
                    },
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    @app.exception_handler(IdempotencyConflictError)
    async def idempotency_conflict_handler(
        request: Request, exc: IdempotencyConflictError
    ) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.warning(
            "idempotency_payload_conflict",
            idempotency_key=exc.key,
            error=str(exc),
            correlation_id=cid,
        )
        return JSONResponse(
            status_code=409,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="IDEMPOTENCY_CONFLICT",
                    message=exc.message,
                    details={"idempotency_key": exc.key},
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    @app.exception_handler(RequestValidationError)
    async def validation_exception_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.info("request_validation_failed", errors=exc.errors(), correlation_id=cid)
        return JSONResponse(
            status_code=422,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="VALIDATION_ERROR",
                    message="Request payload validation failed.",
                    details=exc.errors(),
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    @app.exception_handler(StarletteHTTPException)
    async def http_exception_handler(request: Request, exc: StarletteHTTPException) -> JSONResponse:
        cid = get_correlation_id(request)
        return JSONResponse(
            status_code=exc.status_code,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code=f"HTTP_{exc.status_code}",
                    message=str(exc.detail),
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    from psycopg import OperationalError as PsycopgOperationalError
    from sqlalchemy.exc import OperationalError as SQLAOperationalError
    from sqlalchemy.exc import TimeoutError as SQLATimeoutError

    @app.exception_handler(SQLATimeoutError)
    async def db_pool_timeout_handler(request: Request, exc: SQLATimeoutError) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.error("database_pool_timeout", correlation_id=cid)
        return JSONResponse(
            status_code=503,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="SERVICE_UNAVAILABLE",
                    message="Database connection pool is temporarily saturated. Please retry shortly.",
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    @app.exception_handler(SQLAOperationalError)
    @app.exception_handler(PsycopgOperationalError)
    async def db_operational_error_handler(request: Request, exc: Exception) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.error("database_connection_failure", correlation_id=cid)
        return JSONResponse(
            status_code=503,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="SERVICE_UNAVAILABLE",
                    message="Database service is temporarily unavailable. Please retry shortly.",
                    correlation_id=cid,
                )
            ).model_dump(),
        )

    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
        cid = get_correlation_id(request)
        logger.exception("unhandled_server_error", error=str(exc), correlation_id=cid)
        return JSONResponse(
            status_code=500,
            content=APIErrorResponse(
                error=ErrorDetail(
                    code="INTERNAL_SERVER_ERROR",
                    message="An unexpected server error occurred.",
                    correlation_id=cid,
                )
            ).model_dump(),
        )
