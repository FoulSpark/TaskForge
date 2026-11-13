"""API routes for Task Definitions and Controlled Execution."""

from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.orm import Session

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import (
    DefinitionAuditListResponse,
    DefinitionExecuteRequest,
    DefinitionExecutionDetailResponse,
    DefinitionExecutionListResponse,
    TaskResponse,
)
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.session import get_db
from taskforge.definitions.service import (
    DefinitionConflictError,
    DefinitionNotFoundError,
    DefinitionPolicyError,
    DefinitionService,
    DefinitionValidationError,
    IdempotencyConflictError,
)

router = APIRouter(prefix="/definitions", tags=["Definitions"])


@router.post(
    "/{definition_id}/execute",
    response_model=TaskResponse,
    summary="Execute a task definition version",
    description="Submits an execution request against an active task definition version.",
)
def execute_definition(
    definition_id: str,
    payload: DefinitionExecuteRequest,
    response: Response,
    request: Request,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:write")),
):
    service = DefinitionService(db)
    try:
        task, is_created = service.execute_definition(
            principal=principal,
            definition_id=definition_id,
            request=payload,
        )
        try:
            request.state.created_task_id = task.id
        except Exception:
            pass
        response.status_code = status.HTTP_201_CREATED if is_created else status.HTTP_200_OK
        return task
    except DefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
    except IdempotencyConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except DefinitionConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=str(exc),
        ) from exc
    except (DefinitionValidationError, DefinitionPolicyError) as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=str(exc),
        ) from exc


@router.get(
    "/{definition_id}/executions",
    response_model=DefinitionExecutionListResponse,
    summary="List definition executions",
    description="Retrieves a paginated list of tasks executed under a definition with deterministic ordering and status/version filtering.",
)
def list_definition_executions(
    definition_id: str,
    status_filter: str | None = Query(None, alias="status", description="Filter by TaskStatus"),
    version_number: int | None = Query(
        None, ge=1, description="Filter by definition version number"
    ),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    service = DefinitionService(db)
    try:
        items, total = service.list_executions(
            principal=principal,
            definition_id=definition_id,
            status=status_filter,
            version_number=version_number,
            limit=limit,
            offset=offset,
        )
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except DefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{definition_id}/executions/{task_id}",
    response_model=DefinitionExecutionDetailResponse,
    summary="Get execution details",
    description="Deep inspection of an execution with immutable version snapshot, attempts, failure diagnosis, and scrubbed payload.",
)
def get_definition_execution_detail(
    definition_id: str,
    task_id: str,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    service = DefinitionService(db)
    try:
        return service.get_execution_detail(
            principal=principal,
            definition_id=definition_id,
            task_id=task_id,
        )
    except DefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc


@router.get(
    "/{definition_id}/audit",
    response_model=DefinitionAuditListResponse,
    summary="Get definition audit history",
    description="Retrieves a paginated, tenant-scoped audit event stream with sensitive-data redaction.",
)
def get_definition_audit_history(
    definition_id: str,
    action: str | None = Query(None, description="Filter by audit action"),
    limit: int = Query(50, ge=1, le=100, description="Items per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    service = DefinitionService(db)
    try:
        items, total = service.list_audit_events_paginated(
            principal=principal,
            definition_id=definition_id,
            action=action,
            limit=limit,
            offset=offset,
        )
        return {
            "items": items,
            "total": total,
            "limit": limit,
            "offset": offset,
        }
    except DefinitionNotFoundError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=str(exc),
        ) from exc
