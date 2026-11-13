"""API routes for Task Definitions and Controlled Execution."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from sqlalchemy.orm import Session

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import DefinitionExecuteRequest, TaskResponse
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
