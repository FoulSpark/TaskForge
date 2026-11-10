from fastapi import APIRouter, Depends, HTTPException, Query, Request, Response, status
from sqlalchemy.orm import Session

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import (
    TaskCreateRequest,
    TaskListResponse,
    TaskResponse,
)
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.database.session import get_db
from taskforge.tasks.service import TaskService

router = APIRouter(prefix="/tasks", tags=["Tasks"])


@router.post(
    "",
    response_model=TaskResponse,
    summary="Submit a new asynchronous task",
    description="Submits a task for asynchronous background processing with transactional persistence.",
)
def submit_task(
    payload: TaskCreateRequest,
    response: Response,
    request: Request,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:write")),
):
    service = TaskService(db)
    task, is_created = service.submit_task(
        payload,
        tenant_id=principal.tenant_id,
        actor_id=principal.key_id,
    )
    try:
        request.state.created_task_id = task.id
    except Exception:
        pass
    response.status_code = status.HTTP_201_CREATED if is_created else status.HTTP_200_OK
    return task


@router.get(
    "/{task_id}",
    response_model=TaskResponse,
    summary="Get task details by ID",
    description="Retrieve task lifecycle details, execution attempts, events, and results scoped to tenant.",
)
def get_task(
    task_id: str,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    service = TaskService(db)
    task = service.get_task(task_id, tenant_id=principal.tenant_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found.",
        )
    return task


@router.get(
    "",
    response_model=TaskListResponse,
    summary="List tasks with filtering and pagination",
    description="Retrieve a paginated list of tasks filtered by status, task_type, and priority scoped to tenant.",
)
def list_tasks(
    status: str | None = Query(None, description="Filter by task status"),
    task_type: str | None = Query(None, description="Filter by task type"),
    priority: int | None = Query(None, ge=1, le=4, description="Filter by priority (1 to 4)"),
    limit: int = Query(20, ge=1, le=100, description="Items per page"),
    offset: int = Query(0, ge=0, description="Pagination offset"),
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    service = TaskService(db)
    tasks, total = service.list_tasks(
        status=status,
        task_type=task_type,
        priority=priority,
        limit=limit,
        offset=offset,
        tenant_id=principal.tenant_id,
    )
    return TaskListResponse(
        items=[TaskResponse.model_validate(t) for t in tasks],
        total=total,
        limit=limit,
        offset=offset,
        has_more=(offset + limit) < total,
    )


@router.post(
    "/{task_id}/cancel",
    response_model=TaskResponse,
    summary="Cancel an active or queued task",
    description="Transitions a pending, queued, or running task to CANCELLED state.",
)
def cancel_task(
    task_id: str,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:write")),
):
    service = TaskService(db)
    task = service.get_task(task_id, tenant_id=principal.tenant_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found.",
        )
    try:
        cancelled = service.cancel_task(task_id)
        return cancelled
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc


@router.post(
    "/{task_id}/retry",
    response_model=TaskResponse,
    summary="Manually retry a failed or dead-letter task",
    description="Resets retry counter and transitions task back to QUEUED.",
)
def retry_task(
    task_id: str,
    db: Session = Depends(get_db),
    principal: AuthenticatedPrincipal = Depends(require_scope("tasks:write")),
):
    service = TaskService(db)
    task = service.get_task(task_id, tenant_id=principal.tenant_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Task with ID '{task_id}' not found.",
        )
    try:
        retried = service.manual_retry(task_id)
        return retried
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
