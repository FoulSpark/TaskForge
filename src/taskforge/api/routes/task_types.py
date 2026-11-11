"""Runtime task-type catalog endpoint for Phase 10 Web Console."""

from fastapi import APIRouter, Depends

from taskforge.api.dependencies import require_scope
from taskforge.api.schemas import TaskTypesCatalogResponse
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.tasks.catalog import build_task_types_catalog

router = APIRouter(prefix="/task-types", tags=["Tasks"])


@router.get(
    "",
    response_model=TaskTypesCatalogResponse,
    summary="List registered task types and payload schemas",
    description="Returns runtime-registered handlers, display metadata, default JSON payloads, and submission bounds.",
)
def list_task_types(
    _principal: AuthenticatedPrincipal = Depends(require_scope("tasks:read")),
):
    return build_task_types_catalog()
