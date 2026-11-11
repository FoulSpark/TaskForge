from fastapi import APIRouter, Depends

from taskforge.api.dependencies import require_scope
from taskforge.core.auth import AuthenticatedPrincipal
from taskforge.workers.heartbeat import get_active_workers

router = APIRouter(prefix="/workers", tags=["Workers"])


@router.get("", summary="List all active workers and their health metrics")
def list_workers(
    _principal: AuthenticatedPrincipal = Depends(require_scope("admin:operator")),
):
    workers = get_active_workers()
    return {
        "active_workers_count": len(workers),
        "workers": workers,
    }
