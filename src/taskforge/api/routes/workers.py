from fastapi import APIRouter
from taskforge.workers.heartbeat import get_active_workers

router = APIRouter(prefix="/workers", tags=["Workers"])


@router.get("", summary="List all active workers and their health metrics")
def list_workers():
    workers = get_active_workers()
    return {
        "active_workers_count": len(workers),
        "workers": workers,
    }

