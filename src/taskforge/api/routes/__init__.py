from fastapi import APIRouter

from taskforge.api.routes.chaos import router as chaos_router
from taskforge.api.routes.tasks import router as tasks_router
from taskforge.api.routes.workers import router as workers_router

api_v1_router = APIRouter(prefix="/api/v1")
api_v1_router.include_router(tasks_router)
api_v1_router.include_router(workers_router)
api_v1_router.include_router(chaos_router)
