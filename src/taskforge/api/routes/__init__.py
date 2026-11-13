from fastapi import APIRouter

from taskforge.api.routes.auth import router as auth_router
from taskforge.api.routes.chaos import router as chaos_router
from taskforge.api.routes.definitions import router as definitions_router
from taskforge.api.routes.operator import router as operator_router
from taskforge.api.routes.task_types import router as task_types_router
from taskforge.api.routes.tasks import router as tasks_router
from taskforge.api.routes.workers import router as workers_router

api_v1_router = APIRouter(prefix="/api/v1")
api_v1_router.include_router(auth_router)
api_v1_router.include_router(definitions_router)
api_v1_router.include_router(task_types_router)
api_v1_router.include_router(tasks_router)
api_v1_router.include_router(workers_router)
api_v1_router.include_router(chaos_router)
api_v1_router.include_router(operator_router)
