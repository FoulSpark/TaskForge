from celery import Celery
from taskforge.core.config import get_settings
from taskforge.workers.execution_engine import execute_task

settings = get_settings()

celery_app = Celery(
    "taskforge",
    broker=settings.redis_url,
    backend=settings.redis_url,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    task_reject_on_worker_lost=True,
)


@celery_app.task(name="taskforge.execute_task", bind=True)
def run_celery_task(self, task_id: str):
    worker_id = f"celery-{self.request.hostname}"
    success = execute_task(task_id, worker_id)
    return {"task_id": task_id, "success": success}
