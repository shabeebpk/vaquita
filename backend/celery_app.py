from celery import Celery
import logging

logging.basicConfig(level=logging.INFO)

from app.config.system_settings import system_settings

celery_app = Celery(
    "worker",
    broker=system_settings.CELERY_BROKER_URL,
    backend=system_settings.CELERY_RESULT_BACKEND,
)


celery_app.conf.imports = (
    "worker.stage_tasks",
)


    # "worker",
    # broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    # backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),