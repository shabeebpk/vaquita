from celery import Celery
import logging

logging.basicConfig(level=logging.INFO)

celery_app = Celery(
    "worker",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
)

celery_app.conf.imports = (
    "worker.stage_tasks",
)
