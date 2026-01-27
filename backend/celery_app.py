from celery import Celery
import logging

logging.basicConfig(level=logging.INFO)

import os

celery_app = Celery(
    "worker",
    broker=os.getenv("CELERY_BROKER_URL"),
    backend=os.getenv("CELERY_RESULT_BACKEND"),
)


celery_app.conf.imports = (
    "worker.stage_tasks",
)


    # "worker",
    # broker=os.getenv("CELERY_BROKER_URL", "redis://localhost:6379/0"),
    # backend=os.getenv("CELERY_RESULT_BACKEND", "redis://localhost:6379/1"),