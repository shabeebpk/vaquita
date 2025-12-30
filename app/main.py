
import logging
from fastapi import FastAPI
from threading import Thread

from app.api.upload import router as upload_router
from app.api.stream import router as stream_router
from app.api.hypotheses import router as hypotheses_router
from app.worker.runner import start_worker

logging.basicConfig(level=logging.INFO)

app = FastAPI()
app.include_router(upload_router)
app.include_router(stream_router)
app.include_router(hypotheses_router)

from app.core.queues import job_queue
job_queue.put(6)

Thread(target=start_worker, daemon=True).start()
