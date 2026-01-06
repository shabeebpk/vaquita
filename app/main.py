
"""
Main FastAPI application with chat-driven, iterative literature review system.

Architecture:
- Chat API (api/chat.py): Accept user messages, classify, create ConversationMessage
- Upload API (api/upload.py): Accept files, save, enqueue extraction
- Background workers:
  - Extraction worker: Extract text from files, create IngestionSource
  - Orchestration runner: Detect status changes, invoke services, update state
- Services: Ingestion, triple extraction, graph building, decision control
- LLM service: Single gateway for all LLM calls (classifier, extraction, reasoning)

Separation of concerns ensures each component is independently testable and replaceable.
"""

import logging
from fastapi import FastAPI
from threading import Thread

from app.api.chat import router as chat_router
from app.api.upload import router as upload_router
from app.api.stream import router as stream_router
from app.api.hypotheses import router as hypotheses_router
from app.worker.runner import start_worker
from app.ingestion.extractor import start_extraction_worker

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Literature Review System",
    description="Chat-driven, iterative literature review with separation of concerns",
    version="1.0.0"
)

# Register API routers
app.include_router(chat_router)
app.include_router(upload_router)
app.include_router(stream_router)
app.include_router(hypotheses_router)


# Start background workers
def start_background_workers():
    """Start all background workers in daemon threads."""
    logger.info("Starting background workers...")
    
    # Orchestration runner
    orchestration_thread = Thread(target=start_worker, daemon=True, name="OrchestratorWorker")
    orchestration_thread.start()
    logger.info("Orchestration worker started")
        
    # Extraction worker
    extraction_thread = Thread(target=start_extraction_worker, daemon=True, name="ExtractionWorker")
    extraction_thread.start()
    logger.info("Extraction worker started")


# Start workers on app startup
start_background_workers()
