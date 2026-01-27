
"""
Main FastAPI application with chat-driven, iterative literature review system.

Architecture:
- Chat API (api/chat.py): Accept user messages, classify, create ConversationMessage
- Upload API (api/upload.py): Accept files, save, enqueue extraction

Separation of concerns ensures each component is independently testable and replaceable.
"""

import logging
from fastapi import FastAPI

from app.api.chat import router as chat_router
from app.api.upload import router as upload_router
from app.api.events import router as events_router
from app.api.hypotheses import router as hypotheses_router

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
app.include_router(events_router)
app.include_router(hypotheses_router)
