
"""
Main FastAPI application with chat-driven, iterative literature review system.

Architecture:
- Chat API (api/chat.py): Accept user messages, classify, create ConversationMessage
- Upload API (api/upload.py): Accept files, save, enqueue extraction

Separation of concerns ensures each component is independently testable and replaceable.
"""

import builtins
import logging
from fastapi import FastAPI

from app.api.chat import router as chat_router
from app.api.events import router as events_router
from app.api.test import router as test_router
from app.api.verification import router as verification_router

logging.basicConfig(level=logging.INFO)
# logger = logging.getLogger(__name__)
builtins.logger = logging.getLogger("app")

app = FastAPI(
    title="Literature Review System",
    description="Unified API for literature review analysis",
    version="1.1.0"
)


from fastapi.middleware.cors import CORSMiddleware

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
app.include_router(chat_router)
app.include_router(events_router)
app.include_router(test_router)
app.include_router(verification_router)
