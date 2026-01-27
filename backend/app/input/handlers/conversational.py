"""Conversational Handler: Basic interaction."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler

logger = logging.getLogger(__name__)

@register_classifier_handler("CONVERSATIONAL")
class ConversationalHandler(ClassifierHandler):
    """Handles greetings, chit-chat, and ambiguous inputs."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        raw_text = payload.get("raw_text", "")
        logger.info(f"Handling conversational input for job {job_id}")
        
        # In a real implementation, this might call an LLM to chat.
        # For now, we prepare the state for a conversational reply.
        return ClassifierHandlerResult(
            status="ok",
            message="Conversation acknowledged.",
            action_taken="prepared_chat_reply",
            next_step="generate_response"
        )
