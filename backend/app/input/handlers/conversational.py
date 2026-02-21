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
        answer = payload.get("answer")
        raw_text = payload.get("raw_text", "")
        
        # 1. Determine response content
        # Fallback if LLM missed the 'answer' key but gave the label
        content = answer if answer else f"I understand your message: '{raw_text}'. How can I help with your literature review?"
        
        logger.info(f"Handling conversational input for job {job_id}. Answer provided: {bool(answer)}")
        
        # 2. Persist Assistant Response in DB
        from app.storage.models import ConversationMessage, MessageRole, MessageType
        assistant_msg = ConversationMessage(
            job_id=job_id,
            role=MessageRole.SYSTEM.value,
            message_type=MessageType.TEXT.value,
            content=content
        )
        session.add(assistant_msg)
        session.flush() # Get ID
        
        # 3. Publish event to Redis (Traditional UI Bridge)
        from events import publish_event
        publish_event({
            "job_id": job_id,
            "type": "chat_message",
            "role": "assistant",
            "content": content,
            "message_id": assistant_msg.id
        })
        
        return ClassifierHandlerResult(
            status="ok",
            message="Conversation handled and persisted.",
            action_taken="published_and_stored_chat",
            next_step="await_user"
        )
