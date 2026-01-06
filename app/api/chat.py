"""
Chat API: Handle user text messages and classify them for routing.

This endpoint:
1. Accepts user text + optional job_id
2. Stores the message as ConversationMessage (role=user, message_type=text)
3. Classifies the text using the text classifier
4. If content detected: creates IngestionSource and sets job status to READY_TO_INGEST
5. If intent/greeting: creates system ConversationMessage with next action
6. Never directly triggers ingestion, extraction, or other pipeline logic

Separation of concerns:
- Chat endpoint: accepts input, stores messages, classifies, queues for processing
- Background worker (runner.py): detects status changes, invokes services
- Services: execute only their assigned responsibility
"""

import logging
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

from app.storage.db import engine
from app.storage.models import Job, ConversationMessage, IngestionSource, MessageRole, MessageType
from app.input.classifier import get_classifier, ClassificationLabel
from app.schemas.ingestion import ChatMessageResponse
from app.core.queues import job_queue

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/chat", tags=["chat"])


class ChatMessageRequest(BaseModel):
    """Request body for chat message submission."""
    text: str
    job_id: Optional[int] = None


@router.post("/message", response_model=ChatMessageResponse)
async def submit_message(request: ChatMessageRequest) -> ChatMessageResponse:
    """
    Accept a user text message and classify it.
    
    Workflow:
    1. Create job if needed
    2. Store message in ConversationMessage (role=user)
    3. Classify text (content, intent, greeting, mixed)
    4. If content: create IngestionSource, set status to READY_TO_INGEST
    5. If intent/greeting: create system response, explain next action
    6. Enqueue job for processing
    7. Return lightweight response
    
    Args:
        text: User-provided text message
        job_id: Optional existing job ID (new job created if not provided)
    
    Returns:
        ChatMessageResponse with job_id, message_id, classification, next_expected_action
    
    Raises:
        HTTPException: If job not found (when job_id provided) or other errors
    """
    if not request.text or not request.text.strip():
        raise HTTPException(status_code=400, detail="Text cannot be empty")
    
    with Session(engine) as session:
        # Create or fetch job
        if request.job_id:
            job = session.query(Job).filter(Job.id == request.job_id).first()
            if not job:
                raise HTTPException(status_code=404, detail=f"Job {request.job_id} not found")
        else:
            # Create new job
            job = Job(status="CREATED")
            session.add(job)
            session.flush()  # Get the ID before commit
        
        # Store user message in ConversationMessage
        user_message = ConversationMessage(
            job_id=job.id,
            role=MessageRole.USER,
            message_type=MessageType.TEXT,
            content=request.text.strip()
        )
        session.add(user_message)
        session.flush()  # Get message ID
        
        # Classify the text
        classifier = get_classifier(mode="deterministic")  # Fast default; configurable later
        classification = classifier.classify(request.text)
        
        logger.info(
            f"Message {user_message.id} classified as {classification.label.value} "
            f"(confidence {classification.confidence:.2f})"
        )
        
        # Route based on classification
        if classification.is_content_available():
            # Content detected: create IngestionSource
            content_text = request.text if classification.label == ClassificationLabel.CONTENT else classification.content_portion
            
            ingestion_source = IngestionSource(
                job_id=job.id,
                source_type="user_text",
                source_ref=f"message:{user_message.id}",
                raw_text=content_text or request.text,
                processed=False
            )
            session.add(ingestion_source)
            session.flush()
            
            # Set job status to READY_TO_INGEST
            job.status = "READY_TO_INGEST"
            
            next_action = "Your message will be processed for content extraction and analysis."
            
            # If mixed, also create system message about the intent portion
            if classification.label == ClassificationLabel.MIXED and classification.intent_portion:
                system_message = ConversationMessage(
                    job_id=job.id,
                    role=MessageRole.SYSTEM,
                    message_type=MessageType.STATUS,
                    content=f"I detected a request: '{classification.intent_portion}'. I'll process your content and then help with that."
                )
                session.add(system_message)
        
        else:
            # No content: just intent or greeting
            if classification.label == ClassificationLabel.INTENT:
                next_action = "I received your request. Please provide content or documents to analyze, or I can search for relevant papers."
                system_content = "Intent detected. Please provide documents or specify topics to analyze."
            elif classification.label == ClassificationLabel.GREETING:
                next_action = "Hello! You can chat with me about your literature review. Share documents, ask questions, or request paper searches."
                system_content = "Greeting detected. Ready to assist with your literature review."
            else:
                next_action = "I didn't fully understand your input. Please share documents or provide more details."
                system_content = "Unclear input. Please provide more context or documents."
            
            # Create system response message
            system_message = ConversationMessage(
                job_id=job.id,
                role=MessageRole.SYSTEM,
                message_type=MessageType.STATUS,
                content=system_content
            )
            session.add(system_message)
            
            # Don't set READY_TO_INGEST; job remains in previous state
            # (or stays in CREATED if new job)
        
        # Commit all changes
        session.commit()
        
        # Enqueue job for runner to process
        job_queue.put(job.id)
        
        logger.info(
            f"Message {user_message.id} stored in job {job.id}; "
            f"job status: {job.status}; "
            f"classification: {classification.label.value}"
        )
        
        return ChatMessageResponse(
            job_id=job.id,
            message_id=user_message.id,
            classification=classification.label.value,
            next_expected_action=next_action
        )


@router.get("/job/{job_id}/messages")
async def get_job_messages(job_id: int):
    """
    Retrieve all messages for a job.
    
    This endpoint is used by the UI to reconstruct the conversation history
    without relying on SSE event streams.
    
    Args:
        job_id: Job ID
    
    Returns:
        List of ConversationMessage rows for the job
    """
    with Session(engine) as session:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        messages = session.query(ConversationMessage).filter(
            ConversationMessage.job_id == job_id
        ).order_by(ConversationMessage.created_at.asc()).all()
        
        return {
            "job_id": job_id,
            "status": job.status,
            "messages": [
                {
                    "id": m.id,
                    "role": m.role.value,
                    "message_type": m.message_type.value,
                    "content": m.content,
                    "created_at": m.created_at.isoformat() if m.created_at else None
                }
                for m in messages
            ]
        }
