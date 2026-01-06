"""
API Response schemas for chat, upload, and ingestion endpoints.

These schemas provide lightweight responses that focus on status and next
expected action, without exposing internal implementation details.
"""

from pydantic import BaseModel
from typing import List, Optional, Literal


# ============================================================================
# Chat API Responses
# ============================================================================

class ChatMessageResponse(BaseModel):
    """Response from chat message submission."""
    job_id: int
    message_id: int
    classification: Literal["content", "intent", "greeting", "mixed"]
    next_expected_action: str
    
    class Config:
        description = """
        Response from submitting a chat message.
        
        Fields:
        - job_id: Associated job ID (existing or newly created)
        - message_id: ID of the stored ConversationMessage row
        - classification: Result of text classification (content, intent, greeting, or mixed)
        - next_expected_action: Human-readable description of what happens next
          (e.g., "Ingestion will process your input" or "Please provide more details")
        """


# ============================================================================
# Upload API Responses
# ============================================================================

class UploadResponse(BaseModel):
    """Response from file upload."""
    job_id: int
    uploaded_files: List[str]
    extraction_enqueued: bool
    next_expected_action: str
    
    class Config:
        description = """
        Response from uploading files.
        
        Fields:
        - job_id: Associated job ID (existing or newly created)
        - uploaded_files: List of filenames that were uploaded
        - extraction_enqueued: True if extraction task was enqueued (always True for success)
        - next_expected_action: Description of what happens next (e.g., "Files will be extracted")
        """


# ============================================================================
# Ingestion Status Responses
# ============================================================================

class IngestionStatusResponse(BaseModel):
    """Status of ingestion processing for a job."""
    job_id: int
    status: str
    sources_processed: int
    blocks_created: int
    last_update: Optional[str] = None
    
    class Config:
        description = """
        Status response for querying ingestion progress.
        
        Fields:
        - job_id: Associated job ID
        - status: Current job status (e.g., "READY_TO_INGEST", "INGESTED", "FAILED")
        - sources_processed: Number of IngestionSource rows processed
        - blocks_created: Total TextBlock rows created
        - last_update: ISO timestamp of last status change
        """


class IngestionResponse(BaseModel):
    """
    Legacy response from ingestion endpoint (deprecated).
    
    Kept for backward compatibility. New code should use ChatMessageResponse,
    UploadResponse, and IngestionStatusResponse instead.
    """
    job_id: int
    uploaded_files: List[str]
