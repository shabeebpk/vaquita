"""Verification API: Endpoint for creating verification jobs.

Provides an endpoint to initiate verification mode jobs with two entities,
checking if they are connected in the knowledge graph.
"""

import logging
from datetime import datetime
from fastapi import APIRouter, HTTPException, Form
from sqlalchemy.orm import Session

from app.storage.db import engine
from app.storage.models import Job, VerificationResult
from worker.stage_tasks import fetch_stage

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/verify", tags=["verification"])


@router.post("/")
async def create_verification_job(
    entity1: str = Form(...),
    entity2: str = Form(...)
):
    """Create a new verification job to check if two entities are connected.
    
    Args:
        entity1: First entity
        entity2: Second entity
        
    Returns:
        Dict with job_id, status, and verification details
        
    Raises:
        HTTPException: If job creation or verification result initialization fails
    """
    with Session(engine) as session:
        try:
            # Create new verification job
            job = Job(
                mode="verification",
                status="CREATED",
                job_config=None,  # Verification jobs don't use job_config
            )
            session.add(job)
            session.flush()
            
            job_id = job.id
            
            # Create VerificationResult record with entity pair
            verification_result = VerificationResult(
                job_id=job_id,
                source=entity1,
                target=entity2,
                connection_found=None,  # Not yet determined
                connection_type=None,   # Not yet determined
                path=None,              # Will be filled by path_reasoning stage
                explanation=None,       # Will be filled by path_reasoning stage
                supporting_papers=None, # Will be filled by path_reasoning stage
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
            )
            session.add(verification_result)
            
            # Update job to FETCH_QUEUED (ready for fetching literature)
            job.status = "FETCH_QUEUED"
            
            session.commit()
            
            logger.info(
                f"Created verification job {job_id} for entities: '{entity1}' -> '{entity2}'"
            )
            
            # Queue the fetch stage task
            fetch_stage.delay(job_id)
            
            return {
                "job_id": job_id,
                "status": "FETCH_QUEUED",
                "verification_id": verification_result.id,
                "message": f"Verification job created. Checking connection between '{entity1}' and '{entity2}'",
                "details": "Fetching and analyzing literature..."
            }
        
        except Exception as e:
            logger.error(f"Failed to create verification job: {e}")
            raise HTTPException(
                status_code=500,
                detail=f"Failed to create verification job: {str(e)}",
            )
