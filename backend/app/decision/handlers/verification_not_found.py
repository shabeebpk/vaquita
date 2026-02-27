"""Verification Not Found Handler: No connection found between entities.

Stores verification result when source and target entities cannot be connected
after exhausting all search strategies. No further pipeline execution occurs.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, VerificationResult

logger = logging.getLogger(__name__)


@register_handler("verification_not_found")
class VerificationNotFoundHandler(Handler):
    """Completes verification job with negative result."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute verification not found handler.
        
        - Stores verification result as not found in verification_results table
        - Updates job status to COMPLETED
        - Returns result for UI
        """
        try:
            # Get verification context from job_metadata
            source = job_metadata.get("verification_source")
            target = job_metadata.get("verification_target")
            
            if not source or not target:
                return HandlerResult(
                    status="error",
                    message="Missing verification_source or verification_target in job_metadata",
                )
            
            # Get verification result for explanation (if available)
            verification_result = job_metadata.get("verification_result", {})
            explanation = verification_result.get("explanation", "No connection found after exhausting all search strategies")
            
            # Store result in database
            with Session(engine) as session:
                # Create verification result record
                vr = VerificationResult(
                    job_id=job_id,
                    source=source,
                    target=target,
                    connection_found=False,
                    connection_type=None,
                    path=None,
                    explanation=explanation,
                    supporting_papers=None,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                session.add(vr)
                
                # Update job status and result
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "COMPLETED"
                    job.result = {
                        "verification_status": "not_found",
                        "source": source,
                        "target": target,
                        "connection_type": None,
                        "explanation": explanation,
                        "reason": "No connection found after exhausting all search strategies",
                        "completed_at": datetime.utcnow().isoformat(),
                    }
                    session.commit()
                    logger.info(f"Job {job_id} verification NOT FOUND: {source} -> {target}")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
                    session.rollback()
            
            final_output = {
                "job_id": job_id,
                "status": "verification_not_found",
                "source": source,
                "target": target,
                "connection_found": False,
                "reason": "No connection found after exhausting all search strategies",
                "completed_at": datetime.utcnow().isoformat(),
            }
            
            return HandlerResult(
                status="ok",
                message=f"Verification complete: No connection found between {source} and {target}",
                next_action="show_verification_result",
                data=final_output,
            )
        
        except Exception as e:
            logger.error(f"VerificationNotFoundHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to complete verification: {str(e)}",
                next_action="notify_user",
            )
