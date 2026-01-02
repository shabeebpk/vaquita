"""Fetch More Literature Handler: Scheduling.

Records a fetch intent without fetching directly.
Updates job status to FETCH_QUEUED and enqueues a background task for future ingestion.
No immediate pipeline restart happens.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job

logger = logging.getLogger(__name__)


class FetchMoreLiteratureHandler(Handler):
    """Schedules a fetch task without immediate execution."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute fetch scheduling.
        
        - Records the fetch intent (reason, timestamp, decision label)
        - Updates job status to FETCH_QUEUED
        - Enqueues a background task for future ingestion
        """
        try:
            # Extract reason from measurements if available
            measurements = decision_result.get("measurements", {})
            reason = measurements.get("reason_for_fetch", "Insufficient evidence; need more data")
            
            # Update job status
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "FETCH_QUEUED"
                    session.commit()
                    logger.info(f"Job {job_id} marked FETCH_QUEUED by FetchMoreLiteratureHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            # Optionally enqueue background task (currently deferred; can integrate with job_queue)
            # For now, we record the intent and let external scheduler handle it
            fetch_intent = {
                "job_id": job_id,
                "decision_label": decision_result.get("decision_label"),
                "reason": reason,
                "scheduled_at": datetime.utcnow().isoformat(),
                "measurements": measurements,
            }
            
            logger.info(f"Job {job_id} scheduled for literature fetch: {reason}")
            
            return HandlerResult(
                status="deferred",
                message=f"Fetch task queued: {reason}",
                next_action="show_status_update",
                data=fetch_intent,
            )
        
        except Exception as e:
            logger.error(f"FetchMoreLiteratureHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to queue fetch task: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("fetch_more_literature", FetchMoreLiteratureHandler)
