"""Fetch More Literature Handler: Scheduling.

Records a fetch intent without fetching directly.
Updates job status to FETCH_QUEUED and enqueues a background task for future ingestion.
No immediate pipeline restart happens.

Before setting FETCH_QUEUED, checks if max papers limit is reached.
If max papers reached, halts with NO_HYPOTHESIS status instead.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, JobPaperEvidence
from app.config.system_settings import system_settings

logger = logging.getLogger(__name__)


@register_handler("fetch_more_literature")
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
        
        - Checks if max papers limit is reached
        - If limit reached: halts with NO_HYPOTHESIS status (COMPLETED)
        - If limit not reached: records fetch intent and sets FETCH_QUEUED
        """
        try:
            # Get max papers limit from system settings
            max_papers = system_settings.SYSTEM_MAX_PAPERS_PER_JOB
            
            with Session(engine) as session:
                # Count current papers for this job in JobPaperEvidence
                paper_count = session.query(JobPaperEvidence).filter(
                    JobPaperEvidence.job_id == job_id
                ).count()
                
                job = session.query(Job).filter(Job.id == job_id).first()
                if not job:
                    logger.warning(f"Job {job_id} not found for status update")
                    return HandlerResult(status="error", message=f"Job {job_id} not found", next_action="notify_user")
                
                # Check if we've reached max papers limit
                if paper_count >= max_papers:
                    job.status = "COMPLETED"
                    session.commit()
                    logger.info(
                        f"Job {job_id} reached max papers limit ({paper_count}/{max_papers}). "
                        f"Halting with NO_HYPOTHESIS."
                    )
                    
                    return HandlerResult(
                        status="ok",
                        message=f"Maximum papers limit reached ({paper_count}/{max_papers}). Job completed.",
                        next_action="show_termination_reason",
                        data={
                            "outcome": "max_papers_reached",
                            "current_paper_count": paper_count,
                            "max_papers": max_papers,
                            "reason": "System reached maximum number of papers for this job"
                        }
                    )
                
                # Limit not reached - proceed with fetch scheduling
                job.status = "FETCH_QUEUED"
                session.commit()
                logger.info(
                    f"Job {job_id} marked FETCH_QUEUED by FetchMoreLiteratureHandler "
                    f"(current papers: {paper_count}/{max_papers})"
                )
            
            # Extract reason from measurements if available
            measurements = decision_result.get("measurements", {})
            reason = measurements.get("reason_for_fetch", "Insufficient evidence; need more data")
            
            # Record fetch intent
            fetch_intent = {
                "job_id": job_id,
                "decision_label": decision_result.get("decision_label"),
                "reason": reason,
                "scheduled_at": datetime.utcnow().isoformat(),
                "current_paper_count": paper_count,
                "max_papers": max_papers,
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


# FetchMoreLiteratureHandler is now registered via decorator above.
