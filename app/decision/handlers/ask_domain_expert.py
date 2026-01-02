"""Ask Domain Expert Handler: Human Loop.

Flags the job as NEEDS_EXPERT_REVIEW and stores the reason from measurements.
No automation continues after this point.
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


class AskDomainExpertHandler(Handler):
    """Escalates to human domain expert for review."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute expert escalation.
        
        - Extracts reason from measurements (e.g., low diversity, ambiguity)
        - Updates job status to NEEDS_EXPERT_REVIEW
        - Emits event for human review UI
        """
        try:
            # Extract reason from measurements
            measurements = decision_result.get("measurements", {})
            reason = measurements.get("reason_for_expert_review", "Expert review requested")
            
            # Update job status
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "NEEDS_EXPERT_REVIEW"
                    session.commit()
                    logger.info(f"Job {job_id} marked NEEDS_EXPERT_REVIEW by AskDomainExpertHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            expert_review_context = {
                "job_id": job_id,
                "reason": reason,
                "user_text": job_metadata.get("user_text", ""),
                "hypothesis_count": len(hypotheses),
                "top_hypotheses": hypotheses[:3] if len(hypotheses) >= 3 else hypotheses,
                "escalated_at": datetime.utcnow().isoformat(),
                "measurements": measurements,
            }
            
            logger.info(f"Job {job_id} escalated to expert: {reason}")
            
            return HandlerResult(
                status="deferred",
                message=f"Awaiting expert review: {reason}",
                next_action="notify_expert",
                data=expert_review_context,
            )
        
        except Exception as e:
            logger.error(f"AskDomainExpertHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to escalate to expert: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("ask_domain_expert", AskDomainExpertHandler)
