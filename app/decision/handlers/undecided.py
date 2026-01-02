"""Undecided Handler: Safety.

Records system uncertainty when no decision provider reached consensus.
Updates job status to MANUAL_REVIEW and emits safe fallback event.
No retries or automatic actions.
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


class UndecidedHandler(Handler):
    """Handles system uncertainty by escalating to manual review."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute safety escalation.
        
        - Records system uncertainty
        - Updates job status to MANUAL_REVIEW
        - Emits safe fallback event
        """
        try:
            fallback_used = decision_result.get("fallback_used", False)
            fallback_reason = decision_result.get("fallback_reason")
            provider_used = decision_result.get("provider_used")
            measurements = decision_result.get("measurements", {})
            
            # Update job status
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "MANUAL_REVIEW"
                    session.commit()
                    logger.info(f"Job {job_id} marked MANUAL_REVIEW by UndecidedHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            # Build context for manual review
            manual_review_context = {
                "job_id": job_id,
                "reason": "System unable to reach confident decision",
                "decision_result": {
                    "fallback_used": fallback_used,
                    "fallback_reason": fallback_reason,
                    "provider_used": provider_used,
                },
                "measurements": measurements,
                "hypothesis_count": len(hypotheses),
                "top_hypotheses": hypotheses[:5] if len(hypotheses) >= 5 else hypotheses,
                "escalated_at": datetime.utcnow().isoformat(),
            }
            
            logger.warning(f"Job {job_id} escalated to manual review due to system uncertainty (fallback={fallback_used}, reason={fallback_reason})")
            
            return HandlerResult(
                status="deferred",
                message="System uncertain; escalated to manual review",
                next_action="notify_admin",
                data=manual_review_context,
            )
        
        except Exception as e:
            logger.error(f"UndecidedHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to escalate to manual review: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("undecided", UndecidedHandler)
