"""Halt Confident Handler: Finalization.

Freezes the job when the system is confident in the best hypothesis.
Selects the top hypothesis, prepares final output, updates job status to COMPLETED.
No further pipeline execution occurs.
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


class HaltConfidentHandler(Handler):
    """Finalizes a job by selecting the top hypothesis and marking complete."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute finalization.
        
        - Selects top hypothesis by confidence or score
        - Updates job status to COMPLETED
        - Returns final output structure for UI
        """
        try:
            # Select top hypothesis (highest confidence or score)
            top_hypothesis = None
            if hypotheses:
                # Sort by confidence if available, otherwise by some default ranking
                sorted_hyp = sorted(
                    hypotheses,
                    key=lambda h: h.get("confidence", 0),
                    reverse=True
                )
                top_hypothesis = sorted_hyp[0]
            
            # Update job status to COMPLETED
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "COMPLETED"
                    session.commit()
                    logger.info(f"Job {job_id} marked COMPLETED by HaltConfidentHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            # Build final output structure
            final_output = {
                "job_id": job_id,
                "status": "completed",
                "top_hypothesis": top_hypothesis,
                "confidence": top_hypothesis.get("confidence") if top_hypothesis else 0.0,
                "decision_result": decision_result,
                "finalized_at": datetime.utcnow().isoformat(),
            }
            
            logger.info(f"Job {job_id} halted confident: top hypothesis={top_hypothesis.get('id') if top_hypothesis else 'none'}")
            
            return HandlerResult(
                status="ok",
                message=f"Job finalized with top hypothesis (confidence={final_output['confidence']:.2f})",
                next_action="show_final_result",
                data=final_output,
            )
        
        except Exception as e:
            logger.error(f"HaltConfidentHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to finalize job: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("halt_confident", HaltConfidentHandler)
