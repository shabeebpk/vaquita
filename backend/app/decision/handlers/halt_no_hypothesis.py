"""Halt No Hypothesis Handler: Finalization.

Records that no viable hypothesis paths exist and halts execution.
Updates job status to COMPLETED with NO_HYPOTHESIS outcome.
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


@register_handler("halt_no_hypothesis")
class HaltNoHypothesisHandler(Handler):
    """Handles termination when no hypothesis path exists."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute halt with no hypothesis.
        
        - Records no hypothesis paths detected
        - Updates job status to COMPLETED (no further action)
        - Returns termination reason for UI
        """
        try:
            measurements = decision_result.get("measurements", {})
            
            # Build termination reason from measurements
            reason = (
                f"No viable hypothesis paths detected. "
                f"Passed hypotheses: {measurements.get('passed_hypothesis_count', 0)}, "
                f"Growth score: {measurements.get('growth_score', 0):.3f}, "
                f"Max paths per pair: {measurements.get('max_paths_per_pair', 0)}"
            )
            
            # Update job status to COMPLETED (no further action)
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "COMPLETED"
                    session.commit()
                    logger.info(f"Job {job_id} marked COMPLETED (NO_HYPOTHESIS) by HaltNoHypothesisHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            # Build termination context
            termination_context = {
                "job_id": job_id,
                "outcome": "halt_no_hypothesis",
                "reason": reason,
                "measurements": {
                    "passed_hypothesis_count": measurements.get("passed_hypothesis_count", 0),
                    "growth_score": measurements.get("growth_score", 0),
                    "max_paths_per_pair": measurements.get("max_paths_per_pair", 0),
                    "stable": measurements.get("diversity_score", 0) > 0 and measurements.get("graph_density", 0) > 0,
                },
                "terminated_at": datetime.utcnow().isoformat(),
            }
            
            logger.info(f"Job {job_id} halted: no hypothesis paths. {reason}")
            
            return HandlerResult(
                status="ok",
                message="Job terminated: no viable hypothesis paths detected",
                next_action="show_termination_reason",
                data=termination_context,
            )
        
        except Exception as e:
            logger.error(f"HaltNoHypothesisHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Handler execution failed: {str(e)}",
                next_action="notify_admin",
            )
