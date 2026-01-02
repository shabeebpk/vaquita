"""Insufficient Signal Handler: Input Request.

Records that no viable hypotheses exist.
Stores explanation, updates job status to NEED_MORE_INPUT, and requests more data.
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


class InsufficientSignalHandler(Handler):
    """Requests more input when signal is insufficient."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute input request.
        
        - Records insufficient signal
        - Updates job status to NEED_MORE_INPUT
        - Emits event requesting more documents or context
        """
        try:
            measurements = decision_result.get("measurements", {})
            
            # Build explanation from measurements
            explanation = (
                f"Insufficient evidence to make a confident decision. "
                f"Signal strength: {measurements.get('total_signal_strength', 0):.2f}, "
                f"Coverage: {measurements.get('coverage', 0):.2f}, "
                f"Viable hypotheses: {len([h for h in hypotheses if h.get('passed_filter')])}"
            )
            
            # Update job status
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "NEED_MORE_INPUT"
                    session.commit()
                    logger.info(f"Job {job_id} marked NEED_MORE_INPUT by InsufficientSignalHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            input_request_context = {
                "job_id": job_id,
                "explanation": explanation,
                "requested_at": datetime.utcnow().isoformat(),
                "suggestions": [
                    "Provide additional documents or sources",
                    "Refine your search or query",
                    "Add more context about your research question",
                ],
                "measurements": {
                    "signal_strength": measurements.get("total_signal_strength", 0),
                    "coverage": measurements.get("coverage", 0),
                    "viable_hypotheses": len([h for h in hypotheses if h.get("passed_filter")]),
                },
            }
            
            logger.info(f"Job {job_id} marked insufficient signal: {explanation}")
            
            return HandlerResult(
                status="deferred",
                message="Insufficient signal: please provide more documents or context",
                next_action="request_input",
                data=input_request_context,
            )
        
        except Exception as e:
            logger.error(f"InsufficientSignalHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"Failed to process insufficient signal: {str(e)}",
                next_action="notify_user",
            )


# Register this handler
register_handler("insufficient_signal", InsufficientSignalHandler)
