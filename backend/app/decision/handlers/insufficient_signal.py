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
from presentation.events import push_presentation_event

logger = logging.getLogger(__name__)


@register_handler("insufficient_signal")
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
            
            # 1. Group and rank hypotheses (including promising)
            from app.config.admin_policy import admin_policy
            from app.path_reasoning.persistence import group_top_hypotheses
            limit = admin_policy.algorithm.decision_thresholds.top_k_hypotheses_to_store
            ranked_pairs = group_top_hypotheses(hypotheses, limit=limit)

            logger.info(f"Job {job_id} marked insufficient signal: {explanation}")
            
            # Emit presentation event
            push_presentation_event(
                job_id=job_id,
                phase="DECISION",
                status="insufficientsignal",
                result={
                    "graph_size": semantic_graph.get("node_count", 0) if semantic_graph else 0,
                    "edge_count": semantic_graph.get("edge_count", 0) if semantic_graph else 0,
                    "hypothesis_count": measurements.get("passed_hypothesis_count", 0),
                    "growth_score": measurements.get("growth_score", 0),
                    "explanation": explanation,
                },
                metric={
                    "next_step": "need inputs",
                },
                payload={
                    "top_k_hypotheses": ranked_pairs,
                },
                next_action="need_inputs",
            )
            
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


# InsufficientSignalHandler is now registered via decorator above.
