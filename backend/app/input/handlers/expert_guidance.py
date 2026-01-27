"""Expert Guidance Handler: Heuristics."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler

logger = logging.getLogger(__name__)

@register_classifier_handler("EXPERT_GUIDANCE")
class ExpertGuidanceHandler(ClassifierHandler):
    """Handles domain heuristics and theoretical hints."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        heuristics = payload.get("heuristics", [])
        assumptions = payload.get("assumptions", [])
        
        logger.info(f"Adding expert guidance to job {job_id}: {len(heuristics)} heuristics")
        
        # This update would persist to job.config or reasoning_bias table.
        return ClassifierHandlerResult(
            status="ok",
            message="Expert guidance and assumptions recorded.",
            action_taken="updated_heuristics",
            job_state_updates={
                "heuristics": heuristics,
                "assumptions": assumptions
            }
        )
