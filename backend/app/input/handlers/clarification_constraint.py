"""Clarification Constraint Handler: Scoping."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler

logger = logging.getLogger(__name__)

@register_classifier_handler("CLARIFICATION_CONSTRAINT")
class ClarificationConstraintHandler(ClassifierHandler):
    """Handles scoping instructions and filters."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        constraints = payload.get("constraints", {})
        logger.info(f"Applying constraints to job {job_id}: {constraints}")
        
        # This update would persist to job.config or a dedicated Scoping table.
        return ClassifierHandlerResult(
            status="ok",
            message="Job scoping constraints updated.",
            action_taken="updated_constraints",
            job_state_updates={
                "constraints": constraints
            }
        )
