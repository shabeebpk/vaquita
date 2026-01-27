"""Classifier Handler Controller: Orchestration."""
import logging
from typing import Dict, Any, Optional
from app.input.handlers.registry import get_handler_for_label
from app.input.handlers.base import ClassifierHandlerResult

logger = logging.getLogger(__name__)

class ClassifierHandlerController:
    """Orchestrates the lookup and execution of classifier handlers."""

    def execute_handler(
        self,
        label: str,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        """Find and execute the handler for a given classification label.
        
        Args:
            label: The classification label (e.g., 'RESEARCH_SEED').
            job_id: The job context.
            payload: Structured JSON payload from LLM.
            session: DB session.
        """
        handler_class = get_handler_for_label(label)
        
        if not handler_class:
            msg = f"No classifier handler registered for label: {label}"
            logger.error(msg)
            return ClassifierHandlerResult(
                status="error",
                message=msg,
                action_taken="none"
            )
        
        # Instantiate and execute
        handler = handler_class()
        logger.info(f"Invoking {handler_class.__name__} for job {job_id}")
        
        try:
            return handler.handle(job_id, payload, session)
        except Exception as e:
            logger.error(f"Handler {handler_class.__name__} failed for job {job_id}: {e}")
            return ClassifierHandlerResult(
                status="error",
                message=f"Handler execution failed: {str(e)}",
                action_taken="none"
            )

def get_classifier_handler_controller() -> ClassifierHandlerController:
    """Factory for ClassifierHandlerController."""
    return ClassifierHandlerController()
