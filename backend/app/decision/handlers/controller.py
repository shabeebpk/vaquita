"""Handler Orchestrator: Execution and Coordination.

The controller receives a decision label, looks up the corresponding handler,
invokes it, and coordinates status updates. This layer contains no decision logic,
only handler lookup and invocation orchestration.
"""

import logging
from typing import Dict, Any

from app.decision.handlers.registry import get_handler_for_decision
from app.decision.handlers.base import Handler, HandlerResult

logger = logging.getLogger(__name__)


class HandlerController:
    """Orchestrates handler invocation and result persistence.
    
    Responsibilities:
    - Look up handler from registry by decision label
    - Instantiate and invoke the handler
    - Log and return handler results
    - No decision logic; pure orchestration
    """
    
    def execute_handler(
        self,
        decision_label: str,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute the handler for a given decision.
        
        Args:
            decision_label: The decision string (e.g., "halt_confident").
            job_id: The job being processed.
            decision_result: Phase-5 decision dict from DecisionController.
            semantic_graph: Phase-3 semantic graph (read-only).
            hypotheses: List of hypotheses (read-only).
            job_metadata: Job context.
        
        Returns:
            HandlerResult indicating success, failure, or deferred action.
        
        Raises:
            ValueError: If no handler is registered for the decision label.
        """
        # Look up handler
        handler_class = get_handler_for_decision(decision_label)
        
        if not handler_class:
            msg = f"No handler registered for decision: {decision_label}"
            logger.error(msg)
            raise ValueError(msg)
        
        # Instantiate handler
        handler = handler_class()
        
        logger.info(f"Invoking handler {handler_class.__name__} for job {job_id} decision {decision_label}")
        
        try:
            # Execute handler
            result = handler.handle(
                job_id=job_id,
                decision_result=decision_result,
                semantic_graph=semantic_graph,
                hypotheses=hypotheses,
                job_metadata=job_metadata,
            )
            
            logger.info(
                f"Handler {handler_class.__name__} completed for job {job_id}: "
                f"status={result.status}, message={result.message}"
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Handler {handler_class.__name__} raised exception for job {job_id}: {e}")
            # Return error result from the handler exception
            return HandlerResult(
                status="error",
                message=f"Handler execution failed: {str(e)}",
                next_action="notify_user",
            )
    
    def get_handler_names(self) -> set:
        """Get all registered handler names for introspection/validation."""
        from app.decision.handlers.registry import get_global_registry
        return get_global_registry().all_labels()


def get_handler_controller() -> HandlerController:
    """Factory function to instantiate a HandlerController."""
    return HandlerController()
