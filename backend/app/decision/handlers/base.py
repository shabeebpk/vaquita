"""Base Handler Interface.

All decision handlers must conform to this contract.
Handlers receive a decision result and artifacts, execute an action, and return a structured result.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class HandlerResult:
    """Structured result from a handler execution.
    
    Attributes:
        status: One of 'ok', 'error', 'deferred'.
        message: Human-readable summary of what happened.
        next_action: Optional hint for UI (e.g., 'reload', 'show_form', 'notify_user').
        data: Optional arbitrary data for advanced clients (hypotheses selected, form fields, etc.).
    """
    status: str  # 'ok', 'error', 'deferred'
    message: str
    next_action: Optional[str] = None
    data: Optional[Dict[str, Any]] = None


class Handler(ABC):
    """Abstract base class for decision handlers.
    
    Each handler is responsible for executing exactly one decision type.
    Handlers are synchronous but designed to support enqueuing async tasks.
    Handlers must be idempotent per job_id: running twice with the same inputs
    must produce the same result (or be safe to retry).
    """
    
    @abstractmethod
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute the handler logic.
        
        Args:
            job_id: The job being processed.
            decision_result: The Phase-5 decision dict with keys:
                - decision_label (str)
                - provider_used (str)
                - measurements (dict)
                - fallback_used (bool)
                - fallback_reason (str or None)
            semantic_graph: Phase-3 persisted semantic graph (read-only).
            hypotheses: List of persisted hypothesis dicts (read-only).
            job_metadata: Job context dict with id, status, user_text, created_at.
        
        Returns:
            HandlerResult indicating success, failure, or deferred action.
        
        Guarantees:
            - Must not mutate hypotheses, semantic_graph, or job.
            - Must not call other handlers directly.
            - Must be idempotent per job_id.
        """
        pass
    
    @property
    def name(self) -> str:
        """Unique name of this handler, matching decision label."""
        return self.__class__.__name__.lower()
