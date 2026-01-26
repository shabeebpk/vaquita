"""Base Classifier Handler Contract.

Every classification label has a corresponding handler that prepares the project state.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from dataclasses import dataclass


@dataclass
class ClassifierHandlerResult:
    """Result of a classifier handler execution."""
    status: str  # 'ok', 'error'
    message: str
    action_taken: str  # e.g., 'initialized_seed', 'queued_ingestion'
    next_step: Optional[str] = None
    job_state_updates: Optional[Dict[str, Any]] = None


class ClassifierHandler(ABC):
    """Abstract base class for classifier handlers."""

    @abstractmethod
    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any  # SQLAlchemy Session
    ) -> ClassifierHandlerResult:
        """Execute the state preparation logic based on payload.
        
        Args:
            job_id: The job being updated.
            payload: Extract JSON from LLM classifier.
            session: DB session for persistence.
        """
        pass

    @property
    def label(self) -> str:
        """The classification label this handler supports."""
        return self.__class__.__name__.replace("Handler", "").upper()
