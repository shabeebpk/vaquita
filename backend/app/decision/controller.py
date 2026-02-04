"""Decision controller and orchestrator.

Wires measurements, decision providers, and persistence together.
This is the main entry point for Phase-5 orchestration.
"""
import logging
from typing import Dict, Any, Optional
from datetime import datetime

from sqlalchemy.orm import Session

from app.storage.db import engine
from app.storage.models import DecisionResult
from app.decision.space import Decision
from app.decision.measurements import compute_measurements
from app.decision.providers import DecisionProvider, RuleBasedDecisionProvider, LLMDecisionProvider

logger = logging.getLogger(__name__)


class DecisionController:
    """Orchestrates the decision-making process.
    
    Responsibilities:
    - Load measurements from artifacts
    - Select and invoke decision provider(s)
    - Handle fallback (rule-based → LLM if needed)
    - Persist decision result
    - Return decision to runner
    """
    
    def __init__(self, provider_name: str = "rule_based", llm_client=None):
        """
        Initialize the controller with a chosen provider strategy.
        
        Args:
            provider_name: "rule_based", "hybrid", or "llm"
                - "rule_based": only use RuleBasedDecisionProvider
                - "hybrid": use RuleBasedDecisionProvider, fall back to LLM if UNDECIDED
                - "llm": use LLMDecisionProvider only
            llm_client: Optional LLM client for LLMDecisionProvider.
        """
        self.provider_name = provider_name
        self.llm_client = llm_client
        
        if provider_name == "rule_based":
            self.primary_provider = RuleBasedDecisionProvider()
            self.fallback_provider = None
        elif provider_name == "hybrid":
            self.primary_provider = RuleBasedDecisionProvider()
            self.fallback_provider = LLMDecisionProvider(llm_client=llm_client)
        elif provider_name == "llm":
            self.primary_provider = LLMDecisionProvider(llm_client=llm_client)
            self.fallback_provider = RuleBasedDecisionProvider()  # fallback to rule-based
        else:
            raise ValueError(f"Unknown provider: {provider_name}")
        
        logger.info(f"DecisionController initialized with provider={provider_name}")
    
    def decide(
        self,
        job_id: int,
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
        previous_decision_result: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Make a decision for a job based on its artifacts.
        
        Args:
            job_id: The job ID.
            semantic_graph: Phase-3 semantic graph dict.
            hypotheses: List of persisted hypothesis dicts.
            job_metadata: Job context (id, status, user_text, etc.).
            previous_decision_result: Optional previous DecisionResult dict for temporal measurements.
        
        Returns:
            A decision result dict with keys:
            - decision_label (str)
            - provider_used (str)
            - measurements (dict)
            - fallback_used (bool)
            - fallback_reason (str or None)
        """
        # Compute measurements (optionally with indirect path metrics)
        # Pass previous snapshot for temporal placeholders (growth_rate, stability, etc.)
        previous_snapshot = (
            previous_decision_result.get("measurements_snapshot")
            if previous_decision_result else None
        )
        measurements = compute_measurements(
            semantic_graph,
            hypotheses,
            job_metadata,
            previous_measurement_snapshot=previous_snapshot,
        )
        logger.info(f"Computed measurements for job {job_id}: {len(measurements)} signals")
        
        # Invoke primary provider
        context = {
            "job_id": job_id,
            "semantic_graph": semantic_graph,
            "hypotheses": hypotheses,
        }
        
        decision = self.primary_provider.decide(measurements, context)
        logger.info(f"Primary provider ({self.provider_name}) decided: {decision.value}")
        
        fallback_used = False
        fallback_reason = None
        provider_used = self.provider_name
        
        # If primary returns UNDECIDED and we have a fallback, invoke it
        if decision == Decision.UNDECIDED and self.fallback_provider:
            logger.info("Primary returned UNDECIDED; invoking fallback provider")
            try:
                fallback_decision = self.fallback_provider.decide(measurements, context)
                decision = fallback_decision
                fallback_used = True
                fallback_reason = "Primary returned UNDECIDED"
                provider_used = "fallback"
            except Exception as e:
                logger.error(f"Fallback provider failed: {e}; using UNDECIDED")
                fallback_reason = str(e)
        
        result = {
            "decision_label": decision.value,
            "provider_used": provider_used,
            "measurements": measurements,
            "fallback_used": fallback_used,
            "fallback_reason": fallback_reason,
        }
        
        logger.info(f"\n\n\n\n measurements: {measurements}\n\n\n\n")
        # Persist decision result
        self._persist_decision(job_id, result)
        
        return result
    
    def _persist_decision(self, job_id: int, result: Dict[str, Any]) -> int:
        """Persist the decision result to the database.
        
        Returns:
            The id of the persisted DecisionResult.
        """
        with Session(engine) as session:
            record = DecisionResult(
                job_id=job_id,
                decision_label=result.get("decision_label"),
                provider_used=result.get("provider_used"),
                measurements_snapshot=result.get("measurements", {}),
                fallback_used=bool(result.get("fallback_used", False)),
                fallback_reason=result.get("fallback_reason"),
                created_at=datetime.utcnow(),
            )
            session.add(record)
            session.commit()
            session.refresh(record)
            
            logger.info(f"Persisted decision result {record.id} for job {job_id}: {result['decision_label']}")
            return record.id


def get_decision_controller(provider_override: Optional[str] = None) -> DecisionController:
    """Factory function to instantiate a DecisionController.
    
    Uses DECISION_PROVIDER env var, or override parameter, or defaults to "rule_based".
    
    Args:
        provider_override: If provided, overrides environment variable.
    
    Returns:
        An initialized DecisionController.
    """
    from app.config.system_settings import system_settings
    
    provider = provider_override or system_settings.DECISION_PROVIDER
    return DecisionController(provider_name=provider)
