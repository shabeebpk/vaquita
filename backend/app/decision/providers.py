"""Decision provider interface and implementations.

DecisionProvider: abstract interface for decision-making logic.
RuleBasedDecisionProvider: deterministic, rule-based implementation (default).
LLMDecisionProvider: optional LLM-based implementation with constrained output.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any
import logging
import os

from app.decision.space import Decision, all_decisions
from app.decision.config import DecisionConfig
from app.llm import get_llm_service
from app.prompts.loader import load_prompt
from app.storage.db import engine
from app.storage.models import Job
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# Fallback prompt for LLM decision-making
DECISION_LLM_FALLBACK = "Decide: {decision_labels}"



class DecisionProvider(ABC):
    """Abstract base class for decision providers."""
    
    @abstractmethod
    def decide(self, measurements: Dict[str, Any], context: Dict[str, Any]) -> Decision:
        pass


class RuleBasedDecisionProvider(DecisionProvider):
    """
    Deterministic rule-based decision provider.
    
    Uses explicit if-else rules over measurements to select a decision.
    All thresholds are loaded from DecisionConfig (per-job).
    """
    
    def __init__(self):
        """Initialize provider."""
        logger.info("RuleBasedDecisionProvider initialized")
    
    def decide(self, measurements: Dict[str, Any], context: Dict[str, Any]) -> Decision:
        """
        Apply deterministic rules to select a decision.
        """
        # Load config from job context
        job_id = context.get("job_id")
        config = None
        
        if job_id:
             with Session(engine) as session:
                 job = session.query(Job).filter(Job.id == job_id).first()
                 if job and job.job_config:
                     config = DecisionConfig(job.job_config)
        
        if not config:
            config = DecisionConfig()  # defaults
            
        passed_count = measurements.get("passed_hypothesis_count", 0)
        promising_count = measurements.get("promising_hypothesis_count", 0)
        
        # Rule 1: Insufficient Signal
        # Total count check (minimum threshold) is now secondary to "at least one" signal.
        # User: "no longer uses number of passed hypothesis as constraint for to be insufficiant_signal"
        # "that is insufficiant only even if no hypothesis that filterout by just confidence is less"
        is_insufficient = (passed_count == 0 and promising_count == 0)
        
        if is_insufficient:
            logger.info(
                f"Insufficient signal: passed={passed_count}, promising={promising_count}. "
                f"Returning INSUFFICIENT_SIGNAL"
            )
            return Decision.INSUFFICIENT_SIGNAL
        
        # Extract measurements for decision logic
        growth_score = measurements.get("growth_score", 0.0)
        
        # Rule 2: Strategic Targeted Download (Prioritize growth over halts)
        if growth_score > 0:
            logger.info(
                f"Growth detected (score={growth_score:.3f}): "
                f"returning STRATEGIC_DOWNLOAD_TARGETED"
            )
            return Decision.STRATEGIC_DOWNLOAD_TARGETED
        
        max_norm_conf = measurements.get("max_normalized_confidence", 0.0)
        is_dominant = measurements.get("is_dominant_clear", False)
        max_paths_per_pair = measurements.get("max_paths_per_pair", 0)
        mean_path_length = measurements.get("mean_path_length", 1.0)
        graph_density = measurements.get("graph_density", 0.0)
        diversity_score = measurements.get("diversity_score", 0.0)
        evidence_growth_rate = measurements.get("evidence_growth_rate", 0.0)
        
        # Determine if we have indirect paths (no direct edge): path length > 1
        has_indirect_paths = mean_path_length > 1.0
        
        # Rule 3: HALT_CONFIDENT (strict conditions)
        # Only when: no direct edge, sufficient path support, clear dominance, high confidence
        if has_indirect_paths and \
           max_paths_per_pair >= config.PATH_SUPPORT_THRESHOLD and \
           is_dominant and \
           max_norm_conf >= config.HIGH_CONFIDENCE_THRESHOLD:
            logger.info(
                f"Halt confident: indirect paths={has_indirect_paths}, "
                f"paths_per_pair={max_paths_per_pair} >= {config.PATH_SUPPORT_THRESHOLD}, "
                f"dominant={is_dominant}, confidence={max_norm_conf:.2f} >= {config.HIGH_CONFIDENCE_THRESHOLD}: "
                f"returning HALT_CONFIDENT"
            )
            return Decision.HALT_CONFIDENT
        
        # Rule 4: HALT_NO_HYPOTHESIS (evidence of stability, not growth)
        # When: no direct edge, weak path support, stable evidence_growth
        # Use graph density and diversity as stability indicators
        is_stable = graph_density > 0.0 and diversity_score > 0.0
        growth_is_minimal = evidence_growth_rate <= 0.0 or abs(evidence_growth_rate) < 0.1
        
        if has_indirect_paths and \
           growth_is_minimal and \
           max_paths_per_pair < config.PATH_SUPPORT_THRESHOLD and \
           is_stable:
            logger.info(
                f"Halt no hypothesis: indirect paths={has_indirect_paths}, "
                f"growth_rate={evidence_growth_rate:.2f} ≈ 0, "
                f"paths_per_pair={max_paths_per_pair} < {config.PATH_SUPPORT_THRESHOLD}, "
                f"stable=(density={graph_density:.4f}, diversity={diversity_score:.2f}): "
                f"returning HALT_NO_HYPOTHESIS"
            )
            return Decision.HALT_NO_HYPOTHESIS
        
        # Non-terminal decisions (proceed with normal flow)
        # Rule 5: Low diversity (check both unique pairs and diversity ratio)
        unique_pairs = measurements.get("unique_source_target_pairs", 0)
        if unique_pairs < config.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD or \
           diversity_score < config.DIVERSITY_RATIO_THRESHOLD:
            logger.info(
                f"Low diversity (unique_pairs={unique_pairs}, diversity_score={diversity_score:.2f}): "
                f"returning ASK_DOMAIN_EXPERT"
            )
            return Decision.ASK_DOMAIN_EXPERT
        
        # Rule 6: Sparse hypothesis graph
        if graph_density < config.SPARSE_GRAPH_DENSITY_THRESHOLD:
            logger.info(
                f"Sparse graph (density={graph_density:.4f} < {config.SPARSE_GRAPH_DENSITY_THRESHOLD}): "
                f"returning FETCH_MORE_LITERATURE"
            )
            return Decision.FETCH_MORE_LITERATURE
        
        # Rule 7: Ambiguous case
        logger.info("Ambiguous case: returning ASK_USER_INPUT")
        return Decision.ASK_USER_INPUT


class LLMDecisionProvider(DecisionProvider):
    """
    Optional LLM-based decision provider with constrained output.
    
    Only called as a fallback (e.g., when rule-based returns UNDECIDED).
    The LLM is asked to choose from a predefined decision space only.
    Uses the global LLM service (app.llm.service) for all invocations.
    Loads prompts via the centralized prompt loader (app.prompts.loader).
    If the LLM returns invalid output, falls back to a safe default.
    """
    
    def __init__(self, llm_client=None):
        """
        Initialize LLM provider.
        
        Args:
            llm_client: Deprecated. Ignored. Uses global LLM service instead.
        """
        # Import here to avoid circular dependencies at module load
        from app.llm import get_llm_service
        self.llm_service = get_llm_service()
        # Load prompt template using centralized loader
        from app.config.admin_policy import admin_policy
        self.prompt_template = load_prompt(
            admin_policy.prompt_assets.decision_llm,
            fallback=DECISION_LLM_FALLBACK
        )
        logger.info("LLMDecisionProvider initialized (using global LLM service)")
    
    def decide(self, measurements: Dict[str, Any], context: Dict[str, Any]) -> Decision:
        """
        Ask LLM to decide, constrained to valid decision space.
        
        If LLM is unavailable or returns invalid output, return safe default.
        """
        # Build prompt with measurements and decision space
        decision_labels = ", ".join(all_decisions())
        
        # Format the loaded template with actual values
        try:
            prompt = self.prompt_template.format(
                decision_labels=decision_labels,
                total_hypothesis_count=measurements.get('total_hypothesis_count', 0),
                passed_hypothesis_count=measurements.get('passed_hypothesis_count', 0),
                rejected_hypothesis_count=measurements.get('rejected_hypothesis_count', 0),
                max_normalized_confidence=measurements.get('max_normalized_confidence', 0.0),
                mean_normalized_confidence=measurements.get('mean_normalized_confidence', 0.0),
                diversity_score=measurements.get('diversity_score', 0.0),
                graph_density=measurements.get('graph_density', 0.0),
                is_dominant_clear=measurements.get('is_dominant_clear', False),
                unique_source_target_pairs=measurements.get('unique_source_target_pairs', 0),
                max_paths_per_pair=measurements.get('max_paths_per_pair', 0),
                evidence_growth_rate=measurements.get('evidence_growth_rate', 0.0),
                mean_path_length=measurements.get('mean_path_length', 1.0)
            )
        except Exception as e:
            logger.error(f"Failed to format decision prompt: {e}")
            prompt = decision_labels  # Minimal fallback
        
        try:
            # Call the global LLM service
            decision_text = self.llm_service.generate(prompt).strip().lower()
            
            if not decision_text:
                logger.warning("LLM returned empty response; falling back to ASK_USER_INPUT")
                return Decision.ASK_USER_INPUT
            
            # Try to parse the LLM response
            for decision_label in all_decisions():
                if decision_label in decision_text:
                    logger.info(f"LLM decided: {decision_label}")
                    from app.decision.space import decision_from_string
                    return decision_from_string(decision_label)
            
            logger.warning(f"LLM returned unparsable response: {decision_text}; falling back")
            return Decision.ASK_USER_INPUT
        
        except Exception as e:
            logger.error(f"LLM decision failed: {e}; falling back to ASK_USER_INPUT")
            return Decision.ASK_USER_INPUT
