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
from app.decision.config import get_decision_config
from app.llm import get_llm_service
from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

# Fallback prompt for LLM decision-making
DECISION_LLM_FALLBACK = "Decide: {decision_labels}"



class DecisionProvider(ABC):
    """Abstract base class for decision providers."""
    
    @abstractmethod
    def decide(self, measurements: Dict[str, Any], context: Dict[str, Any]) -> Decision:
        """
        Make a decision based on measurements and context.
        
        Args:
            measurements: Dictionary of signals computed by measurements layer.
            context: Additional context (job_id, semantic_graph, hypotheses, etc.).
        
        Returns:
            A Decision enum value.
        """
        pass


class RuleBasedDecisionProvider(DecisionProvider):
    """
    Deterministic rule-based decision provider.
    
    Uses explicit if-else rules over measurements to select a decision.
    This is the default, always-available, LLM-free provider.
    All thresholds are loaded from DecisionConfig (environment-configurable).
    """
    
    def __init__(self):
        """Initialize provider and load config thresholds."""
        self.config = get_decision_config()
        logger.info("RuleBasedDecisionProvider initialized with DecisionConfig")
    
    def decide(self, measurements: Dict[str, Any], context: Dict[str, Any]) -> Decision:
        """
        Apply deterministic rules to select a decision.
        
        Rules (in order):
        1. If total hypotheses < MIN_HYPOTHESES_THRESHOLD → INSUFFICIENT_SIGNAL
        2. If no passed hypotheses → INSUFFICIENT_SIGNAL
        3. If dominant hypothesis is clear and max_norm_conf >= HIGH_CONFIDENCE_THRESHOLD → HALT_CONFIDENT
        4. If unique_pairs < LOW_DIVERSITY_PAIRS_THRESHOLD → ASK_DOMAIN_EXPERT
        6. If graph_density < SPARSE_DENSITY_THRESHOLD → FETCH_MORE_LITERATURE
        7. Default → ASK_USER_INPUT
        """
        total_count = measurements.get("total_hypothesis_count", 0)
        passed_count = measurements.get("passed_hypothesis_count", 0)
        
        # Rule 1: Minimum threshold not met
        if total_count < self.config.MINIMUM_HYPOTHESES_THRESHOLD:
            logger.info(
                f"Total hypotheses {total_count} < threshold {self.config.MINIMUM_HYPOTHESES_THRESHOLD}: "
                f"returning INSUFFICIENT_SIGNAL"
            )
            return Decision.INSUFFICIENT_SIGNAL
        
        # Rule 2: No passed hypotheses
        if passed_count == 0:
            logger.info("No passed hypotheses: returning INSUFFICIENT_SIGNAL")
            return Decision.INSUFFICIENT_SIGNAL
        
        max_norm_conf = measurements.get("max_normalized_confidence", 0.0)
        mean_norm_conf = measurements.get("mean_normalized_confidence", 0.0)
        is_dominant = measurements.get("is_dominant_clear", False)
        unique_pairs = measurements.get("unique_source_target_pairs", 0)
        graph_density = measurements.get("graph_density", 0.0)
        diversity_score = measurements.get("diversity_score", 0.0)
        
        # Rule 3: High confidence + clear dominant
        if is_dominant and max_norm_conf >= self.config.HIGH_CONFIDENCE_THRESHOLD:
            logger.info(
                f"Dominant hypothesis with high normalized confidence "
                f"(max={max_norm_conf:.2f} >= {self.config.HIGH_CONFIDENCE_THRESHOLD}): "
                f"returning HALT_CONFIDENT"
            )
            return Decision.HALT_CONFIDENT
        
        # Rule 4: Low diversity (check both unique pairs and diversity ratio)
        if unique_pairs < self.config.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD or \
           diversity_score < self.config.DIVERSITY_RATIO_THRESHOLD:
            logger.info(
                f"Low diversity (unique_pairs={unique_pairs}, diversity_score={diversity_score:.2f}): "
                f"returning ASK_DOMAIN_EXPERT"
            )
            return Decision.ASK_DOMAIN_EXPERT
        
        # Rule 5: Sparse hypothesis graph
        if graph_density < self.config.SPARSE_GRAPH_DENSITY_THRESHOLD:
            logger.info(
                f"Sparse graph (density={graph_density:.4f} < {self.config.SPARSE_GRAPH_DENSITY_THRESHOLD}): "
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
        self.prompt_template = load_prompt(
            "decision_llm.txt",
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
                job_user_text=context.get('job_user_text', '')
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
