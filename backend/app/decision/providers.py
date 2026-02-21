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
                 from app.storage.models import Job
                 job = session.query(Job).filter(Job.id == job_id).first()
                 if job and job.job_config:
                     from app.decision.config import DecisionConfig
                     config = DecisionConfig(job.job_config)
        
        if not config:
            from app.decision.config import DecisionConfig
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
        
        # Rule 2: Sliding Window Stagnancy Check (The "N Consecutive" Solution)
        # We check last N cycles. If ALL show minimal/zero growth, we halt.
        if job_id:
            with Session(engine) as session:
                from app.storage.models import DecisionResult
                from sqlalchemy import desc
                
                # Fetch last N-1 records (N = config.STABILITY_CYCLE_THRESHOLD)
                # We subtract 1 because the current cycle's growth is in 'growth_score'
                window_size = config.STABILITY_CYCLE_THRESHOLD
                n_minus_1 = max(0, window_size - 1)
                
                recent_results = session.query(DecisionResult).filter(
                    DecisionResult.job_id == job_id
                ).order_by(desc(DecisionResult.created_at)).limit(n_minus_1).all()
                
                growth_history = []
                for res in recent_results:
                    # Use a safety fallback for missing snapshots
                    snap = res.measurements_snapshot or {}
                    growth_history.append(float(snap.get("growth_score", 0.0)))
                
                # Check current growth vs threshold
                is_stagnant_now = abs(growth_score) <= config.MIN_ABSOLUTE_GROWTH_THRESHOLD
                
                # Full window check: only if we have enough history to evaluate N cycles
                if len(growth_history) == n_minus_1 and is_stagnant_now:
                    all_stagnant = all(abs(g) <= config.MIN_ABSOLUTE_GROWTH_THRESHOLD for g in growth_history)
                    if all_stagnant:
                        logger.info(
                            f"Saturation detected: last {window_size} cycles (inc. current) "
                            f"show absolute growth <= {config.MIN_ABSOLUTE_GROWTH_THRESHOLD}. "
                            f"History: {growth_history} + current: {growth_score:.3f}. "
                            f"Returning HALT_NO_HYPOTHESIS"
                        )
                        return Decision.HALT_NO_HYPOTHESIS

        # Rule 3: Strategic Targeted Download (Prioritize growth over other halts)
        if growth_score > config.MIN_ABSOLUTE_GROWTH_THRESHOLD:
            logger.info(
                f"Absolute growth detected (score={growth_score:.3f} > {config.MIN_ABSOLUTE_GROWTH_THRESHOLD}): "
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
        
        # Rule 4: HALT_CONFIDENT (strict conditions)
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
        
        # Rule 5: HALT_NO_HYPOTHESIS (Single cycle stability check - backup to Rule 2)
        # When: no direct edge, weak path support, stable evidence_growth
        # Use graph density and diversity as stability indicators
        is_stable = graph_density > config.SPARSE_GRAPH_DENSITY_THRESHOLD and \
                    diversity_score > config.DIVERSITY_RATIO_THRESHOLD
        
        # Rule 5 check: evidence_growth_rate is a relative ratio (%)
        growth_is_minimal = abs(evidence_growth_rate) <= config.MIN_RELATIVE_GROWTH_THRESHOLD
        
        if has_indirect_paths and \
           growth_is_minimal and \
           max_paths_per_pair < config.PATH_SUPPORT_THRESHOLD and \
           is_stable:
            logger.info(
                f"Halt no hypothesis (Stability-based): indirect paths={has_indirect_paths}, "
                f"relative growth_rate={evidence_growth_rate:.2f} <= {config.MIN_RELATIVE_GROWTH_THRESHOLD}, "
                f"paths_per_pair={max_paths_per_pair} < {config.PATH_SUPPORT_THRESHOLD}, "
                f"stable=(density={graph_density:.4f}, diversity={diversity_score:.2f}): "
                f"returning HALT_NO_HYPOTHESIS"
            )
            return Decision.HALT_NO_HYPOTHESIS
        
        # Non-terminal decisions (proceed with normal flow)
        # Rule 6: Low diversity or sparse graph -> need more data
        unique_pairs = measurements.get("unique_source_target_pairs", 0)
        if unique_pairs < config.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD or \
           diversity_score < config.DIVERSITY_RATIO_THRESHOLD or \
           graph_density < config.SPARSE_GRAPH_DENSITY_THRESHOLD:
            logger.info(
                f"Low diversity/sparse graph (unique_pairs={unique_pairs}, diversity_score={diversity_score:.2f}, "
                f"density={graph_density:.4f}): returning FETCH_MORE_LITERATURE"
            )
            return Decision.FETCH_MORE_LITERATURE
        
        # Rule 7: Fallback -> insufficient signal (The "Ambiguous" Solution)
        # If we reached here, it means we don't have enough confidence to halt or download,
        # but the graph isn't necessarily "sparse" or "zero growth". 
        # We treat this as needing more signal.
        logger.info("Ambiguous case: no decisive rules met. Returning INSUFFICIENT_SIGNAL")
        return Decision.INSUFFICIENT_SIGNAL


class LLMDecisionProvider(DecisionProvider):
    """
    Optional LLM-based decision provider with constrained output.
    
    Only called as a fallback (e.g., when rule-based returns insufficient signal).
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
                logger.warning("LLM returned empty response; falling back to INSUFFICIENT_SIGNAL")
                return Decision.INSUFFICIENT_SIGNAL
            
            # Try to parse the LLM response
            for decision_label in all_decisions():
                if decision_label in decision_text:
                    logger.info(f"LLM decided: {decision_label}")
                    from app.decision.space import decision_from_string
                    return decision_from_string(decision_label)
            
            logger.warning(f"LLM returned unparsable response: {decision_text}; falling back to INSUFFICIENT_SIGNAL")
            return Decision.INSUFFICIENT_SIGNAL
        
        except Exception as e:
            logger.error(f"LLM decision failed: {e}; falling back to INSUFFICIENT_SIGNAL")
            return Decision.INSUFFICIENT_SIGNAL
