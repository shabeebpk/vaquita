"""Configuration for Decision & Control phase thresholds.

All hardcoded numeric thresholds are centralized here and can be overridden
via environment variables for tuning without code changes.
"""
import os
import logging

logger = logging.getLogger(__name__)


class DecisionConfig:
    """Loads and stores all decision logic thresholds from environment variables."""
    
    def __init__(self):
        """Initialize config from environment variables with safe defaults."""
        
        # ===== Confidence Normalization =====
        # Confidence values from Phase-4 are raw support counts (integers).
        # Normalize them to [0, 1] range for decision logic.
        # normalized_confidence = min(confidence / CONFIDENCE_NORMALIZATION_FACTOR, 1.0)
        self.CONFIDENCE_NORMALIZATION_FACTOR = self._get_int_env(
            "DECISION_CONFIDENCE_NORM_FACTOR",
            default=10,
            description="Divide raw confidence by this to get normalized [0,1]"
        )
        
        # ===== High Confidence Threshold =====
        # For HALT_CONFIDENT decision: max_normalized_confidence >= this value
        self.HIGH_CONFIDENCE_THRESHOLD = self._get_float_env(
            "DECISION_HIGH_CONFIDENCE_THRESHOLD",
            default=0.7,
            description="Normalized confidence >= this triggers HALT_CONFIDENT (0-1 range)"
        )
        
        # ===== Dominant Hypothesis Gap Ratio =====
        # For is_dominant_clear: gap between 1st and 2nd > (gap_ratio * first_confidence)
        # Example: if first=8, second=6, gap=2, and gap_ratio=0.3, then 2 > 0.3*8=2.4? No.
        # This is measured on normalized confidence.
        self.DOMINANT_GAP_RATIO = self._get_float_env(
            "DECISION_DOMINANT_GAP_RATIO",
            default=0.3,
            description="1st-2nd confidence gap > (gap_ratio * max_conf) means dominant"
        )
        
        # ===== Diversity Thresholds =====
        # Low diversity (all similar source-target) triggers ASK_DOMAIN_EXPERT
        self.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD = self._get_int_env(
            "DECISION_LOW_DIVERSITY_PAIRS_THRESHOLD",
            default=2,
            description="If unique_source_target_pairs < this, diversity is low"
        )
        
        self.DIVERSITY_RATIO_THRESHOLD = self._get_float_env(
            "DECISION_DIVERSITY_RATIO_THRESHOLD",
            default=0.3,
            description="If diversity_score < this, diversity is low"
        )
        
        # ===== Graph Density Threshold =====
        # Sparse graph (low density) triggers FETCH_MORE_LITERATURE
        self.SPARSE_GRAPH_DENSITY_THRESHOLD = self._get_float_env(
            "DECISION_SPARSE_GRAPH_DENSITY_THRESHOLD",
            default=0.05,
            description="If graph_density < this, graph is sparse"
        )
        
        # ===== Filtering Thresholds =====
        # If very few hypotheses passed filter vs total, may indicate problem
        self.PASSED_TO_TOTAL_RATIO_THRESHOLD = self._get_float_env(
            "DECISION_PASSED_TO_TOTAL_RATIO_THRESHOLD",
            default=0.2,
            description="If passed/total ratio < this, warning signal"
        )
        
        # ===== Minimum Viable Hypotheses =====
        # If total hypothesis count < this, insufficient_signal
        self.MINIMUM_HYPOTHESES_THRESHOLD = self._get_int_env(
            "DECISION_MINIMUM_HYPOTHESES_THRESHOLD",
            default=1,
            description="If total_hypothesis_count < this, INSUFFICIENT_SIGNAL"
        )
        
        logger.info(
            f"DecisionConfig initialized: "
            f"norm_factor={self.CONFIDENCE_NORMALIZATION_FACTOR}, "
            f"high_conf={self.HIGH_CONFIDENCE_THRESHOLD}, "
            f"gap_ratio={self.DOMINANT_GAP_RATIO}, "
            f"low_diversity_pairs={self.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD}, "
            f"sparse_density={self.SPARSE_GRAPH_DENSITY_THRESHOLD}"
        )
    
    @staticmethod
    def _get_float_env(key: str, default: float, description: str = "") -> float:
        """Load a float from environment, log it, return default if not set or invalid."""
        val = os.getenv(key)
        if val is None:
            logger.debug(f"{key}={default} (default) — {description}")
            return default
        try:
            f = float(val)
            logger.info(f"{key}={f} (env) — {description}")
            return f
        except ValueError:
            logger.warning(f"{key}={val} is not a valid float; using default={default}")
            return default
    
    @staticmethod
    def _get_int_env(key: str, default: int, description: str = "") -> int:
        """Load an int from environment, log it, return default if not set or invalid."""
        val = os.getenv(key)
        if val is None:
            logger.debug(f"{key}={default} (default) — {description}")
            return default
        try:
            i = int(val)
            logger.info(f"{key}={i} (env) — {description}")
            return i
        except ValueError:
            logger.warning(f"{key}={val} is not a valid int; using default={default}")
            return default


# Global singleton instance
_config = None


def get_decision_config() -> DecisionConfig:
    """Get or create the global DecisionConfig instance."""
    global _config
    if _config is None:
        _config = DecisionConfig()
    return _config
