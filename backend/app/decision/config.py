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
        from app.config.system_settings import system_settings
        
        # ===== Confidence Normalization =====
        self.CONFIDENCE_NORMALIZATION_FACTOR = system_settings.DECISION_CONFIDENCE_NORM_FACTOR
        
        # ===== High Confidence Threshold =====
        self.HIGH_CONFIDENCE_THRESHOLD = system_settings.DECISION_HIGH_CONFIDENCE_THRESHOLD
        
        # ===== Dominant Hypothesis Gap Ratio =====
        self.DOMINANT_GAP_RATIO = system_settings.DECISION_DOMINANT_GAP_RATIO
        
        # ===== Diversity Thresholds =====
        self.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD = system_settings.DECISION_LOW_DIVERSITY_PAIRS_THRESHOLD
        
        self.DIVERSITY_RATIO_THRESHOLD = system_settings.DECISION_DIVERSITY_RATIO_THRESHOLD
        
        # ===== Graph Density Threshold =====
        self.SPARSE_GRAPH_DENSITY_THRESHOLD = system_settings.DECISION_SPARSE_GRAPH_DENSITY_THRESHOLD
        
        # ===== Path Support Threshold =====
        self.PATH_SUPPORT_THRESHOLD = system_settings.DECISION_PATH_SUPPORT_THRESHOLD
        
        # ===== Stability Cycle Threshold =====
        self.STABILITY_CYCLE_THRESHOLD = system_settings.DECISION_STABILITY_CYCLE_THRESHOLD
        
        # ===== Filtering Thresholds =====
        self.PASSED_TO_TOTAL_RATIO_THRESHOLD = system_settings.DECISION_PASSED_TO_TOTAL_RATIO_THRESHOLD
        
        # ===== Minimum Viable Hypotheses =====
        self.MINIMUM_HYPOTHESES_THRESHOLD = system_settings.DECISION_MINIMUM_HYPOTHESES_THRESHOLD
        
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
