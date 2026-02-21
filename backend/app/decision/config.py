"""Configuration for Decision & Control phase thresholds.

All decision thresholds are now loaded from AdminPolicy (global admin layer).
This config class provides a bridge for backward compatibility if needed.
"""
import logging

logger = logging.getLogger(__name__)


class DecisionConfig:
    """Loads decision logic thresholds from AdminPolicy."""
    
    def __init__(self, job_config: dict = None):
        """
        Initialize config from AdminPolicy.
        
        Args:
            job_config: Deprecated, kept for backward compatibility.
        """
        from app.config.admin_policy import admin_policy
        
        # Read ALL thresholds from AdminPolicy
        dt = admin_policy.algorithm.decision_thresholds
        
        # ===== Confidence Normalization =====
        self.CONFIDENCE_NORMALIZATION_FACTOR = dt.confidence_norm_factor
        
        # ===== High Confidence Threshold =====
        self.HIGH_CONFIDENCE_THRESHOLD = dt.high_confidence_threshold
        
        # ===== Dominant Hypothesis Gap Ratio =====
        self.DOMINANT_GAP_RATIO = dt.dominant_gap_ratio
        
        # ===== Diversity Thresholds =====
        self.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD = dt.low_diversity_pairs_threshold
        self.DIVERSITY_RATIO_THRESHOLD = dt.diversity_ratio_threshold
        
        # ===== Graph Density Threshold =====
        self.SPARSE_GRAPH_DENSITY_THRESHOLD = dt.sparse_graph_density_threshold
        
        # ===== Path Support Threshold =====
        self.PATH_SUPPORT_THRESHOLD = dt.path_support_threshold
        
        # ===== Stability Cycle Threshold =====
        self.STABILITY_CYCLE_THRESHOLD = dt.stability_cycle_threshold
        self.MIN_ABSOLUTE_GROWTH_THRESHOLD = dt.min_absolute_growth_threshold
        self.MIN_RELATIVE_GROWTH_THRESHOLD = dt.min_relative_growth_threshold
        
        # ===== Filtering Thresholds =====
        self.PASSED_TO_TOTAL_RATIO_THRESHOLD = dt.passed_to_total_ratio_threshold
        
        # ===== Minimum Viable Hypotheses =====
        self.MINIMUM_HYPOTHESES_THRESHOLD = dt.minimum_hypotheses_threshold
        
        logger.debug(
            f"DecisionConfig loaded from AdminPolicy: "
            f"norm_factor={self.CONFIDENCE_NORMALIZATION_FACTOR}, "
            f"high_conf={self.HIGH_CONFIDENCE_THRESHOLD}"
        )


_config = None

def get_decision_config(job_config: dict = None) -> DecisionConfig:
    """
    Get or create the global DecisionConfig instance.
    """
    global _config
    if _config is None:
        _config = DecisionConfig(job_config)
    return _config
