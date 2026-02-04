"""Configuration for Decision & Control phase thresholds.

All hardcoded numeric thresholds are centralized here and can be overridden
via environment variables for tuning without code changes.
"""
import os
import logging

logger = logging.getLogger(__name__)


class DecisionConfig:
    """Loads and stores all decision logic thresholds from environment variables."""
    
    def __init__(self, job_config: dict = None):
        """Initialize config from job configuration."""
        job_config = job_config or {}
        algo_params = job_config.get("algorithm_params", {})
        heuristics = algo_params.get("heuristics", {})
        
        # ===== Confidence Normalization =====
        self.CONFIDENCE_NORMALIZATION_FACTOR = int(heuristics.get("decision_confidence_norm_factor", 10))
        
        # ===== High Confidence Threshold =====
        self.HIGH_CONFIDENCE_THRESHOLD = float(heuristics.get("decision_high_confidence_threshold", 0.7))
        
        # ===== Dominant Hypothesis Gap Ratio =====
        self.DOMINANT_GAP_RATIO = float(heuristics.get("decision_dominant_gap_ratio", 0.3))
        
        # ===== Diversity Thresholds =====
        self.LOW_DIVERSITY_UNIQUE_PAIRS_THRESHOLD = int(heuristics.get("decision_low_diversity_pairs_threshold", 2))
        self.DIVERSITY_RATIO_THRESHOLD = float(heuristics.get("decision_diversity_ratio_threshold", 0.3))
        
        # ===== Graph Density Threshold =====
        self.SPARSE_GRAPH_DENSITY_THRESHOLD = float(heuristics.get("decision_sparse_graph_density_threshold", 0.05))
        
        # ===== Path Support Threshold =====
        self.PATH_SUPPORT_THRESHOLD = int(heuristics.get("decision_path_support_threshold", 2))
        
        # ===== Stability Cycle Threshold =====
        self.STABILITY_CYCLE_THRESHOLD = int(heuristics.get("decision_stability_cycle_threshold", 3))
        
        # ===== Filtering Thresholds =====
        self.PASSED_TO_TOTAL_RATIO_THRESHOLD = float(heuristics.get("decision_passed_to_total_ratio_threshold", 0.2))
        
        # ===== Minimum Viable Hypotheses =====
        self.MINIMUM_HYPOTHESES_THRESHOLD = int(heuristics.get("decision_minimum_hypotheses_threshold", 1))
        
        logger.debug(
            f"DecisionConfig initialized: "
            f"norm_factor={self.CONFIDENCE_NORMALIZATION_FACTOR}, "
            f"high_conf={self.HIGH_CONFIDENCE_THRESHOLD}"
        )
