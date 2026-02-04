"""
Configuration loader for indirect path measurements.
Loads from environment variables with defaults.
"""

import os
import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class IndirectPathConfig:
    """Configuration for indirect path measurements."""
    
    def __init__(self, job_config: dict = None):
        """Initialize config from job configuration."""
        from app.config.system_settings import system_settings
        job_config = job_config or {}
        algo_params = job_config.get("algorithm_params", {})
        heuristics = algo_params.get("heuristics", {})
        
        # Feature toggles from SystemSettings (Invariants)
        self.MEASUREMENTS_ENABLED = system_settings.INDIRECT_PATH_MEASUREMENTS_ENABLED
        self.TEMPORAL_PLACEHOLDERS = system_settings.INDIRECT_PATH_TEMPORAL_PLACEHOLDERS
        
        # Parameters from JobConfig (Heuristics)
        self.CONFIDENCE_NORM_FACTOR = float(heuristics.get("decision_confidence_norm_factor", 10.0))
        self.DOMINANCE_GAP_THRESHOLD = float(heuristics.get("indirect_path_dominance_gap_threshold", 0.2))
        self.MIN_PATH_LENGTH = int(heuristics.get("indirect_path_min_length", 3))
        self.MAX_PATH_LENGTH = int(heuristics.get("indirect_path_max_length", 4))
        
        logger.debug(
            f"IndirectPathConfig loaded: "
            f"enabled={self.MEASUREMENTS_ENABLED}, "
            f"temporal={self.TEMPORAL_PLACEHOLDERS}, "
            f"norm_factor={self.CONFIDENCE_NORM_FACTOR}, "
            f"gap_threshold={self.DOMINANCE_GAP_THRESHOLD}"
        )
