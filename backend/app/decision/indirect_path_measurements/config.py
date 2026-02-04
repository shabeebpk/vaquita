"""
Configuration loader for indirect path measurements.
Loads from AdminPolicy (global admin layer).
"""

import logging
from typing import Dict, Any

logger = logging.getLogger(__name__)


class IndirectPathConfig:
    """Configuration for indirect path measurements."""
    
    def __init__(self, job_config: dict = None):
        """
        Initialize config from AdminPolicy and SystemSettings.
        
        Args:
            job_config: Deprecated, kept for backward compatibility.
        """
        from app.config.system_settings import system_settings
        from app.config.admin_policy import admin_policy
        
        # Feature toggles from SystemSettings (Invariants)
        self.MEASUREMENTS_ENABLED = system_settings.INDIRECT_PATH_MEASUREMENTS_ENABLED
        self.TEMPORAL_PLACEHOLDERS = system_settings.INDIRECT_PATH_TEMPORAL_PLACEHOLDERS
        
        # Parameters from AdminPolicy
        dt = admin_policy.algorithm.decision_thresholds
        ip = admin_policy.algorithm.indirect_path
        
        self.CONFIDENCE_NORM_FACTOR = float(dt.confidence_norm_factor)
        self.DOMINANCE_GAP_THRESHOLD = float(ip.dominance_gap_threshold)
        self.MIN_PATH_LENGTH = int(ip.min_length)
        self.MAX_PATH_LENGTH = int(ip.max_length)
        
        logger.debug(
            f"IndirectPathConfig loaded from AdminPolicy: "
            f"enabled={self.MEASUREMENTS_ENABLED}, "
            f"temporal={self.TEMPORAL_PLACEHOLDERS}, "
            f"norm_factor={self.CONFIDENCE_NORM_FACTOR}, "
            f"gap_threshold={self.DOMINANCE_GAP_THRESHOLD}"
        )
