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
    
    # Enable/disable measurements
    MEASUREMENTS_ENABLED: bool = True
    
    # Temporal placeholder computation
    TEMPORAL_PLACEHOLDERS: bool = True
    
    # Normalization factor for confidence (raw_confidence / this = normalized [0,1])
    CONFIDENCE_NORM_FACTOR: float = 5.0
    
    # Dominance clarity threshold (confidence gap above this = clear dominant)
    DOMINANCE_GAP_THRESHOLD: float = 0.2
    
    # Path length thresholds for structure analysis
    MIN_PATH_LENGTH: int = 2  # Minimum nodes in a path
    MAX_PATH_LENGTH: int = 2  # Maximum nodes before warning
    
    @classmethod
    def load_from_env(cls) -> None:
        """Load configuration from environment variables."""
        cls.MEASUREMENTS_ENABLED = (
            os.getenv("INDIRECT_PATH_MEASUREMENTS_ENABLED", "true").lower() == "true"
        )
        cls.TEMPORAL_PLACEHOLDERS = (
            os.getenv("INDIRECT_PATH_TEMPORAL_PLACEHOLDERS", "true").lower() == "true"
        )
        cls.CONFIDENCE_NORM_FACTOR = float(
            os.getenv("DECISION_CONFIDENCE_NORM_FACTOR", "5.0")
        )
        cls.DOMINANCE_GAP_THRESHOLD = float(
            os.getenv("INDIRECT_PATH_DOMINANCE_GAP_THRESHOLD", "0.2")
        )
        cls.MIN_PATH_LENGTH = int(
            os.getenv("INDIRECT_PATH_MIN_LENGTH", "2")
        )
        cls.MAX_PATH_LENGTH = int(
            os.getenv("INDIRECT_PATH_MAX_LENGTH", "5")
        )
        
        logger.info(
            f"IndirectPathConfig loaded: "
            f"enabled={cls.MEASUREMENTS_ENABLED}, "
            f"temporal={cls.TEMPORAL_PLACEHOLDERS}, "
            f"norm_factor={cls.CONFIDENCE_NORM_FACTOR}"
        )
    
    @classmethod
    def to_dict(cls) -> Dict[str, Any]:
        """Export config as dict."""
        return {
            "MEASUREMENTS_ENABLED": cls.MEASUREMENTS_ENABLED,
            "TEMPORAL_PLACEHOLDERS": cls.TEMPORAL_PLACEHOLDERS,
            "CONFIDENCE_NORM_FACTOR": cls.CONFIDENCE_NORM_FACTOR,
            "DOMINANCE_GAP_THRESHOLD": cls.DOMINANCE_GAP_THRESHOLD,
            "MIN_PATH_LENGTH": cls.MIN_PATH_LENGTH,
            "MAX_PATH_LENGTH": cls.MAX_PATH_LENGTH,
        }


def get_indirect_path_config() -> IndirectPathConfig:
    """Get the global indirect path config class."""
    return IndirectPathConfig
