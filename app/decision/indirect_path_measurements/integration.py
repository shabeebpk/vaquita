"""
Integration point for indirect path measurements into decision layer.

Provides helper to extend existing measurement dicts with indirect-path metrics.
Does NOT modify decision logic—purely additive.
"""

import logging
from typing import Dict, List, Any, Optional

from app.decision.indirect_path_measurements.indirect_paths import IndirectPathMeasurements
from app.decision.indirect_path_measurements.config import get_indirect_path_config

logger = logging.getLogger(__name__)


def extend_measurements_with_indirect_paths(
    base_measurements: Dict[str, Any],
    hypotheses: List[Dict[str, Any]],
    previous_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Extend base measurements dict with indirect path measurements.
    
    Args:
        base_measurements: Existing measurements dict (will be updated in-place)
        hypotheses: List of hypothesis dicts (one per indirect path)
        previous_snapshot: Optional previous DecisionResult.measurements_snapshot for temporal comparisons
    
    Returns:
        Updated measurements dict (same object as base_measurements)
    
    Example:
        # In decision.py or wherever measurements are computed:
        measurements = {... existing measurements ...}
        measurements = extend_measurements_with_indirect_paths(
            measurements, hypotheses, previous=previous_snapshot
        )
    """
    config = get_indirect_path_config()
    
    if not config.MEASUREMENTS_ENABLED:
        logger.debug("Indirect path measurements disabled, skipping extension")
        return base_measurements
    
    try:
        indirect_measurements = IndirectPathMeasurements.compute(
            hypotheses, previous_snapshot
        )
        base_measurements.update(indirect_measurements)
        logger.debug(
            f"Extended measurements with {len(indirect_measurements)} "
            f"indirect-path metrics"
        )
    except Exception as e:
        logger.error(
            f"Failed to compute indirect path measurements: {e}",
            exc_info=True,
        )
        # Fail gracefully: do not update measurements
    
    return base_measurements


def should_include_indirect_paths() -> bool:
    """Check if indirect path measurements are enabled."""
    config = get_indirect_path_config()
    return config.MEASUREMENTS_ENABLED
