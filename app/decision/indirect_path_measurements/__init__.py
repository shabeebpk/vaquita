"""
Indirect path measurements submodule.

Provides clean separation of indirect-path measurement computation from decision logic.
These measurements can be toggled on/off via environment configuration.

Note: compute_measurements is in app/decision/measurements.py (parent), not here.
This submodule provides indirect path-specific measurements and integration helpers.
"""

from app.decision.indirect_path_measurements.indirect_paths import IndirectPathMeasurements
from app.decision.indirect_path_measurements.config import IndirectPathConfig, get_indirect_path_config
from app.decision.indirect_path_measurements.integration import (
    extend_measurements_with_indirect_paths,
    should_include_indirect_paths,
)

__all__ = [
    "IndirectPathMeasurements",
    "IndirectPathConfig",
    "get_indirect_path_config",
    "extend_measurements_with_indirect_paths",
    "should_include_indirect_paths",
]
