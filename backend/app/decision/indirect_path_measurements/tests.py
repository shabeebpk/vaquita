"""
Basic validation and sanity checks for indirect path measurements.
Run to verify measurements are computed correctly.
"""

import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def validate_measurements(
    measurements: Dict[str, Any],
    hypotheses: List[Dict[str, Any]],
) -> Dict[str, str]:
    """
    Validate measurements against hypotheses.
    
    Args:
        measurements: Computed measurements dict
        hypotheses: Source hypotheses list
    
    Returns:
        Dict of validation results: {check_name: status}
        status = "OK" | "WARN" | "FAIL"
    """
    results = {}
    
    # Check 1: Counts match
    if measurements.get("total_hypothesis_count") == len(hypotheses):
        results["count_match"] = "OK"
    else:
        results["count_match"] = "FAIL"
        logger.warning(
            f"Total count mismatch: {measurements.get('total_hypothesis_count')} "
            f"!= {len(hypotheses)}"
        )
    
    # Check 2: Pass count <= total count
    passed = measurements.get("passed_hypothesis_count", 0)
    total = measurements.get("total_hypothesis_count", 0)
    if passed <= total:
        results["pass_count"] = "OK"
    else:
        results["pass_count"] = "FAIL"
        logger.warning(f"Passed count {passed} > total {total}")
    
    # Check 3: Pass ratio in [0, 1]
    ratio = measurements.get("pass_ratio", -1)
    if 0.0 <= ratio <= 1.0:
        results["pass_ratio"] = "OK"
    else:
        results["pass_ratio"] = "FAIL"
        logger.warning(f"Pass ratio {ratio} out of bounds")
    
    # Check 4: Multiple same-source-target paths increase max_paths_per_pair
    same_pair_hyps = [
        h for h in hypotheses
        if h.get("source") == h.get("source") and h.get("target") == h.get("target")
    ]
    # Find actual max_paths_per_pair by checking hypotheses
    pair_groups = {}
    for h in hypotheses:
        key = (h.get("source"), h.get("target"))
        if key not in pair_groups:
            pair_groups[key] = []
        pair_groups[key].append(h)
    
    actual_max_paths = 0
    for hyps in pair_groups.values():
        distinct = len(set(tuple(h.get("path", [])) for h in hyps if "path" in h))
        actual_max_paths = max(actual_max_paths, distinct)
    
    if actual_max_paths > 1:
        if measurements.get("max_paths_per_pair", 0) >= actual_max_paths:
            results["distinct_paths"] = "OK"
        else:
            results["distinct_paths"] = "WARN"
            logger.warning(
                f"max_paths_per_pair {measurements.get('max_paths_per_pair')} "
                f"< actual {actual_max_paths}"
            )
    else:
        results["distinct_paths"] = "SKIP"
    
    # Check 5: Redundancy score in [0, 1]
    red = measurements.get("redundancy_score", -1)
    if 0.0 <= red <= 1.0:
        results["redundancy"] = "OK"
    else:
        results["redundancy"] = "FAIL"
        logger.warning(f"Redundancy score {red} out of bounds")
    
    # Check 6: Normalized confidence in [0, 1]
    conf = measurements.get("max_normalized_confidence", -1)
    if 0.0 <= conf <= 1.0:
        results["normalized_confidence"] = "OK"
    else:
        results["normalized_confidence"] = "WARN"
    
    # Check 7: Diversity score in [0, 1]
    div = measurements.get("diversity_score", -1)
    if 0.0 <= div <= 1.0:
        results["diversity"] = "OK"
    else:
        results["diversity"] = "FAIL"
    
    # Check 8: Graph density in [0, 1]
    dens = measurements.get("graph_density", -1)
    if 0.0 <= dens <= 1.0:
        results["density"] = "OK"
    else:
        results["density"] = "FAIL"
    
    # Check 9: Path length variance >= 0
    var = measurements.get("path_length_variance", -1)
    if var >= 0.0:
        results["path_variance"] = "OK"
    else:
        results["path_variance"] = "FAIL"
    
    # Check 10: Temporal placeholders are numeric or None
    growth = measurements.get("evidence_growth_rate")
    stability = measurements.get("hypothesis_stability")
    timestamp = measurements.get("time_since_last_update")
    if (isinstance(growth, (int, float)) and 
        isinstance(stability, (int, float)) and 
        isinstance(timestamp, int)):
        results["temporal_placeholders"] = "OK"
    else:
        results["temporal_placeholders"] = "FAIL"
    
    ok_count = sum(1 for v in results.values() if v == "OK")
    logger.info(
        f"Measurements validation: {ok_count}/{len(results)} checks passed"
    )
    
    return results


def example_usage():
    """Example: compute and validate measurements."""
    from app.decision.indirect_path_measurements.indirect_paths import IndirectPathMeasurements
    from app.decision.indirect_path_measurements.config import IndirectPathConfig
    
    # Load config
    IndirectPathConfig.load_from_env()
    IndirectPathMeasurements.load_config(IndirectPathConfig.to_dict())
    
    # Sample hypotheses
    sample_hyps = [
        {
            "source": "A",
            "target": "B",
            "path": ["A", "X", "B"],
            "confidence": 5,
            "passed_filter": True,
        },
        {
            "source": "A",
            "target": "B",
            "path": ["A", "Y", "B"],
            "confidence": 4,
            "passed_filter": True,
        },
        {
            "source": "A",
            "target": "C",
            "path": ["A", "Z", "C"],
            "confidence": 3,
            "passed_filter": False,
            "filter_reason": "low_confidence",
        },
    ]
    
    # Compute measurements
    measurements = IndirectPathMeasurements.compute(sample_hyps)
    
    # Validate
    validation = validate_measurements(measurements, sample_hyps)
    
    logger.info(f"Measurements: {measurements}")
    logger.info(f"Validation: {validation}")
    
    return measurements, validation


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    example_usage()
