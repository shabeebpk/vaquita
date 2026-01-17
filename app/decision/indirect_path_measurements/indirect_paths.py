"""
Indirect Path-Based Measurements

Computes structural metrics from indirect paths (hypotheses with intermediate nodes).
All computations operate on the hypotheses table only, deriving aggregated metrics
without storing per-path state. Configuration-driven, easily toggleable.

Measurements computed:
- Path enumeration: max_paths_per_pair, mean_paths_per_pair, dominant_pair_path_ratio
- Intermediate nodes: unique_intermediate_nodes_dominant, redundancy_score
- Path structure: mean_path_length, path_length_variance
- Confidence distribution: confidence_variance, dominant_confidence_gap
- Diversity: pair_distribution_entropy
- Temporal placeholders: evidence_growth_rate, hypothesis_stability, time_since_last_update
"""

from __future__ import annotations

import logging
import math
from typing import Dict, List, Tuple, Any, Optional
from collections import defaultdict
from datetime import datetime

logger = logging.getLogger(__name__)


class IndirectPathMeasurements:
    """Computes indirect-path-based measurements from hypotheses."""
    
    # Configuration (set by load_config)
    ENABLE_INDIRECT_PATH_MEASUREMENTS = True
    COMPUTE_TEMPORAL_PLACEHOLDERS = True
    CONFIDENCE_NORM_FACTOR = 5.0
    DOMINANCE_GAP_THRESHOLD = 0.2
    
    @staticmethod
    def load_config(config_dict: Optional[Dict[str, Any]] = None) -> None:
        """Load configuration from env-like dict. If None, uses defaults."""
        if config_dict is None:
            config_dict = {}
        IndirectPathMeasurements.ENABLE_INDIRECT_PATH_MEASUREMENTS = config_dict.get(
            "MEASUREMENTS_ENABLED", True
        )
        IndirectPathMeasurements.COMPUTE_TEMPORAL_PLACEHOLDERS = config_dict.get(
            "TEMPORAL_PLACEHOLDERS", True
        )
        IndirectPathMeasurements.CONFIDENCE_NORM_FACTOR = float(
            config_dict.get("CONFIDENCE_NORM_FACTOR", 5.0)
        )
        IndirectPathMeasurements.DOMINANCE_GAP_THRESHOLD = float(
            config_dict.get("DOMINANCE_GAP_THRESHOLD", 0.2)
        )
        logger.info(
            f"IndirectPathMeasurements config loaded: "
            f"enabled={IndirectPathMeasurements.ENABLE_INDIRECT_PATH_MEASUREMENTS}, "
            f"temporal={IndirectPathMeasurements.COMPUTE_TEMPORAL_PLACEHOLDERS}"
        )
    
    @staticmethod
    def compute(
        hypotheses: List[Dict[str, Any]],
        previous_snapshot: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Compute all indirect-path measurements from hypotheses list.
        
        Args:
            hypotheses: List of hypothesis dicts (each represents one indirect path).
                       Expected keys: source, target, path, confidence, passed_filter
            previous_snapshot: Previous DecisionResult.measurements_snapshot dict for temporal comparisons.
        
        Returns:
            Dict with all measurement keys (see module docstring).
        """
        if not IndirectPathMeasurements.ENABLE_INDIRECT_PATH_MEASUREMENTS:
            logger.debug("Indirect path measurements disabled, returning empty dict")
            return {}
        
        if not hypotheses:
            logger.debug("No hypotheses provided, returning zero measurements")
            return IndirectPathMeasurements._zero_measurements()
        
        measurements = {}
        
        # === Basic Counts ===
        measurements["total_hypothesis_count"] = len(hypotheses)
        passed = [h for h in hypotheses if h.get("passed_filter", False)]
        measurements["passed_hypothesis_count"] = len(passed)
        measurements["pass_ratio"] = (
            len(passed) / len(hypotheses) if hypotheses else 0.0
        )
        
        # === Group by (source, target) pairs ===
        pair_groups = IndirectPathMeasurements._group_by_source_target(hypotheses)
        measurements["unique_source_target_pairs"] = len(pair_groups)
        
        # === Paths per pair ===
        paths_per_pair = IndirectPathMeasurements._paths_per_pair(pair_groups)
        measurements["max_paths_per_pair"] = (
            max(paths_per_pair) if paths_per_pair else 0
        )
        measurements["mean_paths_per_pair"] = (
            sum(paths_per_pair) / len(paths_per_pair) if paths_per_pair else 0.0
        )
        
        # === Dominant pair (most confident) ===
        dominant_pair_id = IndirectPathMeasurements._find_dominant_pair(
            pair_groups, hypotheses
        )
        measurements["dominant_pair_id"] = dominant_pair_id
        
        if dominant_pair_id:
            dominant_hyps = pair_groups[dominant_pair_id]
            measurements["dominant_pair_path_ratio"] = (
                len(set(tuple(h.get("path", [])) for h in dominant_hyps if "path" in h))
                / len(dominant_hyps)
                if dominant_hyps else 0.0
            )
            measurements["unique_intermediate_nodes_dominant"] = (
                IndirectPathMeasurements._count_unique_intermediates(dominant_hyps)
            )
        else:
            measurements["dominant_pair_path_ratio"] = 0.0
            measurements["unique_intermediate_nodes_dominant"] = 0
        
        # === Redundancy Score (intermediate node reuse) ===
        measurements["redundancy_score"] = (
            IndirectPathMeasurements._compute_redundancy_score(hypotheses)
        )
        
        # === Confidence metrics ===
        confidences = [h.get("confidence", 0) for h in hypotheses if h.get("passed_filter", False)]
        measurements["max_normalized_confidence"] = (
            min(max(confidences) / IndirectPathMeasurements.CONFIDENCE_NORM_FACTOR, 1.0)
            if confidences else 0.0
        )
        measurements["mean_normalized_confidence"] = (
            (sum(confidences) / len(confidences)) / IndirectPathMeasurements.CONFIDENCE_NORM_FACTOR
            if confidences else 0.0
        )
        measurements["confidence_variance"] = (
            IndirectPathMeasurements._compute_variance(confidences)
        )
        measurements["dominant_confidence_gap"] = (
            IndirectPathMeasurements._compute_confidence_gap(pair_groups, hypotheses)
        )
        
        # === Dominance clarity ===
        measurements["is_dominant_clear"] = (
            measurements["dominant_confidence_gap"] > IndirectPathMeasurements.DOMINANCE_GAP_THRESHOLD
        )
        
        # === Diversity metrics ===
        measurements["diversity_score"] = (
            IndirectPathMeasurements._compute_diversity_score(hypotheses)
        )
        measurements["pair_distribution_entropy"] = (
            IndirectPathMeasurements._compute_entropy(paths_per_pair)
        )
        
        # === Graph density (simplified) ===
        measurements["graph_density"] = (
            IndirectPathMeasurements._compute_graph_density(hypotheses)
        )
        
        # === Path structure metrics ===
        path_lengths = [len(h.get("path", [])) for h in hypotheses]
        measurements["mean_path_length"] = (
            sum(path_lengths) / len(path_lengths) if path_lengths else 0.0
        )
        measurements["path_length_variance"] = (
            IndirectPathMeasurements._compute_variance(path_lengths)
        )
        
        # === Filter rejection reasons (summary) ===
        measurements["filter_rejection_reasons"] = (
            IndirectPathMeasurements._aggregate_rejection_reasons(hypotheses)
        )
        
        # === Temporal placeholders (read-only for now) ===
        if IndirectPathMeasurements.COMPUTE_TEMPORAL_PLACEHOLDERS:
            measurements["evidence_growth_rate"] = (
                IndirectPathMeasurements._compute_growth_rate(
                    len(passed), previous_snapshot
                )
            )
            measurements["hypothesis_stability"] = (
                IndirectPathMeasurements._compute_stability(hypotheses, previous_snapshot)
            )
            measurements["time_since_last_update"] = (
                int(datetime.utcnow().timestamp())
            )
        
        logger.debug(
            f"Computed {len(measurements)} indirect-path measurements "
            f"from {len(hypotheses)} hypotheses"
        )
        return measurements
    
    @staticmethod
    def _group_by_source_target(
        hypotheses: List[Dict[str, Any]]
    ) -> Dict[Tuple[str, str], List[Dict[str, Any]]]:
        """Group hypotheses by (source, target) pair."""
        groups: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
        for h in hypotheses:
            key = (h.get("source"), h.get("target"))
            if key[0] and key[1]:
                groups[key].append(h)
        return dict(groups)
    
    @staticmethod
    def _paths_per_pair(
        pair_groups: Dict[Tuple[str, str], List[Dict[str, Any]]],
    ) -> List[int]:
        """Count distinct paths for each (source, target) pair."""
        counts = []
        for pair_hyps in pair_groups.values():
            # Count distinct path tuples
            distinct_paths = len(
                set(tuple(h.get("path", [])) for h in pair_hyps if "path" in h)
            )
            counts.append(distinct_paths if distinct_paths > 0 else 1)
        return counts
    
    @staticmethod
    def _find_dominant_pair(
        pair_groups: Dict[Tuple[str, str], List[Dict[str, Any]]],
        hypotheses: List[Dict[str, Any]],
    ) -> Optional[Tuple[str, str]]:
        """Find the (source, target) pair with highest mean confidence."""
        max_confidence = -1.0
        dominant = None
        for pair_id, pair_hyps in pair_groups.items():
            confidences = [h.get("confidence", 0) for h in pair_hyps]
            if confidences:
                mean_conf = sum(confidences) / len(confidences)
                if mean_conf > max_confidence:
                    max_confidence = mean_conf
                    dominant = pair_id
        return dominant
    
    @staticmethod
    def _count_unique_intermediates(hypotheses: List[Dict[str, Any]]) -> int:
        """Count unique intermediate nodes across all paths in hypothesis group."""
        intermediates = set()
        for h in hypotheses:
            path = h.get("path", [])
            if len(path) > 2:
                # Intermediate nodes are all except first and last
                intermediates.update(path[1:-1])
        return len(intermediates)
    
    @staticmethod
    def _compute_redundancy_score(hypotheses: List[Dict[str, Any]]) -> float:
        """
        Compute redundancy score: degree of intermediate-node reuse.
        High score = many paths reuse same intermediate nodes (redundant).
        Low score = paths use diverse intermediates (non-redundant).
        
        Formula: (total_intermediate_occurrences - unique_intermediates) / total_intermediates
        """
        all_intermediates = []
        for h in hypotheses:
            path = h.get("path", [])
            if len(path) > 2:
                all_intermediates.extend(path[1:-1])
        
        if not all_intermediates:
            return 0.0
        
        unique_count = len(set(all_intermediates))
        total_count = len(all_intermediates)
        
        # Redundancy = how much reuse there is
        redundancy = (total_count - unique_count) / total_count if total_count > 0 else 0.0
        return min(1.0, max(0.0, redundancy))
    
    @staticmethod
    def _compute_variance(values: List[float]) -> float:
        """Compute variance of numeric values."""
        if len(values) < 2:
            return 0.0
        mean = sum(values) / len(values)
        variance = sum((x - mean) ** 2 for x in values) / len(values)
        return variance
    
    @staticmethod
    def _compute_confidence_gap(
        pair_groups: Dict[Tuple[str, str], List[Dict[str, Any]]],
        hypotheses: List[Dict[str, Any]],
    ) -> float:
        """
        Compute confidence gap: (1st_max - 2nd_max) / 1st_max.
        Measures how dominant the top pair is.
        """
        pair_max_confs = []
        for pair_hyps in pair_groups.values():
            confs = [h.get("confidence", 0) for h in pair_hyps]
            if confs:
                pair_max_confs.append(max(confs))
        
        if len(pair_max_confs) < 2:
            return 0.0
        
        pair_max_confs.sort(reverse=True)
        gap = (pair_max_confs[0] - pair_max_confs[1]) / pair_max_confs[0]
        return gap
    
    @staticmethod
    def _compute_diversity_score(hypotheses: List[Dict[str, Any]]) -> float:
        """
        Compute diversity score: unique_nodes / total_nodes in all paths.
        High = diverse graph; Low = concentrated around few nodes.
        """
        all_nodes = set()
        node_count = 0
        
        for h in hypotheses:
            path = h.get("path", [])
            all_nodes.update(path)
            node_count += len(path)
        
        if node_count == 0:
            return 0.0
        
        diversity = len(all_nodes) / node_count
        return min(1.0, diversity)
    
    @staticmethod
    def _compute_entropy(values: List[int]) -> float:
        """Compute Shannon entropy of value distribution."""
        if not values or sum(values) == 0:
            return 0.0
        
        total = sum(values)
        entropy = 0.0
        for v in values:
            if v > 0:
                p = v / total
                entropy -= p * math.log2(p)
        return entropy
    
    @staticmethod
    def _compute_graph_density(hypotheses: List[Dict[str, Any]]) -> float:
        """
        Simplified graph density: edges / max_possible_edges.
        Estimate: unique_pairs / (unique_nodes choose 2).
        """
        all_nodes = set()
        unique_pairs = set()
        
        for h in hypotheses:
            path = h.get("path", [])
            all_nodes.update(path)
            source = h.get("source")
            target = h.get("target")
            if source and target:
                unique_pairs.add((source, target))
        
        if len(all_nodes) < 2:
            return 0.0
        
        max_edges = len(all_nodes) * (len(all_nodes) - 1) / 2  # undirected
        actual_edges = len(unique_pairs)
        
        density = actual_edges / max_edges if max_edges > 0 else 0.0
        return min(1.0, density)
    
    @staticmethod
    def _aggregate_rejection_reasons(
        hypotheses: List[Dict[str, Any]]
    ) -> Dict[str, int]:
        reasons: Dict[str, int] = defaultdict(int)
        for h in hypotheses:
            if not h.get("passed_filter", False):
                reason = h.get("filter_reason", "unknown")
                if isinstance(reason, dict):
                    keys = sorted(str(k) for k in reason.keys())
                    reason_key = "|".join(keys) if keys else "unknown"
                elif isinstance(reason, list):
                    items = sorted(str(i) for i in reason)
                    reason_key = "|".join(items) if items else "unknown"
                elif isinstance(reason, str):
                    reason_key = reason
                else:
                    reason_key = str(reason) if reason is not None else "unknown"
                reasons[reason_key] += 1
        return dict(reasons)

    
    @staticmethod
    def _compute_growth_rate(
        current_passed: int,
        previous_snapshot: Optional[Dict[str, Any]],
    ) -> float:
        """
        Temporal placeholder: growth rate of passed hypotheses.
        Returns delta from previous snapshot, or 0 if no previous.
        """
        if not previous_snapshot:
            return 0.0
        
        prev_passed = previous_snapshot.get("passed_hypothesis_count", 0)
        if prev_passed == 0:
            return float(current_passed)
        
        growth = (current_passed - prev_passed) / prev_passed
        return growth
    
    @staticmethod
    def _compute_stability(
        hypotheses: List[Dict[str, Any]],
        previous_snapshot: Optional[Dict[str, Any]],
    ) -> float:
        """
        Temporal placeholder: hypothesis stability score.
        Compares current hypothesis set to previous; returns 0.0-1.0.
        Not yet used for decisions.
        """
        if not previous_snapshot:
            return 0.0
        
        # Simple overlap: shared source-target pairs
        current_pairs = {
            (h.get("source"), h.get("target")) for h in hypotheses
        }
        prev_pairs = set(previous_snapshot.get("_hypothesis_pairs", []))
        
        if not prev_pairs:
            return 1.0 if current_pairs else 0.0
        
        overlap = len(current_pairs & prev_pairs)
        stability = overlap / len(prev_pairs) if prev_pairs else 0.0
        return min(1.0, stability)
    
    @staticmethod
    def _zero_measurements() -> Dict[str, Any]:
        """Return zero-initialized measurements dict."""
        return {
            "total_hypothesis_count": 0,
            "passed_hypothesis_count": 0,
            "pass_ratio": 0.0,
            "unique_source_target_pairs": 0,
            "max_paths_per_pair": 0,
            "mean_paths_per_pair": 0.0,
            "dominant_pair_path_ratio": 0.0,
            "dominant_pair_id": None,
            "unique_intermediate_nodes_dominant": 0,
            "redundancy_score": 0.0,
            "max_normalized_confidence": 0.0,
            "mean_normalized_confidence": 0.0,
            "confidence_variance": 0.0,
            "dominant_confidence_gap": 0.0,
            "is_dominant_clear": False,
            "diversity_score": 0.0,
            "pair_distribution_entropy": 0.0,
            "graph_density": 0.0,
            "mean_path_length": 0.0,
            "path_length_variance": 0.0,
            "filter_rejection_reasons": {},
            "evidence_growth_rate": 0.0,
            "hypothesis_stability": 0.0,
            "time_since_last_update": 0,
        }
