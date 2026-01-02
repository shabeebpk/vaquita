"""Measurements / signals layer.

Computes deterministic, reusable signals from existing artifacts
(semantic graph, hypotheses, job metadata). These are pure functions
with no decision logic—only data aggregation.

Key design: separates hypothesis populations (total, passed, rejected)
and applies statistics only to passed hypotheses.
"""
from typing import Dict, List, Any
import logging

from app.decision.config import get_decision_config

logger = logging.getLogger(__name__)


def compute_measurements(
    semantic_graph: Dict[str, Any],
    hypotheses: List[Dict[str, Any]],
    job_metadata: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compute a dictionary of deterministic signals from artifacts.
    
    Separates hypothesis populations and applies statistics only to passed hypotheses.
    
    Args:
        semantic_graph: Phase-3 semantic graph dict (nodes, edges).
        hypotheses: List of persisted hypothesis dicts (all explore + query results).
        job_metadata: Job context (id, status, user_text, created_at, etc.).
    
    Returns:
        A measurements dict with keys:
        - total_hypothesis_count: all hypotheses
        - passed_hypothesis_count: where passed_filter == True
        - rejected_hypothesis_count: where passed_filter == False
        - filtered_to_total_ratio: passed / total
        
        (All statistical measures below are computed from passed hypotheses only)
        - max_confidence: raw confidence (integer support count)
        - mean_confidence: average raw confidence
        - confidence_std: standard deviation
        - max_normalized_confidence: confidence / NORM_FACTOR, clamped to [0,1]
        - mean_normalized_confidence: average normalized confidence
        - unique_source_target_pairs: from passed only
        - diversity_score: ratio of unique nodes to total nodes
        - is_dominant_clear: bool
        - dominant_hypothesis_index: index in passed list
        - has_any_viable_hypothesis: bool (total > 0)
        
        - graph_density: edges / max_possible_edges
        - semantic_graph_node_count, semantic_graph_edge_count
        - job_* metadata
    """
    measurements = {}
    
    # ===== Split hypothesis populations =====
    total_hypotheses = hypotheses  # all rows
    passed_hypotheses = [h for h in hypotheses if h.get("passed_filter", False)]
    rejected_hypotheses = [h for h in hypotheses if not h.get("passed_filter", False)]
    
    # Population counts
    measurements["total_hypothesis_count"] = len(total_hypotheses)
    measurements["passed_hypothesis_count"] = len(passed_hypotheses)
    measurements["rejected_hypothesis_count"] = len(rejected_hypotheses)
    
    # Filter ratio (diagnostic)
    if measurements["total_hypothesis_count"] > 0:
        measurements["filtered_to_total_ratio"] = (
            measurements["passed_hypothesis_count"] / measurements["total_hypothesis_count"]
        )
    else:
        measurements["filtered_to_total_ratio"] = 0.0
    
    measurements["has_any_viable_hypothesis"] = measurements["total_hypothesis_count"] > 0
    
    # ===== Statistics computed ONLY from passed hypotheses =====
    config = get_decision_config()
    
    if passed_hypotheses:
        # Raw confidence (integer support counts)
        confidences = [h.get("confidence", 0) for h in passed_hypotheses]
        measurements["max_confidence"] = max(confidences) if confidences else 0
        measurements["mean_confidence"] = sum(confidences) / len(confidences) if confidences else 0
        
        # Standard deviation (simple)
        mean = measurements["mean_confidence"]
        if len(confidences) > 1:
            variance = sum((c - mean) ** 2 for c in confidences) / len(confidences)
            measurements["confidence_std"] = variance ** 0.5
        else:
            measurements["confidence_std"] = 0.0
        
        # Normalized confidence (0-1 range)
        # normalized = min(raw_confidence / NORMALIZATION_FACTOR, 1.0)
        normalized_confidences = [
            min(c / config.CONFIDENCE_NORMALIZATION_FACTOR, 1.0) for c in confidences
        ]
        measurements["max_normalized_confidence"] = max(normalized_confidences) if normalized_confidences else 0.0
        measurements["mean_normalized_confidence"] = (
            sum(normalized_confidences) / len(normalized_confidences) if normalized_confidences else 0.0
        )
        
        # Unique source-target pairs (from passed only)
        pairs = set()
        for h in passed_hypotheses:
            pairs.add((h.get("source"), h.get("target")))
        measurements["unique_source_target_pairs"] = len(pairs)
        
        # Diversity: ratio of unique nodes in paths to total nodes involved (from passed only)
        all_nodes_in_paths = set()
        for h in passed_hypotheses:
            path = h.get("path", [])
            all_nodes_in_paths.update(path)
        total_nodes = sum(len(h.get("path", [])) for h in passed_hypotheses)
        if total_nodes > 0:
            measurements["diversity_score"] = len(all_nodes_in_paths) / total_nodes
        else:
            measurements["diversity_score"] = 0.0
        
        # Dominant hypothesis: clear if first normalized_confidence >> second
        # Uses normalized confidence for consistent probability-like semantics
        if len(normalized_confidences) > 1:
            first_conf = normalized_confidences[0]
            second_conf = normalized_confidences[1]
            gap = first_conf - second_conf
            # "Clear dominant" if gap > (gap_ratio * first_confidence)
            measurements["is_dominant_clear"] = (
                gap > config.DOMINANT_GAP_RATIO * first_conf if first_conf > 0 else False
            )
            measurements["dominant_hypothesis_index"] = 0
        else:
            measurements["is_dominant_clear"] = len(passed_hypotheses) > 0
            measurements["dominant_hypothesis_index"] = 0 if passed_hypotheses else -1
    else:
        # No passed hypotheses
        measurements["max_confidence"] = 0
        measurements["mean_confidence"] = 0.0
        measurements["confidence_std"] = 0.0
        measurements["max_normalized_confidence"] = 0.0
        measurements["mean_normalized_confidence"] = 0.0
        measurements["unique_source_target_pairs"] = 0
        measurements["diversity_score"] = 0.0
        measurements["is_dominant_clear"] = False
        measurements["dominant_hypothesis_index"] = -1
    
    # ===== Graph-level signals =====
    semantic_nodes = semantic_graph.get("nodes", [])
    semantic_edges = semantic_graph.get("edges", [])
    node_count = len(semantic_nodes)
    edge_count = len(semantic_edges)
    
    if node_count > 1:
        max_edges = node_count * (node_count - 1)
        measurements["graph_density"] = edge_count / max_edges if max_edges > 0 else 0.0
    else:
        measurements["graph_density"] = 0.0
    
    measurements["semantic_graph_node_count"] = node_count
    measurements["semantic_graph_edge_count"] = edge_count
    
    # ===== Job metadata signals =====
    measurements["job_id"] = job_metadata.get("id")
    measurements["job_status"] = job_metadata.get("status")
    measurements["job_user_text_length"] = len(job_metadata.get("user_text", ""))
    
    return measurements
