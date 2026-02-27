"""Measurements / signals layer.

Computes deterministic, reusable signals from existing artifacts
(semantic graph, hypotheses, job metadata). These are pure functions
with no decision logic—only data aggregation.

Key design: separates hypothesis populations (total, passed, rejected)
and applies statistics only to passed hypotheses.

Indirect path measurements are optionally extended via app/decision/indirect_path_measurements/
submodule for additional structural metrics (paths_per_pair, redundancy_score, etc.).
"""
from typing import Dict, List, Any, Optional
import logging

from app.decision.config import DecisionConfig
from app.decision.indirect_path_measurements.config import IndirectPathConfig
from app.decision.indirect_path_measurements.integration import extend_measurements_with_indirect_paths
from app.storage.models import Job
from app.storage.db import engine
from sqlalchemy.orm import Session
from app.path_reasoning.filtering.logic import is_low_confidence_rejection

logger = logging.getLogger(__name__)


def compute_measurements(
    semantic_graph: Dict[str, Any],
    hypotheses: List[Dict[str, Any]],
    job_metadata: Dict[str, Any],
    previous_measurement_snapshot: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Compute a dictionary of deterministic signals from artifacts.
    """
    measurements = {}
    
    # Load Configuration from Job
    job_id = job_metadata.get("id")
    decision_config = None
    indirect_config = None
    
    # verification mode measurements can short-circuit and use job metadata
    job_mode = job_metadata.get("mode")
    if job_mode == "verification":
        # For verification, only check if all queries are 'done' (halting condition)
        with Session(engine) as session:
            from app.storage.models import SearchQuery
            remaining_new = session.query(SearchQuery).filter(
                SearchQuery.job_id == job_id,
                SearchQuery.status == "new"
            ).count()
        # Verification halts when all queries are done (no 'new' queries remain)
        measurements["verification_complete"] = (remaining_new == 0)
        vr = job_metadata.get("verification_result") or {}
        measurements["verification_found"] = vr.get("found", False)
        measurements["verification_type"] = vr.get("type")
        # other measurements are irrelevant for verification; return early
        return measurements

    if job_id:
        with Session(engine) as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if job and job.job_config:
                decision_config = DecisionConfig(job.job_config)
                indirect_config = IndirectPathConfig(job.job_config)
    
    if not decision_config:
        decision_config = DecisionConfig() # Defaults
    if not indirect_config:
        indirect_config = IndirectPathConfig() # Defaults

    # ===== Split hypothesis populations =====
    total_hypotheses = hypotheses  # all rows
    passed_hypotheses = [h for h in hypotheses if h.get("passed_filter", False)]
    rejected_hypotheses = [h for h in hypotheses if not h.get("passed_filter", False)]
    promising_hypotheses = [h for h in hypotheses if is_low_confidence_rejection(h)]
    
    # Population counts
    measurements["total_hypothesis_count"] = len(total_hypotheses)
    measurements["passed_hypothesis_count"] = len(passed_hypotheses)
    measurements["rejected_hypothesis_count"] = len(rejected_hypotheses)
    measurements["promising_hypothesis_count"] = len(promising_hypotheses)
    
    # Filter ratio (diagnostic)
    if measurements["total_hypothesis_count"] > 0:
        measurements["filtered_to_total_ratio"] = (
            measurements["passed_hypothesis_count"] / measurements["total_hypothesis_count"]
        )
    else:
        measurements["filtered_to_total_ratio"] = 0.0
    
    
    # ===== Statistics computed ONLY from passed hypotheses =====
    
    if passed_hypotheses:
        # Raw confidence (integer support counts)
        confidences = [h.get("confidence", 0) for h in passed_hypotheses]
        # Normalized confidence (0-1 range)
        normalized_confidences = [
            min(c / decision_config.CONFIDENCE_NORMALIZATION_FACTOR, 1.0) for c in confidences
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
        measurements["unique_nodes_in_paths"] = len(all_nodes_in_paths)
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
                gap > decision_config.DOMINANT_GAP_RATIO * first_conf if first_conf > 0 else False
            )
        else:
            measurements["is_dominant_clear"] = len(passed_hypotheses) > 0
    else:
        # No passed hypotheses
        measurements["max_normalized_confidence"] = 0.0
        measurements["mean_normalized_confidence"] = 0.0
        measurements["unique_source_target_pairs"] = 0
        measurements["unique_nodes_in_paths"] = 0
        measurements["diversity_score"] = 0.0
        measurements["is_dominant_clear"] = False
    
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
    
    # ===== Extend with indirect path measurements (optional, configured via .env) =====
    # If INDIRECT_PATH_MEASUREMENTS_ENABLED=true, compute additional structural metrics
    # from hypotheses. These are read-only for now and do not influence decision logic.
    measurements = extend_measurements_with_indirect_paths(
        measurements,
        hypotheses,
        previous_snapshot=previous_measurement_snapshot,
        config=indirect_config,
    )
    
    # ===== Growth Score Calculation =====
    # score = Δ(unique_nodes) + Δ(diversity_ratio) + Δ(passed_count)
    if previous_measurement_snapshot:
        prev = previous_measurement_snapshot
        
        delta_nodes = measurements.get("unique_nodes_in_paths", 0) - prev.get("unique_nodes_in_paths", 0)
        delta_div = measurements.get("diversity_score", 0.0) - prev.get("diversity_score", 0.0)
        delta_passed = measurements.get("passed_hypothesis_count", 0) - prev.get("passed_hypothesis_count", 0)
        
        measurements["growth_score"] = float(delta_nodes + delta_div + delta_passed)
        logger.info(
            f"Growth Score: {measurements['growth_score']:.3f} "
            f"(Δnodes={delta_nodes}, Δdiv={delta_div:.3f}, Δpassed={delta_passed})"
        )
    else:
        # First run: no growth possible, or assume initial signals are growth?
        # User implies Δ, so we need a previous snapshot.
        measurements["growth_score"] = 0.0
    
    return measurements
