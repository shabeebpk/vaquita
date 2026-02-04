"""
Signal computation: evaluate measurement deltas to classify SearchQuery learning outcomes.

Separated from fetching and execution logic.
Compares DecisionResult snapshots before and after SearchQueryRun.
Signal never directly triggers control flow; it only updates query learning state.
"""
import logging
import os
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

from app.storage.models import SearchQueryRun, SearchQuery, DecisionResult, Paper

logger = logging.getLogger(__name__)


class SignalConfig:
    """Configuration for signal computation."""
    
    def __init__(self, job_config: dict = None):
        """Initialize config from job configuration."""
        job_config = job_config or {}
        expert_settings = job_config.get("expert_settings", {})
        signals_config = expert_settings.get("signals", {})
        
        # Thresholds for signal classification
        self.positive_threshold = float(signals_config.get("positive_threshold", 1.0))
        self.negative_threshold = float(signals_config.get("negative_threshold", -1.0))
        
        # Reputation adjustments
        self.reputation_on_positive = int(signals_config.get("reputation_positive_delta", 10))
        self.reputation_on_negative = int(signals_config.get("reputation_negative_delta", -20))
        
        # Measurement weights for delta computation
        weights = signals_config.get("weights", {})
        self.measurement_weights = {
            "passed_hypothesis_count": float(weights.get("passed_hypothesis_count", 1.0)),
            "mean_confidence": float(weights.get("mean_confidence", 0.8)),
            "graph_density": float(weights.get("graph_density", 0.5)),
            "filtered_to_total_ratio": float(weights.get("filtered_to_total_ratio", 0.3)),
        }
        
        # Normalization: max expected delta per measurement
        max_deltas = signals_config.get("max_deltas", {})
        self.measurement_max_deltas = {
            "passed_hypothesis_count": float(max_deltas.get("passed_hypothesis_count", 100.0)),
            "mean_confidence": float(max_deltas.get("mean_confidence", 20.0)),
            "graph_density": float(max_deltas.get("graph_density", 0.2)),
            "filtered_to_total_ratio": float(max_deltas.get("filtered_to_total_ratio", 0.5)),
        }
        
        logger.debug(
            f"SignalConfig: positive_threshold={self.positive_threshold}, "
            f"negative_threshold={self.negative_threshold}, "
            f"rep_positive={self.reputation_on_positive}, rep_negative={self.reputation_on_negative}"
        )


def get_last_decision_before_run(
    job_id: int,
    search_query_run: SearchQueryRun,
    session: Session
) -> Optional[Dict[str, Any]]:
    """
    Get the DecisionResult that preceded this SearchQueryRun.
    
    Args:
        job_id: Job ID
        search_query_run: SearchQueryRun instance
        session: SQLAlchemy session
    
    Returns:
        Dict with DecisionResult info or None if not found
    """
    previous_run = session.query(DecisionResult).filter(
        DecisionResult.job_id == job_id,
        DecisionResult.created_at < search_query_run.created_at
    ).order_by(DecisionResult.created_at.desc()).first()
    
    if previous_run:
        return {
            "measurements": previous_run.measurements_snapshot,
            "decision_label": previous_run.decision_label,
            "created_at": previous_run.created_at
        }
    
    return None


def get_current_decision_after_run(
    job_id: int,
    search_query_run: SearchQueryRun,
    session: Session
) -> Optional[Dict[str, Any]]:
    """
    Get the DecisionResult that followed this SearchQueryRun.
    
    Args:
        job_id: Job ID
        search_query_run: SearchQueryRun instance
        session: SQLAlchemy session
    
    Returns:
        Dict with DecisionResult info or None if not found
    """
    next_decision = session.query(DecisionResult).filter(
        DecisionResult.job_id == job_id,
        DecisionResult.created_at > search_query_run.created_at
    ).order_by(DecisionResult.created_at.asc()).first()
    
    if next_decision:
        return {
            "measurements": next_decision.measurements_snapshot,
            "decision_label": next_decision.decision_label,
            "created_at": next_decision.created_at
        }
    
    return None


def find_pending_run_for_evaluation(
    job_id: int,
    current_decision: Dict[str, Any],
    session: Session
) -> List[SearchQueryRun]:
    """
    Find all pending SearchQueryRuns that occurred between the previous decision
    and the current decision.
    
    Strict Timing Rule:
    previous_decision.created_at < run.created_at < current_decision.created_at
    
    Attribution Rule:
    Only returns runs that have NOT yet had a signal applied (signal_delta is None).
    
    Args:
        job_id: Job ID
        current_decision: Dict with current decision info
        session: SQLAlchemy session
        
    Returns:
        List of SearchQueryRun instances (empty list if none found)
    """
    # 1. Find previous decision
    previous_decision = session.query(DecisionResult).filter(
        DecisionResult.job_id == job_id,
        DecisionResult.created_at < current_decision['created_at']
    ).order_by(DecisionResult.created_at.desc()).first()
    
    if not previous_decision:
        logger.info("No previous decision found; cannot establish time window for signal attribution.")
        return []
    
    # 2. Find SearchQueryRuns in the time window with no signal applied
    pending_runs = session.query(SearchQueryRun).filter(
        SearchQueryRun.job_id == job_id,
        SearchQueryRun.created_at > previous_decision.created_at,
        SearchQueryRun.created_at < current_decision['created_at'],
        SearchQueryRun.signal_delta.is_(None)
    ).order_by(SearchQueryRun.created_at.desc()).all()
    
    if not pending_runs:
        logger.info(
            f"No pending SearchQueryRun found between {previous_decision.created_at} "
            f"and {current_decision['created_at']}"
        )
        return []
        
    return pending_runs


def compute_measurement_delta(
    previous_measurements: Dict[str, Any],
    current_measurements: Dict[str, Any],
    config: Optional[SignalConfig] = None
) -> float:
    """
    Compute weighted delta between measurement sets.
    
    Args:
        previous_measurements: Measurements dict from before SearchQueryRun
        current_measurements: Measurements dict from after SearchQueryRun
        config: SignalConfig (created if None)
    
    Returns:
        Normalized delta score (can be negative, zero, or positive)
    """
    if config is None:
        config = SignalConfig()
    
    if not previous_measurements or not current_measurements:
        logger.warning("Cannot compute delta with missing measurements")
        return 0.0
    
    total_weighted_delta = 0.0
    
    for measurement_name, weight in config.measurement_weights.items():
        prev_value = previous_measurements.get(measurement_name, 0)
        curr_value = current_measurements.get(measurement_name, 0)
        
        # Raw delta
        raw_delta = curr_value - prev_value
        
        # Normalize by max expected delta
        max_delta = config.measurement_max_deltas.get(measurement_name, 1.0)
        if max_delta > 0:
            normalized_delta = raw_delta / max_delta
        else:
            normalized_delta = 0.0
        
        # Weighted contribution
        weighted = normalized_delta * weight
        total_weighted_delta += weighted
        
        logger.debug(
            f"  {measurement_name}: {prev_value} → {curr_value} "
            f"(raw_delta={raw_delta}, normalized={normalized_delta:.3f}, weighted={weighted:.3f})"
        )
    
    logger.info(f"Computed total_weighted_delta: {total_weighted_delta:.3f}")
    return total_weighted_delta
