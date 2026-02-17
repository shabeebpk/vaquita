"""
Signal application: apply learning outcomes to SearchQuery reputation and status.

Separated from evaluator to isolate side effects (mutation of SearchQuery state).
Never triggers control flow; only updates query learning metadata.
"""
import logging
from typing import Optional
from datetime import datetime
from sqlalchemy.orm import Session

from app.storage.models import SearchQueryRun, SearchQuery, Job
from app.signals.evaluator import SignalConfig

logger = logging.getLogger(__name__)


def classify_signal(
    delta: float,
    config: Optional[SignalConfig] = None
) -> tuple:
    """
    Classify signal as positive, zero, or negative.
    
    Args:
        delta: Weighted measurement delta
        config: SignalConfig (created if None, using defaults)
    
    Returns:
        Tuple of (signal_delta: int, status: str)
        - signal_delta: 1 (positive), 0 (zero), -1 (negative)
        - status: 'reusable', 'exhausted', 'blocked'
    """
    if config is None:
        config = SignalConfig()
    
    if delta >= config.positive_threshold:
        return 1, "reusable"
    elif delta <= config.negative_threshold:
        return -1, "blocked"
    else:
        return 0, "exhausted"


def apply_signal_result(
    search_query_run: SearchQueryRun,
    signal_delta: int,
    new_status: str,
    session: Session,
    config: Optional[SignalConfig] = None
) -> None:
    """
    Apply signal result to SearchQuery: update status, reputation, and papers.
    """
    if config is None:
        # Load from job
        job = session.query(Job).filter(Job.id == search_query_run.job_id).first()
        if job and job.job_config:
            config = SignalConfig(job.job_config)
        else:
            config = SignalConfig()
    
    search_query = session.query(SearchQuery).filter(
        SearchQuery.id == search_query_run.search_query_id
    ).first()
    
    if not search_query:
        logger.error(f"SearchQuery not found for run {search_query_run.id}")
        return
    
    # Update signal_delta in run
    search_query_run.signal_delta = signal_delta
    
    # Update SearchQuery status and reputation
    old_status = search_query.status
    old_reputation = search_query.reputation_score
    
    search_query.status = new_status
    
    search_query.status = new_status
    
    if signal_delta > 0:
        search_query.reputation_score += config.reputation_on_positive
        logger.info(
            f"SearchQuery {search_query.id}: reputation {old_reputation} → "
            f"{search_query.reputation_score} (positive signal)"
        )
        
    else:  # Zero or Negative
        if signal_delta < 0:
            search_query.reputation_score += config.reputation_on_negative
            logger.info(
                f"SearchQuery {search_query.id}: reputation {old_reputation} → "
                f"{search_query.reputation_score} (negative signal)"
            )
        else:
            logger.info(f"SearchQuery {search_query.id}: reputation unchanged (zero signal)")
    
    search_query.updated_at = datetime.utcnow()
    
    logger.info(
        f"Applied signal result to SearchQuery {search_query.id}: "
        f"status {old_status} → {new_status}, delta={signal_delta}"
    )
