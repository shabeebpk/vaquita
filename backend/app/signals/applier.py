"""
Signal application: apply learning outcomes to SearchQuery reputation and status.

Separated from evaluator to isolate side effects (mutation of SearchQuery state).
Never triggers control flow; only updates query learning metadata.
"""
import logging
from typing import Optional
from datetime import datetime
from sqlalchemy.orm import Session

from app.storage.models import SearchQueryRun, SearchQuery
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
        config: SignalConfig (created if None)
    
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
    
    Side effects:
    - Updates SearchQueryRun.signal_delta
    - Updates SearchQuery.status and reputation_score
    - Marks papers as used_for_research if positive signal
    
    Args:
        search_query_run: SearchQueryRun instance
        signal_delta: 1, 0, or -1
        new_status: 'reusable', 'exhausted', or 'blocked'
        session: SQLAlchemy session
        config: SignalConfig (created if None)
    """
    if config is None:
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
    
    # ---------------------------------------------------------
    # SIGNAL ATTRIBUTION
    # ---------------------------------------------------------
    # If positive signal:
    #   fetched_paper_ids -> accepted_paper_ids
    #   rejected_paper_ids = []
    # If zero or negative signal:
    #   fetched_paper_ids -> rejected_paper_ids
    #   accepted_paper_ids = []
    
    # We must explicitly cast list to ensure it's JSON-serializable if it wasn't already
    fetched_ids = list(search_query_run.fetched_paper_ids or [])
    
    if signal_delta > 0:
        search_query_run.accepted_paper_ids = fetched_ids
        search_query_run.rejected_paper_ids = []
        
        search_query.reputation_score += config.reputation_on_positive
        logger.info(
            f"SearchQuery {search_query.id}: reputation {old_reputation} → "
            f"{search_query.reputation_score} (positive signal)"
        )
        logger.info(f"Run {search_query_run.id}: Attributed {len(fetched_ids)} papers to ACCEPTED.")
        
    else:  # Zero or Negative
        search_query_run.accepted_paper_ids = []
        search_query_run.rejected_paper_ids = fetched_ids
        
        if signal_delta < 0:
            search_query.reputation_score += config.reputation_on_negative
            logger.info(
                f"SearchQuery {search_query.id}: reputation {old_reputation} → "
                f"{search_query.reputation_score} (negative signal)"
            )
        else:
            logger.info(f"SearchQuery {search_query.id}: reputation unchanged (zero signal)")
            
        logger.info(f"Run {search_query_run.id}: Attributed {len(fetched_ids)} papers to REJECTED.")
    
    search_query.updated_at = datetime.utcnow()
    
    logger.info(
        f"Applied signal result to SearchQuery {search_query.id}: "
        f"status {old_status} → {new_status}, delta={signal_delta}"
    )
