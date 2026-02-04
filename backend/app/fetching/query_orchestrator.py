"""
SearchQuery lifecycle and orchestration.

Handles creation, reuse, and expansion of SearchQueries based on status and reputation.
SearchQuery is a first-class entity representing intent, not execution.
Never stores fetched results; execution tracked separately in SearchQueryRun.
"""
import hashlib
import logging
import json
import os
from typing import Dict, Any, Optional, Tuple, List
from datetime import datetime
from sqlalchemy.orm import Session

from app.storage.models import SearchQuery, SearchQueryRun
from app.domains import resolve_domain, DomainResolverConfig

logger = logging.getLogger(__name__)


class QueryOrchestratorConfig:
    """Configuration for query orchestration."""
    
    def __init__(self, job_config: dict = None):
        """Initialize config from job configuration."""
        job_config = job_config or {}
        query_config = job_config.get("query_config", {})
        
        # Max signature length for hypothesis_signature
        self.signature_length = int(query_config.get("signature_length", 64))
        
        # Initial reputation score for new queries
        self.initial_reputation = int(query_config.get("initial_reputation", 0))
        
        # Reputation decay per exhaustion
        self.reputation_exhaustion_decay = int(query_config.get("reputation_exhaustion_decay", -5))
        
        # Max reuse attempts for a single query before marking exhausted
        self.max_reuse_attempts = int(query_config.get("max_reuse_attempts", 3))
        
        logger.debug(
            f"QueryOrchestratorConfig: signature_len={self.signature_length}, "
            f"initial_rep={self.initial_reputation}, max_reuse={self.max_reuse_attempts}"
        )


def compute_hypothesis_signature(hypothesis: Dict[str, Any], config: Optional[QueryOrchestratorConfig] = None) -> str:
    """
    Compute stable hash from hypothesis endpoints (source and target).
    Never references hypothesis row IDs.
    
    Args:
        hypothesis: Dict with 'source' and 'target' keys
    
    Returns:
        Hex hash string (truncated to configured length)
    """
    source = str(hypothesis.get("source", "")).lower()
    target = str(hypothesis.get("target", "")).lower()
    
    # Stable hash from endpoints
    combined = f"{source}→{target}"
    hash_obj = hashlib.sha256(combined.encode("utf-8"))
    signature = hash_obj.hexdigest()
    
    if config is None:
        config = QueryOrchestratorConfig()
    return signature[:config.signature_length]


def get_or_create_search_query(
    hypothesis: Dict[str, Any],
    job_id: int,
    session: Session,
    query_text: str = "",
    llm_client: Optional[Any] = None,
    config: Optional[QueryOrchestratorConfig] = None
) -> SearchQuery:
    """
    Get existing SearchQuery by hypothesis_signature or create new one.
    
    Args:
        hypothesis: Hypothesis dict
        job_id: Job ID
        session: SQLAlchemy session
        query_text: Optional custom query text (derived from hypothesis if empty)
        llm_client: LLM client for domain resolution
        config: QueryOrchestratorConfig (created if None)
    
    Returns:
        SearchQuery model instance
    """
    if config is None:
        config = QueryOrchestratorConfig()
    
    hypothesis_signature = compute_hypothesis_signature(hypothesis, config=config)
    
    # Check if query already exists
    existing = session.query(SearchQuery).filter(
        SearchQuery.job_id == job_id,
        SearchQuery.hypothesis_signature == hypothesis_signature
    ).first()
    
    if existing:
        logger.debug(f"Found existing SearchQuery: {existing.id} (signature={hypothesis_signature})")
        return existing
    
    # Generate query text from hypothesis if not provided
    if not query_text:
        source = hypothesis.get("source", "")
        target = hypothesis.get("target", "")
        query_text = f"relationship between {source} and {target}"
    
    # Attempt domain resolution
    domain_config = DomainResolverConfig()
    resolved_domain, domain_confidence = resolve_domain(
        hypothesis, llm_client, domain_config
    )
    
    logger.info(
        f"Resolved domain for hypothesis {hypothesis_signature}: "
        f"{resolved_domain} (confidence={domain_confidence:.2f})"
    )
    
    # Capture current configuration snapshot
    config_snapshot = {
        "signature_length": config.signature_length,
        "initial_reputation": config.initial_reputation,
        "domain_resolver_threshold": domain_config.deterministic_threshold,
        "llm_threshold": domain_config.llm_threshold,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Create new SearchQuery
    search_query = SearchQuery(
        job_id=job_id,
        hypothesis_signature=hypothesis_signature,
        query_text=query_text,
        resolved_domain=resolved_domain,
        status="new",
        reputation_score=config.initial_reputation,
        config_snapshot=config_snapshot
    )
    
    session.add(search_query)
    session.flush()  # Get ID without committing
    
    logger.info(
        f"Created SearchQuery: {search_query.id} "
        f"(sig={hypothesis_signature}, domain={resolved_domain}, status=new)"
    )
    
    return search_query


def should_run_query(
    search_query: SearchQuery,
    session: Session,
    config: Optional[QueryOrchestratorConfig] = None
) -> Tuple[bool, str]:
    """
    Determine if a SearchQuery should be run based on status and history.
    
    Args:
        search_query: SearchQuery model instance
        session: SQLAlchemy session
        config: QueryOrchestratorConfig (created if None)
    
    Returns:
        Tuple of (should_run: bool, reason: str)
    """
    if config is None:
        config = QueryOrchestratorConfig()
    
    # Never run blocked queries
    if search_query.status == "blocked":
        return False, "Query blocked (negative signal history)"
    
    # Never run exhausted queries
    if search_query.status == "exhausted":
        return False, "Query exhausted (zero signal)"
    
    # New queries always run
    if search_query.status == "new":
        return True, "Initial attempt"
    
    # Reusable queries run if not exceeded max attempts
    if search_query.status == "reusable":
        run_count = session.query(SearchQueryRun).filter(
            SearchQueryRun.search_query_id == search_query.id
        ).count()
        
        if run_count < config.max_reuse_attempts:
            return True, f"Reuse attempt {run_count + 1}/{config.max_reuse_attempts}"
        else:
            return False, f"Exceeded max reuse attempts ({config.max_reuse_attempts})"
    
    return False, "Unknown status"


from sqlalchemy import func

def get_all_fetched_ids_for_job(
    job_id: int,
    session: Session
) -> List[int]:
    """
    Get all paper IDs ever fetched for a specific job.
    Uses database-side JSONB aggregation for efficiency.
    
    Args:
        job_id: Job ID
        session: SQLAlchemy session
        
    Returns:
        List of paper IDs
    """
    # Flatten the fetched_paper_ids JSONB array from all runs for this job
    # distinct() might be good if needed, but per-run logic should handle consistency
    result = session.query(
        func.jsonb_array_elements_text(SearchQueryRun.fetched_paper_ids)
    ).filter(
        SearchQueryRun.job_id == job_id
    ).all()
    
    # helper returns tuples, convert to integers
    if not result:
        return []
        
    return [int(row[0]) for row in result]


def record_search_run(
    search_query: SearchQuery,
    job_id: int,
    provider_used: str,
    reason: str,  # 'initial_attempt', 'reuse', 'expansion'
    fetched_paper_ids: List[int],
    accepted_paper_ids: List[int],
    rejected_paper_ids: List[int],
    session: Session,
    config: Optional[QueryOrchestratorConfig] = None
) -> SearchQueryRun:
    """
    Record execution of a SearchQuery.
    
    Args:
        search_query: SearchQuery model instance
        job_id: Job ID
        provider_used: Provider name ('arxiv', 'crossref', etc.)
        reason: Execution reason
        fetched_paper_ids: List of all fetched paper IDs (job-unique)
        accepted_paper_ids: List of accepted paper IDs
        rejected_paper_ids: List of rejected paper IDs
        session: SQLAlchemy session
        config: QueryOrchestratorConfig (unused but accepted for API consistency)
    
    Returns:
        SearchQueryRun model instance
    """
    run = SearchQueryRun(
        search_query_id=search_query.id,
        job_id=job_id,
        provider_used=provider_used,
        reason=reason,
        fetched_paper_ids=fetched_paper_ids,
        accepted_paper_ids=accepted_paper_ids,
        rejected_paper_ids=rejected_paper_ids,
        signal_delta=None  # Computed later during signal evaluation
    )
    
    session.add(run)
    session.flush()
    
    logger.info(
        f"Recorded SearchQueryRun: {run.id} "
        f"(query={search_query.id}, provider={provider_used}, "
        f"fetched={len(fetched_paper_ids)}, accepted={len(accepted_paper_ids)})"
    )
    
    return run
