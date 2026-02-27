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
from app.config.job_config import JobConfig

logger = logging.getLogger(__name__)


class QueryOrchestratorConfig:
    """Configuration for query orchestration."""
    
    def __init__(self, job_config: Optional[JobConfig] = None):
        """
        Initialize config from JobConfig or AdminPolicy.
        
        Args:
            job_config: Optional[JobConfig] instance.
        """
        from app.config.admin_policy import admin_policy
        qo = admin_policy.query_orchestrator

        # If job_config is passed as a dict for legacy reasons, convert it
        if job_config and isinstance(job_config, dict):
            job_config = JobConfig(**job_config)
        
        # Max signature length for hypothesis_signature
        self.signature_length = int(qo.signature_length)
        
        # Initial reputation score for new queries
        self.initial_reputation = int(qo.initial_reputation)
        
        logger.debug(
            f"QueryOrchestratorConfig loaded from AdminPolicy: "
            f"signature_len={self.signature_length}, "
            f"initial_rep={self.initial_reputation}"
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
    focus_areas: list = None,
    config: Optional[QueryOrchestratorConfig] = None
) -> SearchQuery:
    """
    Get existing SearchQuery by hypothesis_signature or create new one.
    
    Args:
        hypothesis: Hypothesis dict
        job_id: Job ID
        session: SQLAlchemy session
        query_text: Optional custom query text (derived from hypothesis if empty)
        focus_areas: Optional list of keywords to inject into query (AND/OR logic)
        config: QueryOrchestratorConfig (created if None)
    
    Returns:
        SearchQuery model instance
    """
    if config is None:
        config = QueryOrchestratorConfig()
    
    focus_areas = focus_areas or []
    
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
        logger.info(f"Query without foucs areas: {query_text}")
    
    # Inject focus areas: AND/OR query expansion
    if focus_areas:
        focus_str = " OR ".join(focus_areas)
        query_text = f"({query_text}) AND ({focus_str})"
        logger.info(f"Enhanced query with focus_areas: {query_text}")

    # Inherit domain from hypothesis (Domain Resolution Contract)
    resolved_domain = hypothesis.get("domain")
    
    # Capture current configuration snapshot
    config_snapshot = {
        "signature_length": config.signature_length,
        "initial_reputation": config.initial_reputation,
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
    Determine if a SearchQuery should be run based on status.
    
    Simple rule: Only 'new' queries should run. Once a query is run, it becomes 'done'.
    
    Args:
        search_query: SearchQuery model instance
        session: SQLAlchemy session
        config: QueryOrchestratorConfig (created if None)
    
    Returns:
        Tuple of (should_run: bool, reason: str)
    """
    if config is None:
        config = QueryOrchestratorConfig()
    
    # Only new queries run
    if search_query.status == "new":
        return True, "Ready to execute"
    
    # All other statuses (done) should not run
    return False, f"Query already executed (status={search_query.status})"


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
    from app.storage.models import JobPaperEvidence
    
    result = session.query(JobPaperEvidence.paper_id).filter(
        JobPaperEvidence.job_id == job_id
    ).all()
    
    if not result:
        return []
        
    return [row.paper_id for row in result]


def record_search_run(
    search_query: SearchQuery,
    job_id: int,
    provider_used: str,
    reason: str,  # 'initial_attempt', 'reuse', 'expansion'
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
        # Paper IDs moved to JobPaperEvidence (Strategic Ledger)
        signal_delta=None  # Computed later during signal evaluation
    )
    
    session.add(run)
    session.flush()
    
    logger.info(
        f"Recorded SearchQueryRun: {run.id} "
        f"(query={search_query.id}, provider={provider_used})"
    )
    
    return run


def update_search_query_status(
    search_query: SearchQuery,
    session: Session
):
    """
    Mark a SearchQuery as 'done' after execution.
    
    Args:
        search_query: SearchQuery model instance
        session: SQLAlchemy session
    """
    old_status = search_query.status
    search_query.status = "done"
    
    if old_status != search_query.status:
        logger.info(f"SearchQuery {search_query.id} status updated: {old_status} -> done")


def compute_entities_hash(entities: list) -> str:
    """Compute hash of entities for deduplication check.
    
    Different orderings ([a,c] vs [c,a]) are treated as different.
    
    Args:
        entities: List of entity strings
    
    Returns:
        Hex hash string
    """
    entity_str = "|".join(str(e).lower() for e in entities)
    hash_obj = hashlib.sha256(entity_str.encode("utf-8"))
    return hash_obj.hexdigest()[:16]


def check_entities_duplicate(
    job_id: int,
    entities: list,
    session: Session
) -> bool:
    """Check if same entities have already been used for search in this job.
    
    Args:
        job_id: Job ID
        entities: List of entity strings
        session: SQLAlchemy session
    
    Returns:
        True if duplicate found, False otherwise
    """
    entities_hash = compute_entities_hash(entities)
    
    existing = session.query(SearchQuery).filter(
        SearchQuery.job_id == job_id,
        SearchQuery.entities_hash == entities_hash
    ).first()
    
    return existing is not None


def create_verification_search_queries(
    job_id: int,
    source: str,
    target: str,
    session: Session,
    config: Optional[QueryOrchestratorConfig] = None,
    resolved_domain: Optional[str] = None,
) -> list:
    """
    Create search queries for verification mode.
    
    Verification mode hierarchy:
    1. Start with [source, target] combined ([A,C])
    2. Then try [source] alone ([A])
    3. Then try [target] alone ([C])
    
    Args:
        job_id: Job ID
        source: Source entity
        target: Target entity
        session: SQLAlchemy session
        config: QueryOrchestratorConfig
        resolved_domain: Domain for search (from config or null for default)
    
    Returns:
        List of SearchQuery instances created
    """
    if config is None:
        config = QueryOrchestratorConfig()
    
    queries = []
    
    # Capture current config snapshot
    config_snapshot = {
        "signature_length": config.signature_length,
        "initial_reputation": config.initial_reputation,
        "timestamp": datetime.utcnow().isoformat()
    }
    
    # Strategy 1: Combined [source, target]
    entities_combined = [source, target]
    if not check_entities_duplicate(job_id, entities_combined, session):
        query_text = f"relationship between {source} and {target}"
        entities_hash = compute_entities_hash(entities_combined)
        
        sq = SearchQuery(
            job_id=job_id,
            hypothesis_signature=hashlib.sha256(f"{source}→{target}".encode()).hexdigest()[:16],
            query_text=query_text,
            resolved_domain=resolved_domain,
            status="new",
            reputation_score=config.initial_reputation,
            config_snapshot=config_snapshot,
            entities_used=entities_combined,
            entities_hash=entities_hash,
        )
        session.add(sq)
        queries.append(sq)
        logger.info(f"Created verification query: {query_text} (entities={entities_combined})")
    else:
        logger.debug(f"Skipped duplicate entities: {entities_combined}")
    
    # Strategy 2: Source alone [A]
    entities_source = [source]
    if not check_entities_duplicate(job_id, entities_source, session):
        query_text = f"related to {source}"
        entities_hash = compute_entities_hash(entities_source)
        
        sq = SearchQuery(
            job_id=job_id,
            hypothesis_signature=hashlib.sha256(f"{source}_single".encode()).hexdigest()[:16],
            query_text=query_text,
            resolved_domain=resolved_domain,
            status="new",
            reputation_score=config.initial_reputation,
            config_snapshot=config_snapshot,
            entities_used=entities_source,
            entities_hash=entities_hash,
        )
        session.add(sq)
        queries.append(sq)
        logger.info(f"Created verification query: {query_text} (entities={entities_source})")
    else:
        logger.debug(f"Skipped duplicate entities: {entities_source}")
    
    # Strategy 3: Target alone [C]
    entities_target = [target]
    if not check_entities_duplicate(job_id, entities_target, session):
        query_text = f"related to {target}"
        entities_hash = compute_entities_hash(entities_target)
        
        sq = SearchQuery(
            job_id=job_id,
            hypothesis_signature=hashlib.sha256(f"{target}_single".encode()).hexdigest()[:16],
            query_text=query_text,
            resolved_domain=resolved_domain,
            status="new",
            reputation_score=config.initial_reputation,
            config_snapshot=config_snapshot,
            entities_used=entities_target,
            entities_hash=entities_hash,
        )
        session.add(sq)
        queries.append(sq)
        logger.info(f"Created verification query: {query_text} (entities={entities_target})")
    else:
        logger.debug(f"Skipped duplicate entities: {entities_target}")
    
    return queries
