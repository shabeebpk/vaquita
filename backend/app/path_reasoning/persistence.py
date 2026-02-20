"""
Persistence helpers for Phase-4 hypotheses with versioning support.

Provides functions to persist hypothesis rows with versioning:
- Only one is_active=TRUE set per job (single-active-state)
- All versions kept for audit trail
- Old versions marked is_active=FALSE
- Domain calculation only for NEW hypotheses (not cached ones)
"""
from datetime import datetime
import logging
from typing import List, Dict, Optional, Set

from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Hypothesis
from app.storage.models import ReasoningQuery

logger = logging.getLogger(__name__)


def deactivate_hypotheses_for_job(job_id: int, affected_nodes: Set[str] = None) -> int:
    """
    Deactivate hypotheses affected by new nodes (soft delete for versioning).
    
    If affected_nodes provided, only deactivate hypotheses using those nodes.
    Otherwise, deactivate all active hypotheses.
    
    Args:
        job_id: The job ID.
        affected_nodes: Optional set of new node texts.
    
    Returns:
        Number of hypotheses deactivated.
    """
    with Session(engine) as session:
        query = session.query(Hypothesis).filter(
            Hypothesis.job_id == job_id,
            Hypothesis.is_active == True
        )
        
        if affected_nodes:
            hypotheses = query.all()
            count = 0
            for h in hypotheses:
                path_nodes = set(h.path or [])
                if path_nodes & affected_nodes:
                    h.is_active = False
                    h.affected_by_nodes = list(path_nodes & affected_nodes)
                    count += 1
            session.commit()
        else:
            count = query.update({Hypothesis.is_active: False}, synchronize_session=False)
            session.commit()
        
        if count > 0:
            logger.info(f"Deactivated {count} hypotheses for job {job_id}")
        
        return count


def delete_all_hypotheses_for_job(job_id: int) -> int:
    """
    Delete all hypotheses (legacy - now soft deletes with versioning).
    
    Args:
        job_id: The job ID.
    
    Returns:
        Number of hypotheses deactivated.
    """
    return deactivate_hypotheses_for_job(job_id)


def persist_hypotheses(job_id: int, hypotheses: List[Dict], query_id: Optional[int] = None, affected_nodes: Set[str] = None) -> int:
    """
    Persist hypotheses with versioning.

    Deactivates affected hypotheses, inserts new ones with is_active=TRUE.
    Domain calculation ONLY for new hypotheses (not pre-calculated ones).

    Args:
        job_id: The job ID.
        hypotheses: List of hypothesis dicts.
        query_id: Optional query ID.
        affected_nodes: Set of new nodes triggering rebuild (optional).

    Returns:
        Number of rows inserted.
    """
    from app.llm import get_llm_service
    from app.domains.resolver import resolve_domain
    from app.storage.models import Job
    
    if affected_nodes:
        deactivate_hypotheses_for_job(job_id, affected_nodes)
    else:
        deactivate_hypotheses_for_job(job_id)
    
    llm_client = get_llm_service()
    inserted = 0
    
    with Session(engine) as session:
        job = session.query(Job).filter(Job.id == job_id).first()
        job_config = job.job_config if job else {}
        
        max_version_record = session.query(Hypothesis.version).filter(
            Hypothesis.job_id == job_id
        ).order_by(Hypothesis.version.desc()).first()
        next_version = (max_version_record[0] + 1) if max_version_record else 1
        
        for h in hypotheses:
            # Domain Resolution: ONLY for new hypotheses
            if "domain" not in h or h.get("domain") is None:
                domain = resolve_domain(h, job_config, llm_client)
            else:
                domain = h.get("domain")
            
            # Store only the affected nodes this specific hypothesis touches (avoid redundancy)
            path_nodes = set(h.get("path", []))
            hypothesis_affected = list(path_nodes & affected_nodes) if affected_nodes else None
            
            row = Hypothesis(
                job_id=job_id,
                source=h.get("source"),
                target=h.get("target"),
                path=h.get("path", []),
                predicates=h.get("predicates", []),
                explanation=h.get("explanation", ""),
                domain=domain,
                confidence=int(h.get("confidence", 0)),
                mode=h.get("mode", "explore"),
                query_id=query_id,
                passed_filter=h.get("passed_filter", False),
                filter_reason=h.get("filter_reason", None),
                triple_ids=h.get("triple_ids", []),
                source_ids=h.get("source_ids", []),
                block_ids=h.get("block_ids", []),
                version=next_version,
                is_active=True,
                affected_by_nodes=hypothesis_affected,
                created_at=datetime.utcnow(),
            )
            session.add(row)
            inserted += 1
        session.commit()
    
    from app.path_reasoning.filtering.logic import calculate_impact_scores
    with Session(engine) as session:
        calculate_impact_scores(job_id, hypotheses, session)
    logger.info(f"Persisted {inserted} hypotheses for job {job_id} and updated impact scores.")
    return inserted


def get_hypotheses(job_id: int, limit: int = 100, offset: int = 0, include_rejected: bool = True) -> List[Dict]:
    """Fetch active hypotheses for a job for UI listing.

    Returns a list of dicts (only is_active=TRUE).
    """
    with Session(engine) as session:
        query = (
            session.query(Hypothesis)
            .filter(Hypothesis.job_id == job_id, Hypothesis.is_active == True)
        )
        
        if not include_rejected:
            query = query.filter(Hypothesis.passed_filter == True)
            
        rows = (
            query.order_by(Hypothesis.confidence.desc(), Hypothesis.created_at.desc())
            .limit(limit)
            .offset(offset)
            .all()
        )
        result = []
        for r in rows:
            result.append({
                "id": r.id,
                "job_id": r.job_id,
                "source": r.source,
                "target": r.target,
                "path": r.path,
                "predicates": r.predicates,
                "explanation": r.explanation,
                "domain": r.domain,
                "confidence": r.confidence,
                "mode": r.mode,
                "query_id": r.query_id,
                "passed_filter": r.passed_filter,
                "filter_reason": r.filter_reason,
                "source_ids": r.source_ids,
                "triple_ids": r.triple_ids,
                "block_ids": r.block_ids,
                "version": r.version,
                "created_at": r.created_at.isoformat() if r.created_at else None,
            })
        return result


def create_reasoning_query(job_id: int, query_text: str) -> int:
    """Insert a reasoning_queries row and return its id."""
    with Session(engine) as session:
        rq = ReasoningQuery(job_id=job_id, query_text=query_text, created_at=datetime.utcnow())
        session.add(rq)
        session.commit()
        session.refresh(rq)
        return rq.id
