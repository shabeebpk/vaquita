"""
Persistence helpers for Phase-4 hypotheses.

Provides functions to persist hypothesis rows and query them for UI.

Design: Single-active-state per job - only one set of hypotheses exists per job_id at any time.
On each hypothesis generation run, all existing hypotheses for the job are deleted before
new ones are inserted. This ensures no versioning is needed and storage overhead is minimal.
"""
from datetime import datetime
import logging
from typing import List, Dict, Optional

from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Hypothesis
from app.storage.models import ReasoningQuery

logger = logging.getLogger(__name__)


def delete_all_hypotheses_for_job(job_id: int) -> int:
    """
    Delete all existing hypotheses for a job.
    
    Part of single-active-state model: called before persist_hypotheses
    to ensure only one hypothesis set exists per job at any time.
    
    Args:
        job_id: The job ID.
    
    Returns:
        Number of hypotheses deleted.
    """
    with Session(engine) as session:
        count = session.query(Hypothesis).filter(
            Hypothesis.job_id == job_id
        ).delete(synchronize_session=False)
        session.commit()
        
        if count > 0:
            logger.info(f"Deleted {count} existing hypotheses for job {job_id} (preparing for fresh generation)")
        
        return count


def persist_hypotheses(job_id: int, hypotheses: List[Dict], query_id: Optional[int] = None) -> int:
    """Persist a list of hypothesis dicts as rows.

    Implements single-active-state model: assumes all old hypotheses for this job
    have already been deleted. Inserts fresh hypotheses for this generation run.

    Each hypothesis dict is expected to contain keys:
    - source, target, path (list), predicates (list), explanation (str), confidence (int), mode (str)

    Returns the number of rows inserted.
    """
    from app.llm import get_llm_service
    from app.domains.resolver import resolve_domain
    from app.storage.models import Job
    
    llm_client = get_llm_service()
    
    inserted = 0
    with Session(engine) as session:
        # Load job config for override check
        job = session.query(Job).filter(Job.id == job_id).first()
        job_config = job.job_config if job else {}
        
        for h in hypotheses:
            # Domain Resolution Contract: runs once before persistence
            domain = resolve_domain(h, job_config, llm_client)
            
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
                created_at=datetime.utcnow(),
            )
            session.add(row)
            inserted += 1
        session.commit()
    logger.info(f"Persisted {inserted} hypotheses for job {job_id}")
    return inserted


def get_hypotheses(job_id: int, limit: int = 100, offset: int = 0, include_rejected: bool = True) -> List[Dict]:
    """Fetch hypotheses for a job for UI listing.

    Returns a list of dicts.
    """
    with Session(engine) as session:
        query = (
            session.query(Hypothesis)
            .filter(Hypothesis.job_id == job_id)
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
                "mode": r.mode,
                "query_id": r.query_id,
                "passed_filter": r.passed_filter,
                "filter_reason": r.filter_reason,
                "source_ids": r.source_ids,
                "triple_ids": r.triple_ids,
                "block_ids": r.block_ids,
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
