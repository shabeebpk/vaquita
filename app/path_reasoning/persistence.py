"""
Persistence helpers for Phase-4 hypotheses.

Provides functions to persist hypothesis rows and query them for UI.
"""
from datetime import datetime
import logging
from typing import List, Dict, Optional

from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Hypothesis
from app.storage.models import ReasoningQuery

logger = logging.getLogger(__name__)


def persist_hypotheses(job_id: int, hypotheses: List[Dict], query_id: Optional[int] = None) -> int:
    """Persist a list of hypothesis dicts as rows.

    Each hypothesis dict is expected to contain keys:
    - source, target, path (list), predicates (list), explanation (str), confidence (int), mode (str)

    Returns the number of rows inserted.
    """
    inserted = 0
    with Session(engine) as session:
        for h in hypotheses:
            row = Hypothesis(
                job_id=job_id,
                source=h.get("source"),
                target=h.get("target"),
                path=h.get("path", []),
                predicates=h.get("predicates", []),
                explanation=h.get("explanation", ""),
                confidence=int(h.get("confidence", 0)),
                mode=h.get("mode", "explore"),
                query_id=query_id,
                passed_filter=h.get("passed_filter", False),
                filter_reason=h.get("filter_reason", None),
                created_at=datetime.utcnow(),
            )
            session.add(row)
            inserted += 1
        session.commit()
    logger.info(f"Persisted {inserted} hypotheses for job {job_id}")
    return inserted


def get_hypotheses(job_id: int, limit: int = 100, offset: int = 0, include_rejected: bool = False) -> List[Dict]:
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
                "source": r.source,
                "target": r.target,
                "path": r.path,
                "predicates": r.predicates,
                "explanation": r.explanation,
                "confidence": r.confidence,
                "mode": r.mode,
                "mode": r.mode,
                "query_id": r.query_id,
                "passed_filter": r.passed_filter,
                "filter_reason": r.filter_reason,
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
