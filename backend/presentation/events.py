"""
Presentation Event Contract: Structured event push to Redis queue.

All pipeline phases emit their structured event here via `push_presentation_event`.
The event schema is a fixed contract consumed by the presentation worker.

Contract:
    {
        "job_id": int,
        "phase": str,       # CREATION, INGESTION, TRIPLES, GRAPH, PATHREASONING, DECISION, FETCH, DOWNLOAD
        "status": str|None, # Sub-status for DECISION: haltconfident, nohypo, found, notfound, insufficientsignal
        "result": dict,     # Phase-specific result summary
        "next_action": str|None,
        "metric": dict|None,
        "payload": dict|None,
        "error_reason": str|None,
    }
"""

import json
import logging
from typing import Optional

from app.storage.db import engine
from app.storage.models import Job
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)

# (No longer using Redis rpush directly; dispatching via Celery)



def push_presentation_event(
    job_id: int,
    phase: str,
    result: dict,
    status: str | None = None,
    next_action: str | None = None,
    metric: dict | None = None,
    payload: dict | None = None,
    error_reason: str | None = None,
    job_type: str | None = None,
) -> None:
    """Push a structured presentation event onto the Redis queue.

    The presentation worker reads from this queue asynchronously, builds
    an LLM explanation, and then pushes the enriched event to the SSE channel.

    Args:
        job_id:       The job this event belongs to.
        phase:        Pipeline phase name (e.g. "CREATION", "INGESTION").
        result:       Phase-specific result summary dict.
        status:       Optional sub-status (used mainly for DECISION phase).
        next_action:  What the pipeline will do next.
        metric:       Optional quantitative metrics dict.
        payload:      Optional rich payload (e.g. top-K hypotheses, paper list).
        error_reason: If an error occurred, a short reason string.
        job_type:     Optional job mode ('discovery' or 'verification').
    """
    # Auto-resolve job_type if not provided
    if job_id and not job_type:
        try:
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job_type = job.mode
        except Exception as e:
            logger.warning(f"Failed to resolve job_type for job {job_id}: {e}")

    event = {
        "job_id": job_id,
        "job_type": job_type,
        "phase": phase,
        "status": status,
        "result": result,
        "next_action": next_action,
        "metric": metric,
        "payload": payload,
        "error_reason": error_reason,
    }
    try:
        from presentation.worker import process_presentation_event
        process_presentation_event.apply_async(args=[event], queue='presentation')
        logger.debug(f"Dispatched presentation task: job={job_id} phase={phase} status={status}")
    except Exception as e:
        logger.warning(f"Failed to dispatch presentation task for job {job_id} phase {phase}: {e}")
