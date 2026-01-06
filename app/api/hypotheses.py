from fastapi import APIRouter, HTTPException, Depends
from typing import List
import logging

from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Job
from app.graphs.persistence import get_semantic_graph
from app.path_reasoning.persistence import (
    get_hypotheses,
    create_reasoning_query,
    persist_hypotheses,
    delete_all_hypotheses_for_job,
)
from app.path_reasoning.reasoning import run_path_reasoning
from app.schemas.hypotheses import ExploreResponse, QueryRequest, QueryResponse, HypothesisOut

router = APIRouter(prefix="/jobs", tags=["hypotheses"])
logger = logging.getLogger(__name__)


@router.get("/{job_id}/hypotheses/explore", response_model=ExploreResponse)
def get_explore_hypotheses(job_id: int):
    """Return precomputed explore hypotheses for a job. Read-only; never recomputes."""
    with Session(engine) as session:
        job = session.get(Job, job_id)
        if not job:
            raise HTTPException(status_code=404, detail="job not found")
        if job.status != "PATH_REASONING_DONE":
            raise HTTPException(status_code=409, detail="path reasoning not completed for this job")

    # Fetch hypotheses in explore mode only
    rows = get_hypotheses(job_id=job_id, limit=1000, offset=0)
    # Filter to mode == "explore"
    explore_rows = [r for r in rows if r.get("mode") == "explore"]

    return ExploreResponse(job_id=job_id, hypotheses=explore_rows)


@router.post("/{job_id}/hypotheses/query", response_model=QueryResponse)
def post_query_hypotheses(job_id: int, req: QueryRequest):
    """Run a one-off query-mode reasoning run constrained by seeds extracted from `query_text`.

    This endpoint:
    - loads the persisted semantic graph
    - inserts a reasoning_queries row
    - runs the reasoning engine in query mode (deterministic)
    - persists resulting hypotheses with mode='query' and query_id set
    - returns the hypotheses

    This endpoint is read-only with respect to earlier pipeline phases and does not change job.status.
    """
    # Load semantic graph
    semantic_graph = get_semantic_graph(job_id)
    if semantic_graph is None:
        raise HTTPException(status_code=404, detail="semantic graph not found for job")

    # Create reasoning query row
    qid = create_reasoning_query(job_id=job_id, query_text=req.query_text)

    # Simple seed extraction: match semantic node texts or aliases occurring in the query text
    qlow = req.query_text.lower()
    seeds = set()
    for node in semantic_graph.get("nodes", []):
        text = node.get("text")
        if not text:
            continue
        if text.lower() in qlow:
            seeds.add(text)
        for a in node.get("aliases", []) or []:
            if a.lower() in qlow:
                seeds.add(a)

    if not seeds:
        # No matching seeds -> return empty result but keep query row for provenance
        return QueryResponse(query_id=qid, hypotheses=[])

    # Run reasoning
    hyps = run_path_reasoning(
        semantic_graph,
        reasoning_mode="query",
        seeds=seeds,
        max_hops=req.max_hops or 2,
        allow_len3=bool(req.allow_len3),
    )

    # Phase-4.5: Filtering
    from app.path_reasoning.filtering import filter_hypotheses
    hyps = filter_hypotheses(hyps, semantic_graph)

    # SINGLE-ACTIVE-STATE: Delete all existing hypotheses for this job before inserting fresh ones
    # This maintains the principle that only one hypothesis set exists per job at any time
    deleted_count = delete_all_hypotheses_for_job(job_id)
    
    # Persist hypotheses with query_id (all of them, including rejected)
    persist_hypotheses(job_id=job_id, hypotheses=hyps, query_id=qid)

    # Return results (convert to HypothesisOut shapes as returned by persist/get)
    # Use the in-memory hyps list; add query_id and job_id fields to match schema
    out = []
    for h in hyps:
        out.append({
            "id": None,
            "job_id": job_id,
            "source": h.get("source"),
            "target": h.get("target"),
            "path": h.get("path"),
            "predicates": h.get("predicates"),
            "explanation": h.get("explanation"),
            "confidence": h.get("confidence"),
            "mode": h.get("mode"),
            "mode": h.get("mode"),
            "query_id": qid,
            "passed_filter": h.get("passed_filter", False),
            "filter_reason": h.get("filter_reason"),
            "created_at": None,
        })

    # Return only filtered (passed) hypotheses to the user
    out = [h for h in out if h["passed_filter"]]

    return QueryResponse(query_id=qid, hypotheses=out)