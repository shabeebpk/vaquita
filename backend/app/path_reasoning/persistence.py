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
from typing import List, Dict, Optional, Set, Any

from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Hypothesis
from app.storage.models import ReasoningQuery

logger = logging.getLogger(__name__)


def deactivate_hypotheses_for_job(job_id: int, affected_nodes: Set[str] = None, modes: List[str] = None) -> int:
    """
    Deactivate hypotheses (soft delete for versioning).
    
    Args:
        job_id: The job ID.
        affected_nodes: Optional set of new node texts. If provided, only
                       hypotheses touching these nodes are deactivated.
        modes: Optional list of modes to target (e.g., ["explore"]).
               If None, targets all modes.
    
    Returns:
        Number of hypotheses deactivated.
    """
    with Session(engine) as session:
        query = session.query(Hypothesis).filter(
            Hypothesis.job_id == job_id,
            Hypothesis.is_active == True
        )
        
        if modes:
            query = query.filter(Hypothesis.mode.in_(modes))
        
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
    Persist hypotheses with Full Snapshot versioning.

    Every call creates a NEW version containing the COMPLETE set of hypotheses
    for that state. Old rows are marked is_active=FALSE. Existing hypotheses
    re-inserted with the new version number will reuse cached domain/explanation.

    Args:
        job_id: The job ID.
        hypotheses: The complete list of hypotheses for the current graph state.
        query_id: Optional query ID.
        affected_nodes: Optional set of new nodes. Used to tag which ones are new.

    Returns:
        Number of rows inserted.
    """
    from app.llm import get_llm_service
    from app.domains.resolver import resolve_domain
    from app.storage.models import Job
    
    if not hypotheses:
        return 0

    # 1. Determine modes in this batch (usually all "explore" or all "query")
    batch_modes = list(set(h.get("mode", "explore") for h in hypotheses))
    
    llm_client = get_llm_service()
    inserted = 0
    
    with Session(engine) as session:
        # 2. Extract job config
        job = session.query(Job).filter(Job.id == job_id).first()
        job_config = job.job_config if job else {}
        
        # 3. Determine next version
        max_version_record = session.query(Hypothesis.version).filter(
            Hypothesis.job_id == job_id
        ).order_by(Hypothesis.version.desc()).first()
        next_version = (max_version_record[0] + 1) if max_version_record else 1
        
        # 4. Cache existing active hypotheses to reuse expensive domain resolution
        # Key: (source, target, tuple(path)) -> domain
        # This prevents redundant LLM calls when "copying forward" unaffected rows.
        existing_active = session.query(Hypothesis).filter(
            Hypothesis.job_id == job_id,
            Hypothesis.is_active == True,
            Hypothesis.mode.in_(batch_modes)
        ).all()
        
        domain_cache = {}
        for row in existing_active:
            key = (row.source, row.target, tuple(row.path or []))
            domain_cache[key] = row.domain

        # 5. Deactivate current active set for these modes
        deactivate_hypotheses_for_job(job_id, modes=batch_modes)

        # 6. Insert full snapshot
        for h in hypotheses:
            source = h.get("source")
            target = h.get("target")
            path = h.get("path", [])
            key = (source, target, tuple(path))
            
            # Reuse domain if possible
            domain = h.get("domain") or domain_cache.get(key)
            if not domain:
                domain = resolve_domain(h, job_config, llm_client)
            
            # Identify affected nodes in this specific hypothesis
            path_nodes = set(path)
            hypothesis_affected = list(path_nodes & affected_nodes) if affected_nodes else None
            
            row = Hypothesis(
                job_id=job_id,
                source=source,
                target=target,
                path=path,
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


def project_hypotheses_to_graph(job_id: int, semantic_graph: Dict, version: Optional[int] = None) -> Dict:
    """
    Project hypotheses into a plottable graph structure.
    
    Constraints:
    - Never artificially connect 'source' and 'target' directly unless the path is a 1-hop edge.
    - Edges are extracted strictly from the 'path' and 'predicates' arrays.
    - For each edge in the path, lookup the underlying structural evidence from the semantic graph.
    - Resolve DB source_ids (e.g., paper:123) into actual paper titles/urls for rich edge details.
    
    Args:
        job_id: The job ID.
        semantic_graph: The full semantic graph dict (to lookup full edge evidence).
        version: Specific hypothesis version to load. If None, loads all active hypotheses.
        
    Returns:
        Dict: {"nodes": [...], "edges": [...]} suitable for UI rendering.
    """
    from app.storage.models import Paper, IngestionSource
    
    # 1. Fetch hypotheses
    with Session(engine) as session:
        query = session.query(Hypothesis).filter(Hypothesis.job_id == job_id)
        if version is not None:
            query = query.filter(Hypothesis.version == version)
        else:
            query = query.filter(Hypothesis.is_active == True)
            
        hypotheses = query.all()
        
    if not hypotheses:
        return {"nodes": [], "edges": []}

    # 2. Build quick-lookup for semantic edges
    # Semantic graph format: edges = [{"source": "A", "target": "B", "predicate": "P", "source_ids": ["paper:1"], ...}]
    # Index semantic edges by (source, target) pairs, ignoring predicate for lookup
    sem_edges_by_pair = {}
    for edge in semantic_graph.get("edges", []):
        s = getattr(edge.get("source"), "lower", lambda: str(edge.get("source")))()
        t = getattr(edge.get("target"), "lower", lambda: str(edge.get("target")))()
        if s and t:
            if (s, t) not in sem_edges_by_pair:
                sem_edges_by_pair[(s, t)] = []
            sem_edges_by_pair[(s, t)].append(edge)

    # 3. Collect all needed source_ids (to bulk query DB for metadata)
    needed_source_refs = set()
    for h in hypotheses:
        needed_source_refs.update(h.source_ids or [])

    # 4. Resolve DB papers/sources
    source_metadata = {}
    if needed_source_refs:
        paper_ids = [int(str(sid).split(":")[1]) for sid in needed_source_refs if str(sid).startswith("paper:")]
        if paper_ids:
            with Session(engine) as session:
                papers = session.query(Paper).filter(Paper.id.in_(paper_ids)).all()
                for p in papers:
                    source_metadata[f"paper:{p.id}"] = {
                        "type": "paper",
                        "id": p.id,
                        "title": p.title,
                        "url": p.pdf_url or p.doi,
                        "year": p.year
                    }

    # 5. Build projected nodes and edges
    projected_nodes = {}
    projected_edges = {}

    for h in hypotheses:
        path = h.path or []
        
        # Add nodes
        for node_str in path:
            if node_str not in projected_nodes:
                projected_nodes[node_str] = {"id": node_str, "label": node_str}

        # Add edges strictly following the path
        if len(path) > 1:
            for i in range(len(path) - 1):
                u = path[i]
                v = path[i+1]
                
                u_lower = getattr(u, "lower", lambda: str(u))()
                v_lower = getattr(v, "lower", lambda: str(v))()
                
                # Check both forward and reverse directions for associated edges
                matching_edges = sem_edges_by_pair.get((u_lower, v_lower), [])
                if not matching_edges:
                    matching_edges = sem_edges_by_pair.get((v_lower, u_lower), [])
                
                # If we have matches, use the first predicate as a label, but union all sources
                p = matching_edges[0].get("predicate", "related_to") if matching_edges else "related_to"
                
                edge_key = (u, v, p)
                if edge_key not in projected_edges:
                    # Union full evidence from all semantic graph edges between these nodes
                    edge_source_ids = set()
                    triple_ids = set()
                    
                    for edge in matching_edges:
                        edge_source_ids.update(edge.get("source_ids", []))
                        triple_ids.update(edge.get("triple_ids", []))
                    
                    # Attach resolved metadata
                    rich_sources = []
                    for sid in edge_source_ids:
                        if sid in source_metadata:
                            rich_sources.append(source_metadata[sid])
                        else:
                            rich_sources.append({"type": "unknown", "ref": sid})
                    
                    projected_edges[edge_key] = {
                        "source": u,
                        "target": v,
                        "predicate": p,
                        "source_ids": list(edge_source_ids),
                        "source_metadata": rich_sources,
                        "triple_ids": list(triple_ids),
                        # Track which hypotheses use this edge
                        "used_in_hypotheses": [h.id]
                    }
                else:
                    if h.id not in projected_edges[edge_key]["used_in_hypotheses"]:
                        projected_edges[edge_key]["used_in_hypotheses"].append(h.id)

    return {
        "nodes": list(projected_nodes.values()),
        "edges": list(projected_edges.values())
    }


def get_job_papers(job_id: int, session: Session) -> List[Dict[str, Any]]:
    """Collect all papers fetched for this job from JobPaperEvidence."""
    from app.storage.models import JobPaperEvidence, Paper
    from app.config.admin_policy import admin_policy
    
    rows = (
        session.query(JobPaperEvidence, Paper)
        .join(Paper, JobPaperEvidence.paper_id == Paper.id)
        .filter(JobPaperEvidence.job_id == job_id)
        .all()
    )
    papers = []
    
    # Get config for snippet length, fallback to 300
    snippet_len = admin_policy.algorithm.decision_thresholds.abstract_snippet_length
    
    for evidence, paper in rows:
        papers.append({
            "paper_id": paper.id,
            "title": paper.title,
            "url": paper.pdf_url or paper.doi,
            "abstract_snippet": (paper.abstract or "")[:snippet_len] if paper.abstract else None,
            "evaluated": evidence.evaluated,
            "impact_score": evidence.impact_score
        })
    return papers


def group_top_hypotheses(hypotheses: List[Dict], limit: int) -> List[Dict]:
    """
    Group passed hypotheses by (source, target) pair.
    For each pair, collect indirect paths, max confidence, and total support.
    Returns the top `limit` pairs sorted by confidence.
    """
    pairs = {}
    
    # Only consider hypotheses that passed the structural filters
    passed = [h for h in hypotheses if h.get("passed_filter", False)]
    
    for h in passed:
        src = h.get("source")
        tgt = h.get("target")
        if not src or not tgt:
            continue
            
        pair_key = (src, tgt)
        if pair_key not in pairs:
            pairs[pair_key] = {
                "source": src,
                "target": tgt,
                "max_confidence": 0,
                "indirect_paths": [],
                "total_supporting_papers": set(),
                "mode": h.get("mode", "explore"),
                "domain": h.get("domain", "")
            }
            
        # Update max confidence
        conf = h.get("confidence", 0)
        if conf > pairs[pair_key]["max_confidence"]:
            pairs[pair_key]["max_confidence"] = conf
            
        # Add path details
        pairs[pair_key]["indirect_paths"].append({
            "hypothesis_id": h.get("id"),
            "path": h.get("path", []),
            "predicates": h.get("predicates", []),
            "confidence": conf,
            "explanation": h.get("explanation", "")
        })
        
        # Aggregate paper support
        for sid in h.get("source_ids", []):
            if str(sid).startswith("paper:"):
                pairs[pair_key]["total_supporting_papers"].add(str(sid))
                
    # Format and sort
    results = []
    for pair_data in pairs.values():
        pair_data["total_supporting_papers_count"] = len(pair_data["total_supporting_papers"])
        pair_data["total_supporting_papers"] = list(pair_data["total_supporting_papers"])
        # Sort paths within the pair by confidence
        pair_data["indirect_paths"].sort(key=lambda x: x["confidence"], reverse=True)
        results.append(pair_data)
        
    results.sort(key=lambda x: x["max_confidence"], reverse=True)
    return results[:limit]
