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
    # Semantic graph format: edges = [{"subject": "A", "object": "B", "predicate": "P", "source_ids": ["paper:1"], ...}]
    # Index semantic edges by (subject, object) pairs, ignoring predicate for lookup
    sem_edges_by_pair = {}
    for edge in semantic_graph.get("edges", []):
        s = getattr(edge.get("subject"), "lower", lambda: str(edge.get("subject")))().strip()
        o = getattr(edge.get("object"), "lower", lambda: str(edge.get("object")))().strip()
        if s and o:
            if (s, o) not in sem_edges_by_pair:
                sem_edges_by_pair[(s, o)] = []
            sem_edges_by_pair[(s, o)].append(edge)

    # 3. Collect all needed source_ids (to bulk query DB for metadata)
    needed_source_refs = set()
    for h in hypotheses:
        needed_source_refs.update(h.source_ids or [])

    # 4. Resolve DB papers/sources
    source_metadata = {}
    if needed_source_refs:
        from app.storage.models import Paper, File, TextBlock, IngestionSource, IngestionSourceType
        
        # 4a. Identity specific types
        paper_ids = set()
        file_ids = set()
        block_ids = set()
        
        for sid in needed_source_refs:
            s_sid = str(sid)
            if s_sid.startswith("paper:"):
                paper_ids.add(int(s_sid.split(":")[1]))
            elif s_sid.startswith("file:"):
                file_ids.add(int(s_sid.split(":")[1]))
            elif s_sid.startswith("block:"):
                block_ids.add(int(s_sid.split(":")[1]))

        with Session(engine) as session:
            # Resolve Blocks first (since they point to sources)
            if block_ids:
                blocks = session.query(TextBlock).filter(TextBlock.id.in_(list(block_ids))).all()
                for b in blocks:
                    # Map block to its ingestion source
                    source = session.query(IngestionSource).get(b.ingestion_source_id)
                    if source:
                        if source.source_type == IngestionSourceType.USER_TEXT.value:
                            source_metadata[f"block:{b.id}"] = {"type": "user_input", "title": "User provided text", "id": b.id}
                        elif source.source_ref.startswith("paper:"):
                            paper_ids.add(int(source.source_ref.split(":")[1]))
                            # We'll resolve the paper title later and map it back
                            source_metadata[f"block:{b.id}"] = {"type": "paper_ref", "ref": source.source_ref}
                        elif source.source_ref.startswith("file:"):
                            file_ids.add(int(source.source_ref.split(":")[1]))
                            source_metadata[f"block:{b.id}"] = {"type": "file_ref", "ref": source.source_ref}

            # Resolve Papers
            if paper_ids:
                papers = session.query(Paper).filter(Paper.id.in_(list(paper_ids))).all()
                for p in papers:
                    meta = {
                        "type": "paper",
                        "title": f"Fetched paper: {p.title}",
                        "url": p.pdf_url or p.doi,
                    }
                    source_metadata[f"paper:{p.id}"] = meta
                    # Back-fill any blocks pointing to this paper
                    for k, v in source_metadata.items():
                        if v.get("type") == "paper_ref" and v.get("ref") == f"paper:{p.id}":
                            source_metadata[k] = meta

            # Resolve Files
            if file_ids:
                files = session.query(File).filter(File.id.in_(list(file_ids))).all()
                for f in files:
                    meta = {
                        "type": "file",
                        "title": f"Uploaded doc: {f.original_filename}",
                    }
                    source_metadata[f"file:{f.id}"] = meta
                    # Back-fill any blocks pointing to this file
                    for k, v in source_metadata.items():
                        if v.get("type") == "file_ref" and v.get("ref") == f"file:{f.id}":
                            source_metadata[k] = meta

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
                u_norm = str(u).strip().lower()
                v_norm = str(v).strip().lower()
                
                # Check both forward and reverse directions for associated edges
                matching_edges = sem_edges_by_pair.get((u_norm, v_norm), [])
                if not matching_edges:
                    matching_edges = sem_edges_by_pair.get((v_norm, u_norm), [])
                
                # If we have matches, use the first predicate as a label, but union all sources
                p = matching_edges[0].get("predicate", "related_to") if matching_edges else "related_to"
                
                edge_key = (u, v, p)
                if edge_key not in projected_edges:
                    # Union full evidence from all semantic graph edges between these nodes
                    edge_source_ids = set()
                    triple_ids = set()
                    
                    for m_edge in matching_edges:
                        edge_source_ids.update(m_edge.get("source_ids", []))
                        triple_ids.update(m_edge.get("triple_ids", []))
                    
                    # Attach resolved metadata
                    rich_sources = []
                    for sid in edge_source_ids:
                        if sid in source_metadata:
                            rich_sources.append(source_metadata[sid])
                        else:
                            # Fallback descriptive labels
                            s_sid = str(sid)
                            if s_sid.startswith("paper"):
                                rich_sources.append({"type": "paper", "title": "Fetched paper"})
                            elif s_sid.startswith("file"):
                                rich_sources.append({"type": "file", "title": "Uploaded doc"})
                            else:
                                rich_sources.append({"type": "user_text", "title": "User text"})
                    
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


def resolve_triple_evidence_text(triple_ids: List[int], session: Session) -> List[str]:
    """Fetch raw text blocks for the given triple IDs.
    
    This is used to provide concrete evidence snippets in the final presentation.
    """
    from app.storage.models import Triple, TextBlock
    if not triple_ids:
        return []
    
    # Filter out non-integers if any
    clean_ids = [tid for tid in triple_ids if isinstance(tid, int)]
    if not clean_ids:
        return []

    # Triple -> TextBlock -> block_text
    # We join Triple and TextBlock to ensure we only get text for the specific triples.
    blocks = (
        session.query(TextBlock.block_text)
        .join(Triple, Triple.block_id == TextBlock.id)
        .filter(Triple.id.in_(clean_ids))
        .distinct()
        .all()
    )
    return [b.block_text for b in blocks if b.block_text]


def _extract_intermediates(path: list) -> list:
    """
    Extract intermediate nodes from a hypothesis path.
    
    A path is [source, intermediate1, ..., intermediateN, target].
    Intermediates are all nodes except the first and last.
    
    Example:
        path = ["t", "j", "b"]  -> intermediates = ["j"]
        path = ["a", "b", "c"]  -> intermediates = ["b"]
    """
    if not path or len(path) <= 2:
        return []
    return path[1:-1]


def group_top_hypotheses(
    hypotheses: List[Dict],
    limit: int,
    exclude_pair: tuple = None  # (source, target) of dominant pair to exclude from top-K
) -> List[Dict]:
    """
    Group hypotheses by (source, target) pair, including both passed and promising.
    
    Scoring:
    - Passed: 1.0 points
    - Promising (low-confidence rejection only): 0.5 points
    
    For each pair, collects deduplicated intermediates from all paths.
    
    Returns top `limit` pairs sorted by pair_score desc, then max_confidence desc.
    Each entry has: { source, target, intermediates }
    """
    from app.path_reasoning.filtering.logic import is_low_confidence_rejection
    
    pairs = {}
    
    for h in hypotheses:
        is_passed = h.get("passed_filter", False)
        is_promising = is_low_confidence_rejection(h)
        
        if not (is_passed or is_promising):
            continue
            
        src = h.get("source")
        tgt = h.get("target")
        if not src or not tgt:
            continue
            
        pair_key = (src, tgt)
        
        # Skip excluded pair (dominant) if specified
        if exclude_pair and pair_key == tuple(exclude_pair):
            continue
        
        if pair_key not in pairs:
            pairs[pair_key] = {
                "source": src,
                "target": tgt,
                "pair_score": 0.0,
                "max_confidence": 0.0,
                "intermediates_set": set(),  # deduplicated intermediate nodes
            }
        
        # Accumulate score
        score = 1.0 if is_passed else 0.5
        pairs[pair_key]["pair_score"] += score
        
        # Track max confidence for sorting tiebreaks
        conf = h.get("confidence", 0)
        if conf > pairs[pair_key]["max_confidence"]:
            pairs[pair_key]["max_confidence"] = conf
        
        # Collect intermediates from this path
        path = h.get("path", [])
        for node in _extract_intermediates(path):
            if node:
                pairs[pair_key]["intermediates_set"].add(node)
    
    # Build clean output list
    results = []
    for pair_data in pairs.values():
        results.append({
            "source": pair_data["source"],
            "target": pair_data["target"],
            "intermediates": sorted(pair_data["intermediates_set"]),  # sorted for determinism
            # Internal scoring fields for sorting (stripped before sending)
            "_pair_score": pair_data["pair_score"],
            "_max_confidence": pair_data["max_confidence"],
        })
    
    # Sort by pair_score desc, then max_confidence desc
    results.sort(key=lambda x: (-x["_pair_score"], -x["_max_confidence"]))
    
    # Return top-K, stripping internal scoring fields
    output = []
    for r in results[:limit]:
        output.append({
            "source": r["source"],
            "target": r["target"],
            "intermediates": r["intermediates"],
        })
    return output


def get_dominant_pair(
    hypotheses: List[Dict],
    dominant_pair_ids: list,  # e.g. ["openai", "thought"]
) -> Dict:
    """
    Build a clean {source, target, intermediates} entry for the dominant pair.
    
    Collects all intermediate nodes across every hypothesis for that (source, target) pair.
    """
    if not dominant_pair_ids or len(dominant_pair_ids) < 2:
        return {}
    
    src, tgt = dominant_pair_ids[0], dominant_pair_ids[1]
    intermediates_set = set()
    
    for h in hypotheses:
        if h.get("source") == src and h.get("target") == tgt:
            path = h.get("path", [])
            for node in _extract_intermediates(path):
                if node:
                    intermediates_set.add(node)
    
    return {
        "source": src,
        "target": tgt,
        "intermediates": sorted(intermediates_set),
    }

