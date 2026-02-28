"""
Phase-4: Path Reasoning & Hypothesis Discovery

Implements deterministic path enumeration (explore/query), strict rule-based
filtering, hypothesis construction and deterministic scoring. Operates only on the
persisted Phase-3 semantic graph dict and never mutates earlier artifacts.

Public API:
- run_path_reasoning(semantic_graph: dict, reasoning_mode: "explore"|"query", **opts) -> List[dict]

Notes:
- conversion uses networkx.DiGraph (multiple predicates per node-pair are stored
  as lists on the edge attributes)
- scoring uses per-hop best-evidence (max support among predicates for that hop)
  and the hypothesis confidence is the minimum across hops (weakest link)
"""
from __future__ import annotations

from typing import Dict, List, Tuple, Iterable, Optional, Set
import logging
import json

import networkx as nx
import numpy as np
from scipy.spatial.distance import cosine

from app.config.admin_policy import admin_policy
from app.path_reasoning.filtering.logic import apply_hub_suppression_to_graph
from app.embeddings.factory import get_embedding_provider

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Synchronous Redis embedding cache
# Cache key: emb:<text>  value: JSON list of floats
# Reuses the same Redis URL as the rest of the app.
# Falls back silently to re-embedding if Redis is unavailable.
# ---------------------------------------------------------------------------
_redis_sync = None

def _get_redis_sync():
    """Lazily create a sync Redis client (redis-py, not asyncio)."""
    global _redis_sync
    if _redis_sync is not None:
        return _redis_sync
    try:
        import redis as _redis_lib
        from app.config.system_settings import system_settings
        _redis_sync = _redis_lib.from_url(system_settings.REDIS_URL, decode_responses=True)
        _redis_sync.ping()  # Verify connection
        logger.debug("Reasoning: sync Redis client connected for embedding cache")
    except Exception as e:
        logger.debug(f"Reasoning: Redis unavailable for embedding cache: {e}")
        _redis_sync = None
    return _redis_sync


_EMBED_CACHE_PREFIX = "reasoning:emb:"
_EMBED_CACHE_TTL = 7 * 24 * 3600  # 7 days


def _cached_embed(text: str) -> Optional[np.ndarray]:
    """Embed text, using Redis to avoid re-embedding the same string."""
    provider = get_embedding_provider()
    r = _get_redis_sync()
    key = f"{_EMBED_CACHE_PREFIX}{text}"

    # Try cache hit
    if r:
        try:
            cached = r.get(key)
            if cached:
                return np.array(json.loads(cached), dtype=np.float32)
        except Exception:
            pass

    # Cache miss — embed and store
    try:
        vector = np.array(provider.embed([text])[0], dtype=np.float32)
    except Exception as e:
        logger.warning(f"Reasoning: failed to embed '{text}': {e}")
        return None

    if r:
        try:
            r.set(key, json.dumps(vector.tolist()), ex=_EMBED_CACHE_TTL)
        except Exception:
            pass

    return vector


def _graph_to_nx(semantic_graph: Dict) -> nx.DiGraph:
    """Convert Phase-3 semantic graph dict into a networkx.DiGraph.

    Each node uses its `text` value as the node identifier. Node attributes are
    preserved (type, attributes, aliases, cluster_score, etc.). For edges, we
    aggregate predicates/supports between the same (subject, object) pair into
    lists stored on the edge attributes: `predicates` and `supports`.
    """
    G = nx.DiGraph()

    # Add nodes
    for node in semantic_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        text = node.get("text")
        if not text:
            continue
        # Copy attributes except 'text'
        node_attrs = {k: v for k, v in node.items() if k != "text"}
        G.add_node(text, **node_attrs)

    # Add edges with aggregated predicates/supports per node pair
    for edge in semantic_graph.get("edges", []):
        subj = edge.get("subject")
        obj = edge.get("object")
        pred = edge.get("predicate")
        support = edge.get("support", 1)

        if subj is None or obj is None or pred is None:
            continue

        if G.has_edge(subj, obj):
            data = G.edges[subj, obj]
            data.setdefault("predicates", []).append(pred)
            data.setdefault("supports", []).append(int(support))
            data.setdefault("triple_ids_list", []).append(edge.get("triple_ids", []))
            data.setdefault("source_ids_list", []).append(edge.get("source_ids", []))
            data.setdefault("block_ids_list", []).append(edge.get("block_ids", []))
        else:
            G.add_edge(
                subj, obj, 
                predicates=[pred], 
                supports=[int(support)],
                triple_ids_list=[edge.get("triple_ids", [])],
                source_ids_list=[edge.get("source_ids", [])],
                block_ids_list=[edge.get("block_ids", [])]
            )

    return G


def _alias_to_canonical_map(semantic_graph: Dict) -> Dict[str, str]:
    """Build a mapping from alias text -> canonical node text for lookups.
    A canonical node's primary text maps to itself as well.
    """
    m: Dict[str, str] = {}
    for node in semantic_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        text = node.get("text")
        if not text:
            continue
        m[text] = text
        for a in node.get("aliases", []) or []:
            m[a] = text
    return m


def _path_contains_bad_node(path: List[str], G: nx.DiGraph, stoplist: Set[str]) -> bool:
    """Return True if path should be discarded due to stoplisted intermediates or cycles.

    Note: forbidden node type check (entity/metadata/citation/url) is done BEFORE
    path reasoning via prune_forbidden_nodes — not here.
    """
    # Reject repeated nodes (cycles)
    if len(path) != len(set(path)):
        return True

    # For intermediate nodes (excluding first and last), reject stoplist
    for n in path[1:-1]:
        if n.lower() in stoplist:
            return True

    return False


def _direct_edge_exists(path: List[str], G: nx.DiGraph) -> bool:
    """Return True if there is already a direct edge between start and end nodes."""
    return G.has_edge(path[0], path[-1])


def _is_valid_discovery_path(path: List[str], G: nx.DiGraph) -> bool:
    """Validate path for Discovery Mode (strict causal-chain rules).
    
    Accepts A → B → C ONLY if:
    - A → B exists
    - B → C exists
    - A ≠ C (different nodes)
    - No A → C (no forward shortcut)
    - No C → A (no backward shortcut)
    - No A ← B (no bidirectional A-B)
    - No B ← C (no bidirectional B-C)
    
    Rejects common-cause (A ← B → C), collider (A → B ← C), cascade (A ← B ← C).
    """
    if len(path) != 3:
        return True  # Only validate 3-node paths
    
    A, B, C = path[0], path[1], path[2]
    
    # Rule 1: A ≠ C
    if A == C:
        return False
    
    # Rule 2: A must directly connect to B (A → B)
    if not G.has_edge(A, B):
        return False
    
    # Rule 3: B must directly connect to C (B → C)
    if not G.has_edge(B, C):
        return False
    
    # Rule 4: A must NOT directly connect to C in ANY direction
    if G.has_edge(A, C):  # A → C (forward shortcut)
        return False
    if G.has_edge(C, A):  # C → A (backward shortcut)
        return False
    
    # Rule 5: No bidirectional A ↔ B (no A ← B)
    if G.has_edge(B, A):  # B → A would create A ← B
        return False
    
    # Rule 6: No bidirectional B ↔ C (no B ← C)
    if G.has_edge(C, B):  # C → B would create B ← C
        return False
    
    return True


def _get_discovery_mode_paths(G: nx.DiGraph) -> Iterable[List[str]]:
    """Optimized path enumeration for Discovery Mode (3-node paths only).
    
    Directly checks edge patterns instead of enumerating all paths.
    Much faster than all_simple_paths() for large graphs.
    
    Yields valid A → B → C paths where:
    - A directly connects to B
    - B directly connects to C
    - No shortcuts, bidirectionals, or invalid patterns
    """
    # 3-node paths: A → B → C
    for B in G.nodes():
        # Get all predecessors of B (nodes that have edges TO B)
        for A in G.predecessors(B):
            # Get all successors of B (nodes that have edges FROM B)
            for C in G.successors(B):
                path = [A, B, C]
                if _is_valid_discovery_path(path, G):
                    yield path


def _check_verification_connection(source: str, target: str, G: nx.DiGraph) -> Tuple[bool, Optional[str], Optional[List[str]]]:
    """Check if source and target are connected in Verification Mode (permissive).
    
    Returns: (is_connected, path_type, path)
    path_type: 'direct_forward', 'direct_reverse', '2hop_forward', '2hop_reverse',
               'common_cause', 'collider', or None
    path: list of node names if connected, None otherwise
    
    This checks patterns in order of specificity:
    1. Direct edges (A → C or C → A)
    2. 2-hop paths (A → X → C or C → X → A)
    3. Non-strict patterns (common-cause, collider)
    """
    if source not in G or target not in G:
        return False, None, None
    
    # 1. Direct connections
    if G.has_edge(source, target):
        return True, 'direct_forward', [source, target]
    
    if G.has_edge(target, source):
        return True, 'direct_reverse', [target, source]
    
    # 2. 2-hop forward: A → X → C
    for intermediate in G.successors(source):
        if G.has_edge(intermediate, target):
            return True, '2hop_forward', [source, intermediate, target]
    
    # 3. 2-hop reverse: C → X → A
    for intermediate in G.successors(target):
        if G.has_edge(intermediate, source):
            return True, '2hop_reverse', [target, intermediate, source]
    
    # 4. Common-cause: A ← B → C (means B → A and B → C)
    for intermediate in G.nodes():
        if intermediate != source and intermediate != target:
            if G.has_edge(intermediate, source) and G.has_edge(intermediate, target):
                return True, 'common_cause', [source, intermediate, target]
    
    # 5. Collider: A → B ← C (means A → B and C → B)
    for intermediate in G.nodes():
        if intermediate != source and intermediate != target:
            if G.has_edge(source, intermediate) and G.has_edge(target, intermediate):
                return True, 'collider', [source, intermediate, target]
    
    return False, None, None


def _edge_strength_for_hop(u: str, v: str, G: nx.DiGraph) -> int:
    """Determine a single support value for an edge hop (u->v).

    If multiple predicates/supports exist for the pair, take the maximum support
    (best evidence) to represent that hop deterministically.
    """
    data = G.edges.get((u, v), None)
    if not data:
        return 0
    supports = data.get("supports", [])
    if not supports:
        return 0
    return max(int(s) for s in supports)


def _predicates_along_path(path: List[str], G: nx.DiGraph) -> List[List[str]]:
    """Collect predicates lists for each hop on the path, preserving order."""
    preds = []
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        data = G.edges.get((u, v), {})
        preds.append(list(data.get("predicates", [])))
    return preds


def _build_hypothesis(path: List[str], G: nx.DiGraph, mode: str) -> Dict:
    """Construct a hypothesis object from a valid path.

    Hypothesis fields:
    - source: start node text
    - target: end node text
    - path: list of node texts (including intermediates)
    - predicates: flattened list of predicates along path (in hop order)
    - explanation: human-readable explanation describing the indirect connection
    - confidence: integer (min support among hops)
    - mode: reasoning_mode that produced this hypothesis
    """
    # Compute per-hop strengths and overall confidence=min
    hop_strengths = [
        _edge_strength_for_hop(path[i], path[i + 1], G) for i in range(len(path) - 1)
    ]
    confidence = min(hop_strengths) if hop_strengths else 0

    predicates_per_hop = _predicates_along_path(path, G)
    # Flatten predicates for summary view, while preserving hop grouping in explanation
    flat_predicates = [p for hop in predicates_per_hop for p in hop]

    # Build explanation
    explanation_parts = []
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        preds = predicates_per_hop[i]
        part = f"{u} -[{', '.join(preds)}]-> {v}" if preds else f"{u} -> {v}"
        explanation_parts.append(part)
    explanation = " then ".join(explanation_parts)

    # Aggregate traceability IDs
    triple_ids = set()
    source_ids = set()
    block_ids = set()
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        data = G.edges.get((u, v), {})
        for ids in data.get("triple_ids_list", []):
            triple_ids.update(ids)
        for ids in data.get("source_ids_list", []):
            source_ids.update(ids)
        for ids in data.get("block_ids_list", []):
            block_ids.update(ids)

    hypothesis = {
        "source": path[0],
        "target": path[-1],
        "path": path,
        "predicates": flat_predicates,
        "explanation": explanation,
        "confidence": int(confidence),
        "mode": mode,
        "triple_ids": sorted(list(triple_ids)),
        "source_ids": sorted(list(source_ids)),
        "block_ids": sorted(list(block_ids)),
    }
    return hypothesis


def run_path_reasoning(
    semantic_graph: Dict,
    reasoning_mode: str = "explore",
    seeds: Optional[Iterable[str]] = None,
    stoplist: Optional[Set[str]] = None,
    preferred_predicates: Optional[List[str]] = None,
    preferred_predicate_boost_factor: float = 1.2,
) -> List[Dict]:
    """Main entrypoint for Phase-4 reasoning (3-node paths only).

    Args:
        semantic_graph: the Phase-3 semantic graph dict (nodes, edges, summary)
        reasoning_mode: "explore" or "query"
        seeds: in query mode, an iterable of seed node texts or aliases to constrain paths
        stoplist: additional stoplisted intermediate texts (lowercased)
        preferred_predicates: optional list of canonical predicate labels to boost in scoring
        preferred_predicate_boost_factor: multiplier for confidence when preferred predicates found

    Returns:
        List of hypothesis dicts (sorted by confidence desc).
    """
    if reasoning_mode not in {"explore", "query"}:
        raise ValueError("reasoning_mode must be 'explore' or 'query'")

    stoplist = set(s.lower() for s in (stoplist or []))
    preferred_predicates = set(p.lower() for p in (preferred_predicates or []))
    preferred_predicate_boost_factor = max(1.0, min(2.0, float(preferred_predicate_boost_factor)))

    # Convert to graph
    G = _graph_to_nx(semantic_graph)
    
    # Apply hub suppression before path enumeration
    hub_threshold = admin_policy.algorithm.path_reasoning_defaults.hub_degree_threshold
    G = apply_hub_suppression_to_graph(G, hub_threshold)
    
    alias_map = _alias_to_canonical_map(semantic_graph)

    # Discovery Mode: Use optimized strict path enumeration (3-node paths only)
    # Explore mode: enumerate all valid causal chains
    # Query mode: constrain to seed-related causal chains
    if reasoning_mode == "explore":
        candidate_paths = _get_discovery_mode_paths(G)
    elif reasoning_mode == "query":
        seed_set = set(seeds or [])
        if not seed_set:
            # No seeds provided in query mode => return empty
            logger.debug("Query mode requested but no seeds provided; returning empty list")
            return []
        # Filter discovery paths to only those involving seeds
        all_paths = _get_discovery_mode_paths(G)
        candidate_paths = (p for p in all_paths if p[0] in seed_set or p[-1] in seed_set)
    else:
        raise ValueError(f"reasoning_mode must be 'explore' or 'query', got '{reasoning_mode}'")

    hypotheses: List[Dict] = []
    seen_hypotheses: Set[Tuple[str, str, Tuple[str, ...]]] = set()

    for path in candidate_paths:
        # Deterministic filtering
        if _path_contains_bad_node(path, G, stoplist):
            continue
        if _direct_edge_exists(path, G):
            continue

        # Build hypothesis
        hyp = _build_hypothesis(path, G, reasoning_mode)

        key = (hyp["source"], hyp["target"], tuple(hyp["path"]))
        if key in seen_hypotheses:
            continue
        seen_hypotheses.add(key)
        hypotheses.append(hyp)

    # Apply preferred_predicates boost (increase confidence if contains preferred predicates)
    if preferred_predicates:
        for hyp in hypotheses:
            path_preds = set(p.lower() for p in hyp.get("predicates", []))
            if path_preds & preferred_predicates:
                # Boost confidence by multiplier (capped at 100)
                hyp["confidence"] = min(int(hyp["confidence"] * preferred_predicate_boost_factor), 100)
                logger.debug(
                    f"Boosted hypothesis {hyp['source']} -> {hyp['target']} "
                    f"confidence by {preferred_predicate_boost_factor}x (preferred predicates found)"
                )

    # Phase 4.5: Apply filtering (permanent rules reject entirely)
    from app.path_reasoning.filtering.logic import filter_hypotheses, resolve_domains_batch
    from app.llm import get_llm_service
    
    passed_hypotheses, failed_hypotheses = filter_hypotheses(hypotheses, semantic_graph)
    
    logger.info(f"Filtering: {len(passed_hypotheses)} passed, {len(failed_hypotheses)} rejected permanently")
    
    if not passed_hypotheses:
        logger.debug("No hypotheses passed filtering")
        return []
    
    # Phase 4.6: Batch domain resolution (only on passed hypotheses)
    llm_client = get_llm_service()
    final_hypotheses = resolve_domains_batch(passed_hypotheses, llm_client)
    
    # Sort hypotheses by confidence desc, deterministic tie-break by source->target
    final_hypotheses.sort(key=lambda h: (-h["confidence"], h["source"], h["target"]))
    return final_hypotheses


def _collect_supporting_papers(path: List[str], G: nx.DiGraph) -> List[Dict]:
    """Collect supporting papers (source IDs) from all edges in a path.
    
    Args:
        path: List of node texts forming the path
        G: NetworkX DiGraph
    
    Returns:
        List of dicts with {paper_id, triple_ids, predicates}
    """
    papers_map: Dict[int, Dict] = {}
    
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        edge_data = G.edges.get((u, v), {})
        
        # Collect source IDs from this edge
        for source_id_list in edge_data.get("source_ids_list", []):
            for paper_id in source_id_list:
                if paper_id not in papers_map:
                    papers_map[paper_id] = {
                        "paper_id": paper_id,
                        "triple_ids": [],
                        "predicates": [],
                    }
                papers_map[paper_id]["predicates"].extend(edge_data.get("predicates", []))
        
        # Collect triple IDs
        for triple_id_list in edge_data.get("triple_ids_list", []):
            for triple_id in triple_id_list:
                if papers_map:
                    first_paper = next(iter(papers_map.values()))
                    if triple_id not in first_paper["triple_ids"]:
                        first_paper["triple_ids"].append(triple_id)
    
    return list(papers_map.values())


def _find_closest_node(text: str, semantic_graph: Dict, threshold: Optional[float] = None) -> str:
    """Find the closest node text in the semantic graph using embeddings.
    
    If the text directly matches a canonical node or alias, it is returned.
    Otherwise, we use cosine similarity with semantic node embeddings to find the best match.
    """
    if threshold is None:
        threshold = admin_policy.algorithm.decision_thresholds.semantic_similarity_threshold
    # 1. Try exact match (including aliases)
    m = _alias_to_canonical_map(semantic_graph)
    if text in m:
        return m[text]
        
    # 2. Semantic lookup — embed query using Redis-backed cache
    text_emb = _cached_embed(text)
    if text_emb is None:
        return text
        
    best_node = None
    best_sim = -1.0
    
    nodes = semantic_graph.get("nodes", [])
    for node in nodes:
        if node.get("type") != "concept":
            continue
            
        node_emb = node.get("embedding")
        if not node_emb:
            # Fallback: embed node text via cache too
            node_text = node.get("text", "")
            node_emb_arr = _cached_embed(node_text)
            if node_emb_arr is None:
                continue
            node_emb = node_emb_arr
                
        # distance = cosine(text_emb, node_emb), sim = 1 - distance
        sim = 1.0 - cosine(text_emb, np.asarray(node_emb, dtype=np.float32))
        if sim > best_sim:
            best_sim = sim
            best_node = node.get("text")
            
    if best_sim >= threshold and best_node:
        logger.debug(f"Resolved '{text}' -> '{best_node}' via semantic similarity ({best_sim:.3f})")
        return best_node
        
    return text


def run_path_reasoning_verification(
    semantic_graph: Dict,
    source: str,
    target: str,
    stoplist: Optional[Set[str]] = None,
) -> Dict:
    """Verification mode path reasoning: check if source and target are connected.
    
    This function is used in verification mode to determine if two entities
    (source and target) are connected in the knowledge graph.
    
    Strategy:
    1. Check for direct edge (A->C or C->A, treated as undirected)
    2. If not found, check for indirect paths of exactly length 3 (A-B-C or C-B-A)
    3. Return simple result: {found: bool, type: 'direct'|'indirect'|null, path: [...], ...}
    
    Args:
        semantic_graph: The Phase-3 semantic graph dict
        source: Source entity text to search from
        target: Target entity text to search to
        stoplist: Optional set of stoplisted intermediate nodes
    
    Returns:
        Dict with keys:
        - found (bool): Whether a connection exists
        - type (str or null): 'direct' or 'indirect' if found, null otherwise
        - path (list or null): The path if found, null otherwise
        - supporting_papers (list): Contributing papers/edges
        - explanation (str): Human-readable explanation
    """
    stoplist = set(s.lower() for s in (stoplist or []))
    
    # Convert to networkx graph
    G = _graph_to_nx(semantic_graph)
    
    # Resolve aliases and semantic similarity to canonical forms
    similarity_threshold = admin_policy.algorithm.decision_thresholds.semantic_similarity_threshold
    canonical_source = _find_closest_node(source, semantic_graph, threshold=similarity_threshold)
    canonical_target = _find_closest_node(target, semantic_graph, threshold=similarity_threshold)
    
    logger.debug(f"Verification: checking connection {canonical_source} <-> {canonical_target}")
    
    # Check if nodes exist in graph
    if canonical_source not in G:
        logger.debug(f"Source node '{canonical_source}' not in graph")
        return {
            "found": False,
            "type": None,
            "path": None,
            "supporting_papers": [],
            "explanation": f"Source '{source}' not found in knowledge graph"
        }
    
    if canonical_target not in G:
        logger.debug(f"Target node '{canonical_target}' not in graph")
        return {
            "found": False,
            "type": None,
            "path": None,
            "supporting_papers": [],
            "explanation": f"Target '{target}' not found in knowledge graph"
        }
    
    # Verification Mode: Use optimized connection checking
    # Checks in order: direct edges, 2-hop paths, common-cause, collider
    is_connected, path_type, path = _check_verification_connection(canonical_source, canonical_target, G)
    
    if not is_connected:
        logger.info(f"Verification: NOT FOUND connection {canonical_source} <-> {canonical_target}")
        return {
            "found": False,
            "type": None,
            "path": None,
            "supporting_papers": [],
            "explanation": f"No connection found between {source} and {target}"
        }
    
    # Check stoplist on intermediate nodes (if path has length > 2)
    if len(path) > 2 and _path_contains_bad_node(path, G, stoplist):
        logger.info(f"Verification: Connection {canonical_source} <-> {canonical_target} found but rejected (stoplist)")
        return {
            "found": False,
            "type": None,
            "path": None,
            "supporting_papers": [],
            "explanation": f"Connection found but rejected due to stoplisted intermediates"
        }
    
    # Build explanation with predicates
    supporting_papers = _collect_supporting_papers(path, G)
    explanation_parts = []
    for i in range(len(path) - 1):
        u = path[i]
        v = path[i + 1]
        edge_data = G.edges.get((u, v), {})
        preds = edge_data.get("predicates", [])
        part = f"{u} -[{', '.join(preds)}]-> {v}" if preds else f"{u} -> {v}"
        explanation_parts.append(part)
    explanation = " -> ".join(explanation_parts)
    
    logger.info(f"Verification: FOUND connection {canonical_source} <-> {canonical_target} via {path_type}: {' -> '.join(path)}")
    return {
        "found": True,
        "type": path_type,
        "path": path,
        "supporting_papers": supporting_papers,
        "explanation": explanation
    }
