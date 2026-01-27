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
import os
import logging

import networkx as nx

logger = logging.getLogger(__name__)

# Default stoplist for trivial intermediate concepts (empty by default)
DEFAULT_STOPLIST = set(["study", "result", "finding"])  # can be overridden
METADATA_NODE_TYPES = {"metadata", "citation"}


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
        else:
            G.add_edge(subj, obj, predicates=[pred], supports=[int(support)])

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


def _paths_explore(G: nx.DiGraph, max_hops: int = 2, allow_len3: bool = False) -> Iterable[List[str]]:
    """Enumerate indirect paths.

    Explore mode enumerates all A->B->C (length 2) paths. If allow_len3 is True
    and max_hops >= 3, it will also enumerate A->B->C->D paths under the same
    deterministic rules (filtering still applied later).
    """
    # length-2: A->B->C
    for mid in G.nodes:
        preds = list(G.predecessors(mid))
        succs = list(G.successors(mid))
        for a in preds:
            for c in succs:
                if a == c:
                    continue
                yield [a, mid, c]

    # optional length-3: A->B->C->D
    if allow_len3 and max_hops >= 3:
        for b in G.nodes:
            for c in G.successors(b):
                for d in G.successors(c):
                    for a in G.predecessors(b):
                        if len({a, b, c, d}) < 4:
                            continue
                        yield [a, b, c, d]


def _paths_query(G: nx.DiGraph, seeds: Set[str], alias_map: Dict[str, str], max_hops: int = 2, allow_len3: bool = False) -> Iterable[List[str]]:
    """Enumerate paths constrained by seeds (start or end matches any seed).

    Seeds may be canonical texts or alias texts; alias_map maps aliases to canonical.
    We translate seeds to canonical forms and then filter explore paths to those
    that start or end with a seed.
    """
    if not seeds:
        return []
    canonical_seeds = set()
    for s in seeds:
        canonical = alias_map.get(s, s)
        canonical_seeds.add(canonical)

    for path in _paths_explore(G, max_hops=max_hops, allow_len3=allow_len3):
        start = path[0]
        end = path[-1]
        if start in canonical_seeds or end in canonical_seeds:
            yield path


def _path_contains_bad_node(path: List[str], G: nx.DiGraph, stoplist: Set[str]) -> bool:
    """Return True if path should be discarded due to metadata/citation nodes, stoplisted intermediates, or cycles."""
    # Reject repeated nodes (cycles)
    if len(path) != len(set(path)):
        return True

    # Reject if any node is metadata or citation
    for n in path:
        ntype = G.nodes[n].get("type")
        if ntype in METADATA_NODE_TYPES:
            return True

    # For intermediate nodes (excluding first and last), reject stoplist
    for n in path[1:-1]:
        if n.lower() in stoplist:
            return True

    return False


def _direct_edge_exists(path: List[str], G: nx.DiGraph) -> bool:
    """Return True if there is already a direct edge between start and end nodes."""
    return G.has_edge(path[0], path[-1])


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

    hypothesis = {
        "source": path[0],
        "target": path[-1],
        "path": path,
        "predicates": flat_predicates,
        "explanation": explanation,
        "confidence": int(confidence),
        "mode": mode,
    }
    return hypothesis


def run_path_reasoning(
    semantic_graph: Dict,
    reasoning_mode: str = "explore",
    seeds: Optional[Iterable[str]] = None,
    max_hops: int = 2,
    allow_len3: bool = False,
    stoplist: Optional[Set[str]] = None,
) -> List[Dict]:
    """Main entrypoint for Phase-4 reasoning.

    Args:
        semantic_graph: the Phase-3 semantic graph dict (nodes, edges, summary)
        reasoning_mode: "explore" or "query"
        seeds: in query mode, an iterable of seed node texts or aliases to constrain paths
        max_hops: base hop count (2 recommended)
        allow_len3: allow enumerating length-3 paths when True
        stoplist: additional stoplisted intermediate texts (lowercased)

    Returns:
        List of hypothesis dicts.
    """
    if reasoning_mode not in {"explore", "query"}:
        raise ValueError("reasoning_mode must be 'explore' or 'query'")

    stoplist = set(s.lower() for s in (stoplist or DEFAULT_STOPLIST))

    # Convert to graph once
    G = _graph_to_nx(semantic_graph)
    alias_map = _alias_to_canonical_map(semantic_graph)

    # Choose path enumeration strategy
    if reasoning_mode == "explore":
        candidate_paths = _paths_explore(G, max_hops=max_hops, allow_len3=allow_len3)
    else:
        seed_set = set(seeds or [])
        if not seed_set:
            # No seeds provided in query mode => return empty
            logger.debug("Query mode requested but no seeds provided; returning empty list")
            return []
        candidate_paths = _paths_query(G, seed_set, alias_map, max_hops=max_hops, allow_len3=allow_len3)

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

    # Sort hypotheses by confidence desc, deterministic tie-break by source->target
    hypotheses.sort(key=lambda h: (-h["confidence"], h["source"], h["target"]))
    return hypotheses
