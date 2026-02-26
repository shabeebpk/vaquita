"""Incremental semantic merging utilities.

Provides a fallback incremental update strategy for semantic graph merging:
- If no existing semantic graph -> full merge_semantically
- Otherwise, embed only new concept nodes and map them into existing canonical nodes
  when similarity >= threshold; otherwise create new canonical nodes.
- Merge edges by canonical mapping, aggregating support and provenance.

Note: This is an approximation that avoids full reclustering and re-embedding.
"""
import logging
from typing import Dict, List, Any, Tuple, Set
import numpy as np
from scipy.spatial.distance import cosine

from app.graphs.semantic import merge_semantically
from app.embeddings.factory import get_embedding_provider
from app.embeddings.cache import get_embedding_cache
from app.graphs.persistence import get_semantic_graph

logger = logging.getLogger(__name__)


def _embed_texts(provider, texts: List[str]) -> np.ndarray:
    if not texts:
        return np.array([]).reshape(0, provider.get_dimension())
    return provider.embed(texts)


def incremental_merge_semantically(
    job_id: int,
    sanitized_graph: Dict,
    embedding_provider_name: str = "sentence_transformers",
    similarity_threshold: float = None,
    **embedding_kwargs,
) -> Dict:
    """Incrementally merge `sanitized_graph` into existing semantic graph for `job_id`.

    Returns merged semantic graph dict.
    """
    # Load similarity_threshold from admin_policy if not provided
    if similarity_threshold is None:
        from app.config.admin_policy import admin_policy
        similarity_threshold = admin_policy.algorithm.graph_merging.similarity_threshold
    # Load existing semantic graph
    existing = get_semantic_graph(job_id)
    if not existing:
        logger.info("No existing semantic graph found, performing full merge")
        return merge_semantically(sanitized_graph, embedding_provider_name, similarity_threshold, **embedding_kwargs)

    # Build set of existing canonical texts and map to node dict
    existing_nodes = existing.get("nodes", [])
    canonical_texts: List[str] = []
    canonical_map: Dict[str, Dict[str, Any]] = {}
    for node in existing_nodes:
        if not isinstance(node, dict):
            continue
        text = node.get("text")
        if node.get("type") == "concept":
            canonical_texts.append(text)
            canonical_map[text] = node

    # Get concept nodes from sanitized_graph that are not already present
    concept_nodes = [n for n in sanitized_graph.get("nodes", []) if isinstance(n, dict) and n.get("type") == "concept"]
    new_nodes = [n for n in concept_nodes if n.get("text") not in set(canonical_texts)]

    if not new_nodes:
        logger.info("No new concept nodes to merge; returning existing semantic graph")
        return existing

    provider = get_embedding_provider(embedding_provider_name, **embedding_kwargs)
    emb_cache = get_embedding_cache(job_id)

    # Ensure embeddings for canonical_texts are available (cache or compute)
    canonical_embeddings = {}
    texts_to_compute = []
    for t in canonical_texts:
        # Try cache sync get? cache.get is async; but embedding provider embed is sync. Our cache is async; to keep simple, use in-memory provider directly for existing nodes and cache later.
        texts_to_compute.append(t)

    if canonical_texts:
        try:
            can_vecs = provider.embed(canonical_texts)
            for t, v in zip(canonical_texts, can_vecs):
                canonical_embeddings[t] = v
        except Exception as e:
            logger.warning(f"Failed to embed canonical nodes: {e}")
            canonical_embeddings = {}

    # Embed new nodes texts
    new_texts = [n.get("text") for n in new_nodes]
    try:
        new_vecs = provider.embed(new_texts)
    except Exception as e:
        logger.error(f"Embedding failed for new nodes: {e}")
        # Fall back to full merge
        return merge_semantically(sanitized_graph, embedding_provider_name, similarity_threshold, **embedding_kwargs)

    # Map each new node to nearest canonical if similarity >= threshold
    mapping: Dict[str, str] = {}  # new_text -> canonical_text (or itself)
    for new_text, new_vec in zip(new_texts, new_vecs):
        best_sim = -1.0
        best_can = None
        for can_text, can_vec in canonical_embeddings.items():
            try:
                sim = 1.0 - cosine(new_vec, can_vec)
            except Exception:
                sim = 0.0
            if sim > best_sim:
                best_sim = sim
                best_can = can_text
        if best_sim >= similarity_threshold and best_can:
            mapping[new_text] = best_can
            # add alias to canonical_map
            aliases = canonical_map[best_can].get("aliases", [])
            if new_text not in aliases:
                aliases.append(new_text)
                canonical_map[best_can]["aliases"] = aliases
        else:
            # create new canonical node
            mapping[new_text] = new_text
            canonical_map[new_text] = {
                "text": new_text,
                "type": "concept",
                "aliases": [],
                "attributes": new_nodes[[n.get("text") for n in new_nodes].index(new_text)].get("attributes", {}),
                "cluster_score": 1.0,
            }

    # Rebuild semantic_nodes: start from canonical_map values
    semantic_nodes = list(canonical_map.values())

    # Start from existing edges and add rewritten edges from sanitized_graph
    existing_edges = existing.get("edges", [])
    edge_dict = {}
    def _add_edge_to_dict(edge):
        key = (edge.get("subject"), edge.get("predicate"), edge.get("object"))
        if key not in edge_dict:
            edge_dict[key] = {
                "support": 0,
                "triple_ids": set(),
                "source_ids": set(),
                "block_ids": set(),
            }
        meta = edge_dict[key]
        meta["support"] += edge.get("support", 1)
        for tid in edge.get("triple_ids", []):
            meta["triple_ids"].add(tid)
        for sid in edge.get("source_ids", []):
            meta["source_ids"].add(sid)
        for bid in edge.get("block_ids", []):
            meta["block_ids"].add(bid)

    for e in existing_edges:
        _add_edge_to_dict(e)

    # Process sanitized_graph edges: map nodes via mapping if present
    for edge in sanitized_graph.get("edges", []):
        subj = edge.get("subject")
        obj = edge.get("object")
        new_subj = mapping.get(subj, subj)
        new_obj = mapping.get(obj, obj)
        # drop self-loops
        if new_subj == new_obj:
            continue
        new_edge = {
            "subject": new_subj,
            "predicate": edge.get("predicate"),
            "object": new_obj,
            "support": edge.get("support", 1),
            "triple_ids": edge.get("triple_ids", []),
            "source_ids": edge.get("source_ids", []),
            "block_ids": edge.get("block_ids", []),
        }
        _add_edge_to_dict(new_edge)

    # Rebuild edge list
    rewritten = [
        {
            "subject": s,
            "predicate": p,
            "object": o,
            "support": m["support"],
            "triple_ids": sorted(list(m["triple_ids"])),
            "source_ids": sorted(list(m["source_ids"])),
            "block_ids": sorted(list(m["block_ids"])),
        }
        for (s, p, o), m in edge_dict.items()
    ]

    logger.info(f"Incremental merge: added {len(new_nodes)} new concept(s), edges now {len(rewritten)}")

    return {
        "nodes": semantic_nodes,
        "edges": rewritten,
        "summary": {
            "added_concepts": len(new_nodes),
            "edges_after": len(rewritten),
            "merging": "incremental",
        },
    }
