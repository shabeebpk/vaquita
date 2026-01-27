"""Phase-3: Semantic merging (concept consolidation via embeddings).

This module reads a Phase-2.5 sanitized graph, filters concept nodes,
vectorizes them, clusters similar concepts, selects canonical labels,
and produces a new semantic graph with merged nodes.

No re-processing of earlier phases. No LLMs. Only embeddings and clustering.
"""
import logging
from typing import Dict, List, Tuple, Set, Any
import numpy as np
from scipy.spatial.distance import cosine
from sklearn.cluster import AgglomerativeClustering

from app.embeddings.factory import get_embedding_provider
from app.embeddings.interface import EmbeddingProvider

logger = logging.getLogger(__name__)


def _filter_concept_nodes(sanitized_graph: Dict) -> Tuple[List[Dict], Dict[str, int]]:
    """Filter nodes to include only concepts suitable for merging.

    Returns:
        (list of concept nodes, dict mapping node.text -> index)
    """
    nodes = sanitized_graph.get("nodes", [])
    concept_nodes = []
    node_to_idx = {}

    for node in nodes:
        if not isinstance(node, dict):
            continue

        node_type = node.get("type")
        node_text = node.get("text", "")

        # Only merge concept nodes; exclude entity, metadata, citation, noise
        if node_type != "concept":
            continue

        # Skip very short or malformed nodes
        if not node_text or len(node_text.strip()) < 2:
            continue

        # Skip if already suspicious (pure numbers, URLs, etc.)
        if node_text.isdigit() or "://" in node_text:
            continue

        node_to_idx[node_text] = len(concept_nodes)
        concept_nodes.append(node)

    logger.info(f"Filtered {len(concept_nodes)} concept nodes for merging")
    return concept_nodes, node_to_idx


def _vectorize_concepts(
    concept_nodes: List[Dict],
    embedding_provider: EmbeddingProvider,
) -> np.ndarray:
    """Vectorize concept node texts.

    Returns:
        numpy array of shape (len(concept_nodes), embedding_dim)
    """
    texts = [node.get("text", "") for node in concept_nodes]
    vectors = embedding_provider.embed(texts)
    logger.info(f"Vectorized {len(texts)} concept nodes using {embedding_provider.get_name()}")
    return vectors


def _cluster_concepts(
    vectors: np.ndarray,
    similarity_threshold: float = 0.85,
    linkage: str = "average",
) -> np.ndarray:
    """Cluster concept vectors using agglomerative clustering.

    Args:
        vectors: embedding vectors (N, D)
        similarity_threshold: cosine similarity threshold (0.85 = safe, 0.90 = strict)
        linkage: clustering linkage method

    Returns:
        cluster labels array (N,)
    """
    if len(vectors) <= 1:
        return np.array([0] * len(vectors))

    # Convert similarity threshold to distance threshold
    # distance = 1 - cosine_similarity, so threshold distance = 1 - similarity_threshold
    distance_threshold = 1.0 - similarity_threshold

    clustering = AgglomerativeClustering(
        n_clusters=None,
        distance_threshold=distance_threshold,
        linkage=linkage,
        metric="cosine",
    )
    labels = clustering.fit_predict(vectors)
    logger.info(f"Clustered {len(vectors)} vectors into {len(np.unique(labels))} clusters")
    return labels


def _select_canonical_labels(
    concept_nodes: List[Dict],
    cluster_labels: np.ndarray,
    graph_edges: List[Dict],
) -> Dict[int, Tuple[str, List[str]]]:
    """Select canonical label for each cluster.

    Strategy: shortest phrase; ties broken by highest node degree in original graph.

    Returns:
        dict mapping cluster_id -> (canonical_text, [aliases])
    """
    clusters: Dict[int, List[int]] = {}
    for idx, cluster_id in enumerate(cluster_labels):
        if cluster_id not in clusters:
            clusters[cluster_id] = []
        clusters[cluster_id].append(idx)

    # Build degree map (how many edges reference each node)
    degree_map = {}
    for node in concept_nodes:
        text = node.get("text", "")
        count = sum(
            1 for edge in graph_edges
            if edge.get("subject") == text or edge.get("object") == text
        )
        degree_map[text] = count

    canonical_map = {}
    for cluster_id, indices in clusters.items():
        if not indices:
            continue

        nodes_in_cluster = [concept_nodes[i] for i in indices]
        texts = [n.get("text", "") for n in nodes_in_cluster]

        # Sort by: shortest first, then highest degree
        sorted_texts = sorted(
            texts,
            key=lambda t: (len(t), -degree_map.get(t, 0)),
        )

        canonical = sorted_texts[0]
        aliases = [t for t in sorted_texts if t != canonical]

        canonical_map[cluster_id] = (canonical, aliases)
        logger.debug(f"Cluster {cluster_id}: canonical='{canonical}', aliases={aliases}")

    return canonical_map


def _rewrite_edges(
    original_edges: List[Dict],
    node_to_idx: Dict[str, int],
    cluster_labels: np.ndarray,
    canonical_map: Dict[int, Tuple[str, List[str]]],
) -> List[Dict]:
    """Rewrite edges to point to canonical nodes using strict index-safe mapping.

    Mapping flow: original node text → index in concept list → cluster label → canonical.
    Predicates are never modified or interpreted semantically.
    If both endpoints collapse into the same canonical node, the edge is dropped.
    If multiple edges collapse, their support is summed.

    Returns:
        list of rewritten edges (all predicates preserved unchanged)
    """
    # Build strict index-safe mapping: node_text -> index -> cluster_id -> canonical
    text_to_canonical = {}
    for node_text, idx in node_to_idx.items():
        cluster_id = cluster_labels[idx]  # strict: use positional index
        canonical, _ = canonical_map[cluster_id]
        text_to_canonical[node_text] = canonical

    # Rewrite edges with support aggregation, preserving all predicates
    edge_dict: Dict[Tuple[str, str, str], int] = {}  # (subject, predicate, object) -> support
    for edge in original_edges:
        subj = edge.get("subject", "")
        pred = edge.get("predicate", "")  # preserve predicate as-is (never reinterpreted)
        obj = edge.get("object", "")
        support = edge.get("support", 1)

        # Rewrite to canonical nodes (or keep if not in merged set)
        new_subj = text_to_canonical.get(subj, subj)
        new_obj = text_to_canonical.get(obj, obj)

        # Drop self-loops only; multiple predicates and neighbors per node are correct
        if new_subj == new_obj:
            logger.debug(f"Dropped self-loop: {new_subj} -[{pred}]-> {new_obj}")
            continue

        key = (new_subj, pred, new_obj)
        edge_dict[key] = edge_dict.get(key, 0) + support

    # Rebuild edge list
    rewritten = [
        {
            "subject": s,
            "predicate": p,
            "object": o,
            "support": supp,
        }
        for (s, p, o), supp in edge_dict.items()
    ]

    logger.info(f"Rewrote {len(original_edges)} edges into {len(rewritten)} after merging")
    return rewritten


def merge_semantically(
    sanitized_graph: Dict,
    embedding_provider_name: str = "sentence-transformers",
    similarity_threshold: float = 0.85,
    **embedding_kwargs,
) -> Dict:
    """Perform Phase-3 semantic merging.

    Reads a Phase-2.5 sanitized graph, merges semantically similar concept nodes,
    and produces a new semantic graph.

    Args:
        sanitized_graph: Phase-2.5 output
        embedding_provider_name: which embedding backend to use
        similarity_threshold: clustering threshold (0.85 = safe, 0.90 = strict)
        **embedding_kwargs: additional args for embedding provider

    Returns:
        semantic graph dict
    """
    # Step 1: Filter to concept nodes only
    concept_nodes, node_to_idx = _filter_concept_nodes(sanitized_graph)

    if not concept_nodes:
        logger.warning("No concept nodes to merge; returning empty semantic graph")
        return {
            "nodes": [],
            "edges": sanitized_graph.get("edges", []),
            "summary": {
                "concept_nodes_filtered": 0,
                "clusters": 0,
                "nodes_merged": 0,
                "edges_rewritten": 0,
            },
        }

    # Step 2: Vectorize concepts
    provider = get_embedding_provider(embedding_provider_name, **embedding_kwargs)
    vectors = _vectorize_concepts(concept_nodes, provider)

    # Step 3: Cluster
    cluster_labels = _cluster_concepts(vectors, similarity_threshold=similarity_threshold)

    # Step 4: Select canonical labels
    canonical_map = _select_canonical_labels(
        concept_nodes,
        cluster_labels,
        sanitized_graph.get("edges", []),
    )

    # Step 5: Build semantic nodes with aliases and correct cluster scores
    semantic_nodes = []
    for cluster_id, (canonical_text, aliases) in canonical_map.items():
        # Find the original node dict for the canonical
        orig_node = None
        for node in concept_nodes:
            if node.get("text") == canonical_text:
                orig_node = node
                break

        if orig_node is None:
            orig_node = concept_nodes[0]  # fallback

        # Compute cluster score: average cosine similarity between members and centroid
        member_indices = [i for i in range(len(concept_nodes)) if cluster_labels[i] == cluster_id]
        if member_indices:
            member_vectors = vectors[member_indices]  # shape (cluster_size, dim)
            centroid = np.mean(member_vectors, axis=0)  # shape (dim,)
            centroid_norm = np.linalg.norm(centroid)
            if centroid_norm > 0:
                centroid = centroid / centroid_norm  # normalize
            # Cosine similarity: dot product of normalized vectors
            similarities = np.dot(member_vectors, centroid)
            cluster_score = float(np.mean(similarities))
        else:
            cluster_score = 1.0  # single-node cluster has perfect score

        semantic_nodes.append({
            "text": canonical_text,
            "type": "concept",
            "aliases": aliases,
            "attributes": orig_node.get("attributes", {}),
            "cluster_score": cluster_score,  # semantic tightness: 0–1
        })

    # Add non-concept nodes as-is
    for node in sanitized_graph.get("nodes", []):
        if not isinstance(node, dict):
            continue
        if node.get("type") != "concept":
            semantic_nodes.append(node)

    # Step 6: Rewrite edges
    semantic_edges = _rewrite_edges(
        sanitized_graph.get("edges", []),
        node_to_idx,
        cluster_labels,
        canonical_map,
    )

    logger.info(
        f"Phase-3 semantic merging complete: {len(concept_nodes)} concepts → "
        f"{len(canonical_map)} clusters → {len(semantic_nodes)} semantic nodes"
    )

    return {
        "nodes": semantic_nodes,
        "edges": semantic_edges,
        "summary": {
            "concept_nodes_filtered": len(concept_nodes),
            "clusters_formed": len(canonical_map),
            "nodes_merged": len(concept_nodes) - len(canonical_map),
            "edges_before": len(sanitized_graph.get("edges", [])),
            "edges_after": len(semantic_edges),
            "embedding_provider": provider.get_name(),
            "similarity_threshold": similarity_threshold,
            "provenance": "aliases list on canonical nodes; aggregated support on merged edges",
        },
    }
