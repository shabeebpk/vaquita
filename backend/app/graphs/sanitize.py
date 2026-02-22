"""Graph sanitization (Phase 2.5): node classification and noise removal.

Reads a Phase-2 structural graph and removes nodes classified as noise
by the config-driven classify_node rules. All surviving nodes are concepts.

Input:
  {"nodes": [...], "edges": [{subject, predicate, object, support, ...}]}

Output:
  {"nodes": [{"text": str, "type": "concept"}], "edges": [...], "removed_nodes": [...], "summary": {...}}
"""
import logging
from typing import Dict, List

from app.graphs.rules.node_types import classify_node

logger = logging.getLogger(__name__)


def sanitize_graph(structural_graph: Dict) -> Dict:
    """Remove noise nodes and their edges from a Phase-2 structural graph.

    Steps:
    1. Classify every node — 'noise' or 'concept'.
    2. Remove noise nodes and every edge that touches one.
    3. Return annotated graph.

    Args:
        structural_graph: Phase-2 output dict with 'nodes' and 'edges' keys.

    Returns:
        Sanitized graph dict.
    """
    nodes = structural_graph.get("nodes", [])
    edges = structural_graph.get("edges", [])

    # Classify all nodes
    node_types: Dict[str, str] = {node: classify_node(node) for node in nodes}

    noise_nodes = {n for n, t in node_types.items() if t == "noise"}
    concept_count = len(nodes) - len(noise_nodes)

    logger.info(
        "sanitize_graph: %d nodes total — %d concepts kept, %d noise removed.",
        len(nodes), concept_count, len(noise_nodes),
    )

    # Remove edges that touch a noise node
    clean_edges = [
        e for e in edges
        if e.get("subject") not in noise_nodes
        and e.get("object") not in noise_nodes
    ]

    dropped_edges = len(edges) - len(clean_edges)
    if dropped_edges:
        logger.info("sanitize_graph: removed %d edges touching noise nodes.", dropped_edges)

    # Build output node list (concepts only)
    output_nodes = [
        {"text": node, "type": "concept"}
        for node in nodes
        if node not in noise_nodes
    ]

    return {
        "nodes": output_nodes,
        "edges": clean_edges,
        "removed_nodes": list(noise_nodes),
        "summary": {
            "total_nodes_before": len(nodes),
            "total_nodes_after": len(output_nodes),
            "noise_removed": len(noise_nodes),
            "total_edges_before": len(edges),
            "total_edges_after": len(clean_edges),
        },
    }
