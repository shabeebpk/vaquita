"""Graph sanitization (Phase 2.5): node classification, demotion, and hygiene.

This module reads a Phase-2 structural graph (in-memory dict) and produces
a cleaner, type-annotated graph. No spaCy re-processing, no semantic inference.
Only rule-based classification, metadata demotion, and noise removal.

Input: Phase-2 graph dict with structure:
  {
      "nodes": [...],
      "edges": [
          {"subject": str, "predicate": str, "object": str, "support": int, ...},
          ...
      ]
  }

Output: sanitized graph dict with annotated nodes and demoted metadata:
  {
      "nodes": [
          {"text": str, "type": str, "attributes": {...}},
          ...
      ],
      "edges": [
          {"subject": str, "predicate": str, "object": str, "support": int, ...},
          ...
      ],
      "removed_nodes": [...],  # noise/metadata that were demoted
      "summary": {...}
  }
"""
import logging
from typing import Dict, List, Tuple, Set, Any

from app.graphs.rules.node_types import classify_node
from app.graphs.rules.metadata import extract_metadata, METADATA_EDGE_TYPE

logger = logging.getLogger(__name__)


def sanitize_graph(structural_graph: Dict) -> Dict:
    """Sanitize a Phase-2 structural graph.
    
    Steps:
    1. Classify all nodes into types (concept, entity, metadata, citation, noise).
    2. Demote metadata nodes into attributes on their subjects.
    3. Remove noise nodes and rewrite their edges.
    4. Return cleaned, annotated graph.
    
    Args:
        structural_graph: Phase-2 output dict with "nodes" and "edges" keys
    
    Returns:
        sanitized graph dict with typed nodes and metadata attributes
    """
    
    nodes = structural_graph.get("nodes", [])
    edges = structural_graph.get("edges", [])
    
    # Classify all nodes
    node_types: Dict[str, str] = {}
    for node in nodes:
        node_types[node] = classify_node(node)
    
    logger.info(
        "Node classification: concept=%d, entity=%d, metadata=%d, citation=%d, noise=%d",
        sum(1 for t in node_types.values() if t == "concept"),
        sum(1 for t in node_types.values() if t == "entity"),
        sum(1 for t in node_types.values() if t == "metadata"),
        sum(1 for t in node_types.values() if t == "citation"),
        sum(1 for t in node_types.values() if t == "noise"),
    )
    
    # Build node attribute dict for demoted metadata
    node_attributes: Dict[str, Dict[str, Any]] = {n: {} for n in nodes}
    
    # Phase 1: Demote metadata from edges
    # Iterate over edges: if object is metadata, extract its value and demote
    sanitized_edges = []
    for edge in edges:
        subj = edge.get("subject")
        pred = edge.get("predicate")
        obj = edge.get("object")
        
        if obj and node_types.get(obj) == "metadata":
            # Extract metadata from object
            attr_name, attr_val = extract_metadata(obj)
            if attr_name and subj in node_attributes:
                node_attributes[subj][attr_name] = attr_val
                # Record edge with special "demoted" tag for logging
                logger.debug(f"Demoted metadata edge: {subj} -[{pred}]-> {obj} -> attribute {attr_name}={attr_val}")
            # Don't add this edge to sanitized graph (it becomes an attribute)
        else:
            # Keep non-metadata edges
            sanitized_edges.append(edge)
    
    # Phase 2: Remove noise nodes and rewrite edges that reference them
    noise_nodes = {n for n, t in node_types.items() if t == "noise"}
    sanitized_edges_v2 = []
    for edge in sanitized_edges:
        subj = edge.get("subject")
        obj = edge.get("object")
        
        # Skip edges where subject or object is noise
        if subj in noise_nodes or obj in noise_nodes:
            logger.debug(f"Removed edge with noise node: {subj} -[{edge.get('predicate')}]-> {obj}")
            continue
        
        sanitized_edges_v2.append(edge)
    
    # Phase 3: Build output node list with types and attributes
    output_nodes = []
    for node in nodes:
        node_type = node_types.get(node, "concept")
        attrs = node_attributes.get(node, {})
        
        # Skip noise nodes in output
        if node_type == "noise":
            continue
        
        output_nodes.append({
            "text": node,
            "type": node_type,
            "attributes": attrs,
        })
    
    removed_nodes = list(noise_nodes)
    
    logger.info(
        f"Sanitization complete: {len(output_nodes)} nodes kept, {len(removed_nodes)} noise nodes removed, "
        f"{len(edges) - len(sanitized_edges_v2)} edges removed"
    )
    
    return {
        "nodes": output_nodes,
        "edges": sanitized_edges_v2,
        "removed_nodes": removed_nodes,
        "summary": {
            "total_nodes_before": len(nodes),
            "total_nodes_after": len(output_nodes),
            "noise_removed": len(removed_nodes),
            "total_edges_before": len(edges),
            "total_edges_after": len(sanitized_edges_v2),
            "metadata_demoted": len(edges) - len(sanitized_edges),
        },
    }
