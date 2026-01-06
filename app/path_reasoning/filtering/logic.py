"""
Phase-4.5: Hypothesis Filtering.

This module implements deterministic, rule-based filtering of hypotheses produced by Phase-4.
It is designed to be reusable (Explore vs Query mode) and strictly read-only regarding
the semantic graph.
"""

from typing import List, Dict, Set, Any, Tuple, Optional
import logging
import networkx as nx
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)

# Default Configuration
DEFAULT_CONFIG = {
    "hub_degree_threshold": 50,  # Max degree for intermediate nodes
    "min_confidence": 10,         # Minimum evidence score
    "generic_predicates": {"related_to", "mentions", "about"},
    "forbidden_node_types": {"entity", "metadata", "citation", "url"},
}

@dataclass
class FilteringContext:
    """Shared immutable context for filtering rules."""
    graph: nx.DiGraph
    degrees: Dict[str, int]
    config: Dict[str, Any]
    
    # Fast path for commonly accessed config values
    hub_threshold: int = field(init=False)
    min_confidence: int = field(init=False)
    generic_predicates: Set[str] = field(init=False)
    forbidden_types: Set[str] = field(init=False)

    def __post_init__(self):
        self.hub_threshold = self.config.get("hub_degree_threshold", 50)
        self.min_confidence = self.config.get("min_confidence", 2)
        self.generic_predicates = self.config.get("generic_predicates", set())
        self.forbidden_types = self.config.get("forbidden_node_types", set())


def _graph_to_nx_for_filtering(semantic_graph: Dict) -> nx.DiGraph:
    """Convert Phase-3 semantic graph dict into a networkx.DiGraph for analysis."""
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

    # Add edges
    for edge in semantic_graph.get("edges", []):
        subj = edge.get("subject")
        obj = edge.get("object")
        if subj and obj:
            G.add_edge(subj, obj)

    return G


# --- Pure Rule Functions ---

def check_hub_suppression(hyp: Dict, ctx: FilteringContext) -> Tuple[bool, Optional[str]]:
    """Rule 1: Reject paths passing through high-degree hubs."""
    path = hyp.get("path", [])
    if len(path) > 2:
        intermediates = path[1:-1]
        for node in intermediates:
            deg = ctx.degrees.get(node, 0)
            if deg > ctx.hub_threshold:
                return False, f"Node '{node}' has degree {deg} > {ctx.hub_threshold}"
    return True, None


def check_role_constraints(hyp: Dict, ctx: FilteringContext) -> Tuple[bool, Optional[str]]:
    """Rule 2: Reject paths containing forbidden node types (entity, metadata, etc)."""
    path = hyp.get("path", [])
    for node in path:
        if not ctx.graph.has_node(node):
            continue
        ntype = ctx.graph.nodes[node].get("type", "concept")
        if ntype and ntype.lower() in ctx.forbidden_types:
            return False, f"Node '{node}' has forbidden type '{ntype}'"
    return True, None


def check_predicate_semantics(hyp: Dict, ctx: FilteringContext) -> Tuple[bool, Optional[str]]:
    """Rule 3: Require at least one non-generic predicate."""
    preds = hyp.get("predicates", [])
    if not preds:
        return True, None  # Or pass? Phase-4 usually guarantees predicates.
    
    all_generic = all(p.lower() in ctx.generic_predicates for p in preds)
    if all_generic:
        return False, f"All predicates are generic: {preds}"
    return True, None


def check_evidence_threshold(hyp: Dict, ctx: FilteringContext) -> Tuple[bool, Optional[str]]:
    """Rule 4: Require minimum confidence score."""
    conf = int(hyp.get("confidence", 0))
    if conf < ctx.min_confidence:
        return False, f"Confidence {conf} < {ctx.min_confidence}"
    return True, None


def check_novelty(hyp: Dict, ctx: FilteringContext) -> Tuple[bool, Optional[str]]:
    """Rule 5: Reject if direct edge exists between source and target."""
    source = hyp.get("source")
    target = hyp.get("target")
    if source and target and ctx.graph.has_edge(source, target):
        return False, f"Direct edge exists between '{source}' and '{target}'"
    return True, None


# Check registry (Ordered)
RULES = [
    ("hub_suppression", check_hub_suppression),
    ("role_constraint", check_role_constraints),
    ("predicate_semantics", check_predicate_semantics),
    ("evidence_threshold", check_evidence_threshold),
    ("novelty", check_novelty),
]


def filter_hypotheses(
    hypotheses: List[Dict],
    semantic_graph: Dict,
    config: Dict[str, Any] = None
) -> List[Dict]:
    """
    Apply Phase-4.5 filtering rules to a list of hypotheses.

    Modifies the hypothesis dictionaries in-place (or returns new ones) by adding:
      - passed_filter (bool): True if passed all checks
      - filter_reason (dict): JSON-serializable details on failure, or None

    The function returns the list of processed hypotheses (ALL of them, not just passed).
    """
    cfg = DEFAULT_CONFIG.copy()
    if config:
        cfg.update(config)

    # Build Context
    G = _graph_to_nx_for_filtering(semantic_graph)
    degrees = dict(G.degree())
    ctx = FilteringContext(graph=G, degrees=degrees, config=cfg)

    processed = []

    for hyp in hypotheses:
        # Clone to avoid unexpected side-effects if needed, though inplace is fine
        # We assume inplace modification of the dict is acceptable as per previous impl.
        
        passed = True
        reasons = {}

        for rule_name, rule_fn in RULES:
            rule_passed, failure_msg = rule_fn(hyp, ctx)
            if not rule_passed:
                passed = False
                reasons[rule_name] = failure_msg
                break  # Stop at first failure
        
        hyp["passed_filter"] = passed
        hyp["filter_reason"] = reasons if not passed else None
        
        processed.append(hyp)

    return processed
