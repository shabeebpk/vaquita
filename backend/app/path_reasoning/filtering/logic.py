"""
Phase-4.5: Hypothesis Filtering.

This module implements deterministic, rule-based filtering of hypotheses produced by Phase-4.
It is designed to be reusable (Explore vs Query mode) and strictly read-only regarding
the semantic graph.
"""

from typing import List, Dict, Set, Any, Tuple, Optional
import logging
import re
import networkx as nx
from dataclasses import dataclass, field
from sqlalchemy.orm import Session
from app.storage.models import JobPaperEvidence, Triple, IngestionSource
from app.graphs.rules.node_types import is_impactful_node

logger = logging.getLogger(__name__)


def _load_default_config() -> Dict[str, Any]:
    """Load filtering defaults from admin_policy. No hardcoding."""
    try:
        from app.config.admin_policy import admin_policy
        pr = admin_policy.algorithm.path_reasoning_defaults
        generic = set(admin_policy.graph_rules.generic_predicates)
        return {
            "hub_degree_threshold": pr.hub_degree_threshold if hasattr(pr, "hub_degree_threshold") else 50,
            "min_confidence": pr.min_confidence if hasattr(pr, "min_confidence") else 2,
            "generic_predicates": generic,
        }
    except Exception as e:
        logger.warning(f"Could not load filtering config from admin_policy: {e}. Using safe defaults.")
        return {
            "hub_degree_threshold": 50,
            "min_confidence": 2,
            "generic_predicates": set(),
        }


DEFAULT_CONFIG = _load_default_config()

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

    def __post_init__(self):
        self.hub_threshold = self.config.get("hub_degree_threshold", 50)
        self.min_confidence = self.config.get("min_confidence", 2)
        self.generic_predicates = self.config.get("generic_predicates", set())


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


# Check registry (Ordered) — only reversible rejections stored here.
# Permanent structural filters (noise) are applied by
# sanitize_graph in sanitize.py BEFORE paths are generated.
RULES = [
    ("hub_suppression", check_hub_suppression),
    ("predicate_semantics", check_predicate_semantics),
    ("evidence_threshold", check_evidence_threshold),
    ("novelty", check_novelty),
]


def is_low_confidence_rejection(hyp: Dict) -> bool:
    """
    Check if a hypothesis was rejected ONLY due to low confidence.
    
    A "promising" lead is one that passes structural and semantic rules
    but fails the evidence threshold.
    """
    if hyp.get("passed_filter", False):
        return False
        
    reasons = hyp.get("filter_reason", {})
    if not reasons:
        return False
        
    # Must have failed evidence_threshold
    if "evidence_threshold" not in reasons:
        return False
        
    # Must NOT have failed any other rule
    # (If length is 1 and it's evidence_threshold, it's a pure confidence rejection)
    return len(reasons) == 1


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


def calculate_impact_scores(job_id: int, hypotheses: List[Dict], session: Session) -> None:
    """
    Calculate and update Impact Scores for ALL papers in the Strategic Ledger.

    Formula:
    score = (hyp_ref_count) + (sum_of_confidence) + (new_entity_count)

    IMPORTANT: Always uses ALL hypotheses for the job from the database, not just
    the current batch. This ensures every paper gets a fair, up-to-date score
    regardless of which fetch/hypothesis batch originally discovered it.
    """
    from app.storage.models import Hypothesis as HypothesisModel

    # 1. Load ALL passed or promising hypotheses for the job from DB
    # This is the key fix: we don't restrict to the current batch
    all_job_hypos = session.query(HypothesisModel).filter(
        HypothesisModel.job_id == job_id,
        HypothesisModel.is_active == True,
    ).all()

    relevant_hypos = [
        h for h in all_job_hypos
        if h.passed_filter or (
            not h.passed_filter and
            h.filter_reason and
            list(h.filter_reason.keys()) == ["evidence_threshold"]
        )
    ]

    if not relevant_hypos:
        logger.info(f"Job {job_id}: No passed or promising hypotheses to compute impact scores.")
        return

    # 2. Build Paper -> Impact Metrics map
    paper_metrics: Dict[int, Dict[str, Any]] = {}

    # Collect all triple IDs from all relevant hypotheses
    all_triple_ids = []
    for h in relevant_hypos:
        all_triple_ids.extend(h.triple_ids or [])

    if not all_triple_ids:
        return

    # Map Triple ID -> Paper ID via IngestionSource
    triple_to_paper = {}
    triples_data = session.query(
        Triple.id, IngestionSource.source_ref
    ).join(
        IngestionSource, Triple.ingestion_source_id == IngestionSource.id
    ).filter(
        Triple.id.in_(list(set(all_triple_ids)))
    ).all()

    for tid, s_ref in triples_data:
        if s_ref and s_ref.startswith("paper:"):
            try:
                pid = int(s_ref.split(":")[1])
                triple_to_paper[tid] = pid
            except (ValueError, IndexError):
                continue

    # 3. Aggregate metrics from ALL hypotheses
    for h in relevant_hypos:
        conf = h.confidence or 0
        t_ids = h.triple_ids or []

        h_papers = {triple_to_paper[tid] for tid in t_ids if tid in triple_to_paper}

        for pid in h_papers:
            if pid not in paper_metrics:
                paper_metrics[pid] = {"refs": 0, "conf": 0.0, "entities": set()}
            paper_metrics[pid]["refs"] += 1
            paper_metrics[pid]["conf"] += float(conf)

    # 4. Entity density from abstract triples for ALL ledger papers
    ledger_entries = session.query(JobPaperEvidence).filter(
        JobPaperEvidence.job_id == job_id
    ).all()

    ledger_paper_ids = [e.paper_id for e in ledger_entries]

    abstract_triples = session.query(
        IngestionSource.source_ref, Triple.subject, Triple.object
    ).join(
        Triple, Triple.ingestion_source_id == IngestionSource.id
    ).filter(
        IngestionSource.job_id == job_id,
        IngestionSource.source_ref.in_([f"paper:{pid}" for pid in ledger_paper_ids])
    ).all()

    for s_ref, subj, obj in abstract_triples:
        try:
            pid = int(s_ref.split(":")[1])
            if pid not in paper_metrics:
                paper_metrics[pid] = {"refs": 0, "conf": 0.0, "entities": set()}
            if is_impactful_node(subj):
                paper_metrics[pid]["entities"].add(subj)
            if is_impactful_node(obj):
                paper_metrics[pid]["entities"].add(obj)
        except Exception:
            continue

    # 5. Update ALL ledger entries — every paper gets recalculated
    updated = 0
    for entry in ledger_entries:
        metrics = paper_metrics.get(entry.paper_id, {"refs": 0, "conf": 0.0, "entities": set()})
        entry.hypo_ref_count = metrics["refs"]
        entry.cumulative_conf = metrics["conf"]
        entry.entity_density = len(metrics["entities"])
        entry.impact_score = float(
            entry.hypo_ref_count +
            entry.cumulative_conf +
            entry.entity_density
        )
        updated += 1

    session.commit()
    logger.info(f"Job {job_id}: Recalculated impact scores for {updated} papers using {len(relevant_hypos)} total hypotheses.")
