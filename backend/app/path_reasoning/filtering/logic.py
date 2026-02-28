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

def apply_hub_suppression_to_graph(G: nx.DiGraph, hub_degree_threshold: int) -> nx.DiGraph:
    """Remove high-degree hub nodes from the graph before path enumeration.
    
    Hub nodes are intermediate nodes with degree > hub_degree_threshold.
    Removing hubs before path enumeration dramatically reduces candidate paths
    and memory usage, and improves reasoning performance.
    
    Args:
        G: NetworkX directed graph with hub nodes
        hub_degree_threshold: Max degree for intermediate nodes (from admin_policy)
    
    Returns:
        Updated graph with hub nodes and their incident edges removed
    """
    if hub_degree_threshold <= 0:
        # Disabled (0 or negative means no suppression)
        return G
    
    # Identify hub nodes (degree includes both in and out edges in DiGraph)
    hubs = set()
    for node in G.nodes():
        degree = G.degree(node)  # Sum of in-degree and out-degree for DiGraph
        if degree > hub_degree_threshold:
            hubs.add(node)
    
    if hubs:
        logger.debug(f"Hub suppression: removing {len(hubs)} nodes with degree > {hub_degree_threshold}")
        G.remove_nodes_from(hubs)
        logger.debug(f"Graph after hub suppression: {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")
    
    return G



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


# Check registry (Ordered) — Permanent rules reject entirely, extractable rules flag only.
# Permanent structural filters (noise) are applied by
# sanitize_graph in sanitize.py BEFORE paths are generated.
# Note: Hub suppression is now applied at the graph level in reasoning.py
#       before path enumeration, not as a hypothesis filter.
PERMANENT_RULES = [
    ("predicate_semantics", check_predicate_semantics),
    ("novelty", check_novelty),
]

EXTRACTABLE_RULES = [
    ("evidence_threshold", check_evidence_threshold),
]

RULES = PERMANENT_RULES + EXTRACTABLE_RULES


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
) -> Tuple[List[Dict], List[Dict]]:
    """
    Apply Phase-4.5 filtering: Permanent rules reject entirely, extractable rules flag.

    Returns:
        (passed_hypotheses, failed_hypotheses)
        - passed_hypotheses: Hypotheses that passed ALL permanent rules
                            (may have failed extractable rules but kept)
        - failed_hypotheses: Hypotheses rejected by permanent rules (NOT stored in DB)
    """
    cfg = DEFAULT_CONFIG.copy()
    if config:
        cfg.update(config)

    # Build Context
    G = _graph_to_nx_for_filtering(semantic_graph)
    degrees = dict(G.degree())
    ctx = FilteringContext(graph=G, degrees=degrees, config=cfg)

    passed_hypotheses = []
    failed_hypotheses = []

    for hyp in hypotheses:
        # Check permanent rules first
        permanent_passed = True
        permanent_reasons = {}
        
        for rule_name, rule_fn in PERMANENT_RULES:
            rule_passed, failure_msg = rule_fn(hyp, ctx)
            if not rule_passed:
                permanent_passed = False
                permanent_reasons[rule_name] = failure_msg
                break  # Stop at first permanent failure
        
        # If permanent rules failed, reject entirely
        if not permanent_passed:
            hyp["passed_filter"] = False
            hyp["filter_reason"] = permanent_reasons
            hyp["rejection_type"] = "permanent"
            failed_hypotheses.append(hyp)
            continue
        
        # Permanent rules passed, now check extractable rules
        extractable_passed = True
        extractable_reasons = {}
        
        for rule_name, rule_fn in EXTRACTABLE_RULES:
            rule_passed, failure_msg = rule_fn(hyp, ctx)
            if not rule_passed:
                extractable_passed = False
                extractable_reasons[rule_name] = failure_msg
                break
        
        # Extractable rule failures don't reject, just flag
        hyp["passed_filter"] = extractable_passed
        hyp["filter_reason"] = extractable_reasons if not extractable_passed else None
        hyp["rejection_type"] = "extractable" if not extractable_passed else None
        
        passed_hypotheses.append(hyp)

    return passed_hypotheses, failed_hypotheses


def resolve_domains_batch(
    hypotheses: List[Dict],
    llm_client: Any,
) -> List[Dict]:
    """
    Resolve domains for hypotheses in batch by grouping on (source, target) pairs.
    
    Groups hypotheses by (source, target), creates a single LLM prompt per group
    with all intermediate paths, and applies the result to all hypotheses in the group.
    
    Args:
        hypotheses: List of filtered hypotheses
        llm_client: LLM client for domain resolution
    
    Returns:
        Hypotheses with 'domain' field populated
    """
    from app.config.admin_policy import admin_policy
    from app.prompts.loader import load_prompt
    
    allowed_domains = admin_policy.algorithm.domain_resolution.allowed_domains
    
    if not allowed_domains:
        logger.warning("No allowed_domains in admin_policy. Skipping domain resolution.")
        for hyp in hypotheses:
            hyp["domain"] = None
        return hypotheses
    
    if llm_client is None:
        logger.warning("LLM client is None. Skipping domain resolution.")
        for hyp in hypotheses:
            hyp["domain"] = None
        return hypotheses
    
    # Group hypotheses by (source, target)
    grouped: Dict[Tuple[str, str], List[Dict]] = {}
    for hyp in hypotheses:
        key = (hyp["source"], hyp["target"])
        if key not in grouped:
            grouped[key] = []
        grouped[key].append(hyp)
    
    # For each group, resolve domain once
    for (source, target), group_hyps in grouped.items():
        # Format paths for this group
        paths_text = []
        for hyp in group_hyps:
            path = hyp.get("path", [])
            confidence = hyp.get("confidence", 0)
            path_str = " → ".join(path)
            paths_text.append(f"  - {path_str} (confidence: {confidence})")
        
        paths_block = "\n".join(paths_text)
        domains_str = ", ".join(allowed_domains)
        
        # Load and format prompt
        prompt_file = admin_policy.prompt_assets.domain_resolver
        template = load_prompt(
            prompt_file,
            fallback=(
                "Classify the domain of the following hypothesis.\n"
                f"Source: {source}\n"
                f"Target: {target}\n"
                f"Paths:\n{paths_block}\n"
                f"Allowed domains: {domains_str}\n"
                "Return ONLY the domain name if it matches an allowed domain, "
                "or 'null' (as text) if none match. No explanation."
            )
        )
        
        try:
            prompt = template.format(
                source=source,
                target=target,
                paths=paths_block,
                domains=domains_str,
            )
        except KeyError:
            # Fallback if template has different variables
            prompt = (
                f"Classify domain: Source={source}, Target={target}\n"
                f"Paths:\n{paths_block}\n"
                f"Domains: {domains_str}"
            )
        
        # Call LLM
        resolved_domain = None
        try:
            response = llm_client.generate(prompt)
            if response:
                resolved = response.strip().lower()
                
                # Validate against allowed_domains
                for domain in allowed_domains:
                    if resolved == domain.lower():
                        resolved_domain = domain
                        logger.debug(f"Resolved domain '{domain}' for {source} → {target}")
                        break
                
                if not resolved_domain and resolved != "null":
                    logger.warning(f"LLM returned invalid domain: '{resolved}' for {source} → {target}")
        except Exception as e:
            logger.error(f"Domain resolution failed for {source} → {target}: {e}")
        
        # Apply resolved domain to all hypotheses in the group
        for hyp in group_hyps:
            hyp["domain"] = resolved_domain
    
    return hypotheses


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
