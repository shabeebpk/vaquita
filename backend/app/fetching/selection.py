"""
Strategic Lead Selection for Fetching.

Implements "Grouped Diversity" to ensure the system investigates unique
relationships instead of repeating similar paths.
"""
import logging
from typing import List, Dict, Any, Tuple, Optional
from sqlalchemy.orm import Session
from app.storage.models import Hypothesis

logger = logging.getLogger(__name__)

def select_top_diverse_leads(session: Session, job_id: int, k: int, hypotheses: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """
    Select Top-K unique (Source, Target) relationships to investigate.
    
    Algorithm (Refined by User):
    1. Filter: Passed OR (Failed ONLY due to 'evidence_threshold').
    2. Group by (source, target).
    3. Determine Group Leader (Max Confidence).
    4. Determine Group Status (Passed if ANY member passed).
    5. Sort Passed Groups (Confidence Desc).
    6. Sort Low-Conf Groups (Confidence Desc).
    7. Select Top-K: Fill with Passed first, then Low-Conf if space remains.
    """
    # 1. Use provided hypotheses list or fetch from DB
    if hypotheses is None:
        rows = session.query(Hypothesis).filter(Hypothesis.job_id == job_id).all()
        # Convert to dicts for consistent processing
        hypos = []
        for r in rows:
            hypos.append({
                "id": r.id,
                "source": r.source,
                "target": r.target,
                "confidence": r.confidence,
                "explanation": r.explanation,
                "path": r.path,
                "predicates": r.predicates,
                "passed_filter": r.passed_filter,
                "filter_reason": r.filter_reason
            })
    else:
        hypos = hypotheses
    
    if not hypos:
        return []

    # 2. Grouping & Filtering Logic
    groups: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for h in hypos:
        pair = (h.get("source"), h.get("target"))
        if not pair[0] or not pair[1]:
            continue
            
        # Check if this hypothesis is a valid lead
        is_passed = h.get("passed_filter", False)
        is_promising = False
        
        filter_reason = h.get("filter_reason")
        if not is_passed and filter_reason:
            # Tier B Filter: Only accept if the ONLY reason for failure was low confidence
            reasons = filter_reason.keys() if isinstance(filter_reason, dict) else []
            if len(reasons) == 1 and "evidence_threshold" in reasons:
                is_promising = True

        # Rule 1: "Considering hypotheses... passed or filtered out/failed due to less confidence only"
        if not is_passed and not is_promising:
            continue

        if pair not in groups:
            groups[pair] = {
                "source": pair[0],
                "target": pair[1],
                "group_confidence": -1.0,
                "group_passed": False,
                "leader_hypo": None
            }

        g = groups[pair]
        
        # Update Group Passed Status (Rule: "If at least one hypothesis in the group is passed -> mark whole group as passed")
        if is_passed:
            g["group_passed"] = True
            
        # Update Group Leader (Rule: "Best keep individual hypoid... selected leader(the highest confidence)")
        current_conf = h.get("confidence", 0)
        if g["leader_hypo"] is None or current_conf > g["group_confidence"]:
            g["group_confidence"] = current_conf
            g["leader_hypo"] = h

    # 3. Create Lists (Rule: "Create two empty lists: passed_groups and low_conf_groups")
    passed_groups = []
    low_conf_groups = []
    
    for g in groups.values():
        if g["group_passed"]:
            passed_groups.append(g)
        else:
            low_conf_groups.append(g)
            
    # 4. Sort (Rule: "Sort ... in descending order by group_confidence")
    passed_groups.sort(key=lambda x: x["group_confidence"], reverse=True)
    low_conf_groups.sort(key=lambda x: x["group_confidence"], reverse=True)
    
    # 5. Selection (Rule: "Iterate over passed_groups... If length < top_k... Iterate over low_conf_groups")
    selected_groups = []
    
    # Fill from Passed
    for g in passed_groups:
        if len(selected_groups) >= k:
            break
        selected_groups.append(g)
        
    # Fill from Low-Conf (if needed)
    if len(selected_groups) < k:
        for g in low_conf_groups:
            if len(selected_groups) >= k:
                break
            selected_groups.append(g)

    # 6. Return Leaders as input to fetch phase
    selected_leads = [g["leader_hypo"] for g in selected_groups]
    
    logger.info(
        f"Refined Top-K Selection: Evaluated {len(groups)} unique relationships for Job {job_id}. "
        f"Selected {len(selected_leads)} leads (K={k}). "
        f"Composition: {len([g for g in selected_groups if g['group_passed']])} Passed, "
        f"{len([g for g in selected_groups if not g['group_passed']])} Low-Conf."
    )
    
    return selected_leads
