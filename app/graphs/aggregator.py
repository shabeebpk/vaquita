"""Evidence aggregation: read triples, normalize, group, threshold, build graph.

This is the main aggregation processor.
"""
import os
import logging
from typing import Dict
from sqlalchemy.orm import Session
from collections import defaultdict

from app.storage.db import engine
from app.storage.models import Triple
from app.graphs.normalizer import normalize_triple
from app.graphs.graph import EvidenceGroup, ConfidenceWeightedGraph

logger = logging.getLogger(__name__)


def aggregate_evidence_for_job(
    job_id: int,
    threshold: int = os.environ.get("MINIMUM_EDGE_SUPPORT")
) -> Dict:
    """Aggregate evidence (triples) for a job into a confidence-weighted graph.
    
    Args:
        job_id: Job ID to aggregate triples for
        threshold: Minimum support count to include an edge (default 2)
    
    Returns:
        dict with keys:
          - job_id
          - threshold
          - total_triples: raw triples read from DB
          - evidence_groups: number of unique normalized triples
          - filtered_groups: number of groups above threshold
          - graph: dict representation of the in-memory graph
          - errors: count of malformed rows (defensive)
    """
    
    evidence_groups: Dict[tuple, EvidenceGroup] = {}
    errors = 0
    total_triples = 0
    
    try:
        with Session(engine) as session:
            # Read all triples for this job
            triples = session.query(Triple).filter(Triple.job_id == job_id).all()
            total_triples = len(triples)
            logger.info(f"Aggregating {total_triples} triples for job {job_id}")
            
            for triple_row in triples:
                try:
                    # Normalize the triple components
                    norm_subject, norm_predicate, norm_object = normalize_triple(
                        triple_row.subject,
                        triple_row.predicate,
                        triple_row.object
                    )
                    
                    # Skip if any component normalized to empty
                    if not norm_subject or not norm_predicate or not norm_object:
                        logger.debug(
                            f"Skipping triple {triple_row.id}: normalized to empty after normalization"
                        )
                        errors += 1
                        continue
                    
                    # Create a normalized key
                    key = (norm_subject, norm_predicate, norm_object)
                    
                    # Get or create evidence group
                    if key not in evidence_groups:
                        evidence_groups[key] = EvidenceGroup(norm_subject, norm_predicate, norm_object)
                    
                    # Add this triple as evidence
                    evidence_groups[key].add_evidence(triple_row.block_id, triple_row.source_id)
                    
                except Exception as e:
                    logger.error(f"Error processing triple {triple_row.id}: {e}")
                    errors += 1
                    continue
        
        # Build the confidence-weighted graph
        graph = ConfidenceWeightedGraph(threshold=threshold)
        filtered_groups = 0
        
        for group in evidence_groups.values():
            if group.count >= threshold:
                graph.add_edge_from_group(group)
                filtered_groups += 1
        
        logger.info(
            f"Aggregation complete for job {job_id}: "
            f"{total_triples} triples -> {len(evidence_groups)} groups -> "
            f"{filtered_groups} groups above threshold {threshold}"
        )
        
        return {
            "job_id": job_id,
            "threshold": threshold,
            "total_triples": total_triples,
            "evidence_groups": len(evidence_groups),
            "filtered_groups": filtered_groups,
            "errors": errors,
            "graph": graph.to_dict()
        }
        
    except Exception as e:
        logger.error(f"Fatal error during aggregation for job {job_id}: {e}")
        raise
