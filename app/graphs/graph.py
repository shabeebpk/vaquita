"""In-memory graph representation.

The graph is a derived view built from normalized, deduplicated triples.
It is not persisted and can be rebuilt at any time with different thresholds.
"""
from typing import Dict, List, Tuple, Set
from collections import defaultdict
import logging

logger = logging.getLogger(__name__)


class EvidenceGroup:
    """Represents a single normalized triple and its supporting evidence."""
    
    def __init__(self, subject: str, predicate: str, obj: str):
        self.subject = subject
        self.predicate = predicate
        self.object = obj
        self.count = 0  # Support count (number of times this triple appears)
        self.block_ids: Set[int] = set()  # Blocks where this evidence was found
        self.source_ids: Set[int] = set()  # Sources for those blocks
    
    def add_evidence(self, block_id: int, source_id: int):
        """Record this triple appearing in a block from a source."""
        self.count += 1
        self.block_ids.add(block_id)
        if source_id is not None:
            self.source_ids.add(source_id)
    
    def to_dict(self):
        """Return dict representation for API/logging."""
        return {
            "subject": self.subject,
            "predicate": self.predicate,
            "object": self.object,
            "support": self.count,
            "block_ids": sorted(list(self.block_ids)),
            "source_ids": sorted(list(self.source_ids))
        }


class ConfidenceWeightedGraph:
    """In-memory graph built from evidence groups above a confidence threshold."""
    
    def __init__(self, threshold: int = 2):
        """Initialize graph with a support threshold.
        
        Args:
            threshold: minimum support count to include an edge (default 2)
        """
        self.threshold = threshold
        # Adjacency: subject -> list of (predicate, object, support, evidence_group)
        self.adjacency: Dict[str, List[Tuple[str, str, int, EvidenceGroup]]] = defaultdict(list)
        self.nodes: Set[str] = set()
        self.edges_count = 0
    
    def add_edge_from_group(self, group: EvidenceGroup):
        """Add an edge if the group's support meets or exceeds threshold."""
        if group.count < self.threshold:
            logger.debug(
                "Skipping edge (%s -> %s) with support %d (below threshold %d)",
                group.subject, group.predicate, group.count, self.threshold
            )
            return
        
        self.adjacency[group.subject].append((group.predicate, group.object, group.count, group))
        self.nodes.add(group.subject)
        self.nodes.add(group.object)
        self.edges_count += 1
        logger.debug(
            "Added edge (%s -[%s:%d]-> %s)",
            group.subject, group.predicate, group.count, group.object
        )
    
    def get_node_neighbors(self, node: str) -> List[Tuple[str, str, int, EvidenceGroup]]:
        """Get all outgoing edges from a node."""
        return self.adjacency.get(node, [])
    
    def to_dict(self):
        """Return dict representation for API/inspection."""
        return {
            "threshold": self.threshold,
            "nodes": sorted(list(self.nodes)),
            "edges": self.edges_count,
            "adjacency": {
                subject: [
                    {
                        "predicate": pred,
                        "object": obj,
                        "support": support,
                        "evidence": group.to_dict()
                    }
                    for pred, obj, support, group in edges
                ]
                for subject, edges in self.adjacency.items()
            }
        }
