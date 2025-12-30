"""Evidence aggregation and graph construction package.

This phase builds a derived, confidence-weighted graph view from existing triples.
No LLM calls. No database mutations. Deterministic and idempotent.
"""

from .aggregator import aggregate_evidence_for_job
from .structural import project_structural_graph
from .sanitize import sanitize_graph
from .semantic import merge_semantically

__all__ = ["aggregate_evidence_for_job", "project_structural_graph", "sanitize_graph", "merge_semantically"]
