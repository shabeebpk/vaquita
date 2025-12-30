"""Triple normalization for evidence grouping and deduplication.

Normalization is deterministic and never persisted — it's purely for comparison.
"""
import re
import logging

logger = logging.getLogger(__name__)


def normalize_triple_component(s: str) -> str:
    """Normalize a triple component (subject, predicate, or object).
    
    - Convert to lowercase
    - Strip leading/trailing whitespace
    - Collapse internal whitespace
    - Remove trailing punctuation (. , : ;)
    
    Returns: normalized string
    """
    if not isinstance(s, str):
        return ""
    
    # Lowercase
    s = s.lower()
    
    # Strip leading/trailing whitespace
    s = s.strip()
    
    # Collapse internal whitespace
    s = re.sub(r'\s+', ' ', s)
    
    # Remove trailing punctuation
    s = re.sub(r'[.,;:]+$', '', s).strip()
    
    return s


def normalize_triple(subject: str, predicate: str, obj: str) -> tuple:
    """Normalize all three components of a triple.
    
    Returns: (normalized_subject, normalized_predicate, normalized_object)
    """
    return (
        normalize_triple_component(subject),
        normalize_triple_component(predicate),
        normalize_triple_component(obj)
    )
