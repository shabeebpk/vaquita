"""Node classification for Phase-2.5 graph sanitization.

A node is REMOVED if it matches any configured removal pattern or exact string.
Everything else is kept as a concept — no hardcoded allow-lists.

Classification (for nodes that survive removal):
- noise   : matched a removal pattern or exact string
- concept : everything else (scientific terms, techniques, processes, models)

The removal patterns and exact strings are loaded from admin_policy.json
under graph_rules.node_removal_patterns and graph_rules.node_removal_exact.
No hardcoded lists exist here.
"""
import re
import logging
from typing import Tuple

logger = logging.getLogger(__name__)


def _load_rules() -> Tuple[list, set]:
    """Load removal rules from admin_policy config. Returns (compiled_patterns, exact_set)."""
    try:
        from app.config.admin_policy import admin_policy
        raw_patterns = admin_policy.graph_rules.node_removal_patterns
        exact_words = set(w.lower() for w in admin_policy.graph_rules.node_removal_exact)
        compiled = []
        for p in raw_patterns:
            try:
                compiled.append(re.compile(p, re.IGNORECASE))
            except re.error as e:
                logger.warning(f"node_types: Invalid removal pattern {p!r}: {e}")
        return compiled, exact_words
    except Exception as e:
        logger.error(f"node_types: Failed to load graph_rules from admin_policy: {e}. Using empty rules.")
        return [], set()


# Load once at import time
_REMOVAL_PATTERNS, _REMOVAL_EXACT = _load_rules()


def classify_node(node: str, ner_label: str = None) -> str:
    """Classify a single node as 'noise' (will be removed) or 'concept' (kept).

    Args:
        node: node text
        ner_label: unused — kept for backward compatibility only

    Returns:
        'noise' if the node should be removed, 'concept' otherwise.
    """
    if not node or not isinstance(node, str):
        return "noise"

    n = node.strip()
    if not n:
        return "noise"

    # Exact match against removal list (lowercased)
    if n.lower() in _REMOVAL_EXACT:
        return "noise"

    # Pattern match against removal regexes
    for pattern in _REMOVAL_PATTERNS:
        if pattern.match(n):
            return "noise"

    # Everything else is a valid scientific concept
    return "concept"


def is_impactful_node(text: str) -> bool:
    """Heuristic to check if a node is an 'impactful entity' for scoring.
    
    Includes: acronyms, proper nouns (capitalized), and long scientific phrases.
    Excludes: noise (already handled by classify_node) and very short common words.
    """
    if not text or len(text) < 2:
        return False
        
    # If it's all uppercase acronym (e.g., DNA, CRISPR, GPT)
    if text.isupper() and text.isalpha():
        return True
        
    # If it's a multi-word scientific concept (e.g., "gene editing")
    if " " in text:
        return True
        
    # If it's a proper noun (starts with capital)
    if text[0].isupper():
        return True
        
    # If it contains special characters like hyphens or numbers (likely a model or chemical)
    if "-" in text or any(char.isdigit() for char in text):
        return True
        
    return False
