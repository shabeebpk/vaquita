"""Node type classification rules for Phase-2.5 graph sanitization.

This module defines rules for classifying nodes into one of five types:
- concept: abstract nouns suitable for reasoning (hypothesis, model, method)
- entity: proper nouns, named persons, organizations, acronyms
- metadata: years, numeric IDs, DOIs, ISBNs, ISSNs, URLs
- citation: citation-only nodes (pure references)
- noise: malformed or meaningless fragments
"""
import re

# Concept allow-list: structural/scientific terms common across domains
CONCEPT_ALLOW_LIST = {
    "model",
    "method",
    "dataset",
    "algorithm",
    "hypothesis",
    "system",
    "generation",
    "training",
    "evaluation",
    "experiment",
    "metric",
    "result",
    "approach",
    "technique",
    "framework",
    "architecture",
    "theory",
    "principle",
    "assumption",
    "objective",
    "outcome",
    "parameter",
    "variable",
    "process",
    "procedure",
    "analysis",
    "implementation",
    "application",
    "strategy",
    "component",
    "module",
    "layer",
    "stage",
}

# Entity NER labels from spaCy that indicate entities (from Phase-2 doc processing)
# These are re-checked during sanitization if entity label info is available
ENTITY_NER_LABELS = {"PERSON", "ORG", "GPE", "PRODUCT"}

# Acronym pattern: 2+ uppercase letters, optionally with numbers
ACRONYM_PATTERN = r"^[A-Z][A-Z0-9]+$"

# Metadata patterns
YEAR_PATTERN = r"^(19|20)\d{2}$"
DOI_PATTERN = r"^(doi:|10\.\d+/.*)"
ISBN_PATTERN = r"^(?:ISBN|isbn)[\s-]?(?:10|13)?[\s-]?[\d\s-]+$"
ISSN_PATTERN = r"^(?:ISSN|issn)[\s-]?\d{4}[\s-]?\d{4}$"
URL_PATTERN = r"^https?://|^www\."
ARXIV_PATTERN = r"^arxiv:\d+\.\d+$"
PMID_PATTERN = r"^(?:PMID|pmid):\d+$"

# Numeric-only or pure ID patterns
NUMERIC_ONLY_PATTERN = r"^\d+$"
UUID_LIKE_PATTERN = r"^[a-f0-9\-]{20,}$"

# Citation-only patterns: node is purely a reference with no semantic content
CITATION_KEYWORDS = {"citation", "reference", "cite", "ref"}

# Noise deny-list: patterns that are clearly malformed or meaningless
NOISE_PATTERNS = [
    r"^[^\w\s]$",  # single punctuation
    r"^\.{2,}$",  # ellipsis only
    r"^[_\-\s]*$",  # dashes/underscores/spaces only
    r"^[0-9\.\-]{1,3}$",  # very short numeric fragments
]

# Blacklist: strings that are definitely noise regardless of other rules
NOISE_BLACKLIST = {
    "the",
    "a",
    "an",
    "of",
    "and",
    "or",
    "to",
    "in",
    "is",
    "are",
    "be",
    "by",
    "for",
    "with",
    "as",
    "from",
    "on",
    "at",
    "this",
    "that",
    "which",
    "who",
    "what",
    "where",
    "when",
    "why",
    "how",
    "",
}


def classify_node(node: str, ner_label: str = None) -> str:
    """Classify a single node into one of: concept, entity, metadata, citation, noise.
    
    Args:
        node: node text
        ner_label: optional spaCy NER label from Phase-2 processing
    
    Returns: classification string
    """
    if not node or not isinstance(node, str):
        return "noise"
    
    n = node.strip()
    if not n:
        return "noise"
    
    # Noise blacklist (stop words, empty)
    if n.lower() in NOISE_BLACKLIST:
        return "noise"
    
    # Noise patterns
    for pattern in NOISE_PATTERNS:
        if re.match(pattern, n):
            return "noise"
    
    # Metadata: identifiers and references
    if re.match(YEAR_PATTERN, n):
        return "metadata"
    if re.match(DOI_PATTERN, n, re.I):
        return "metadata"
    if re.match(ISBN_PATTERN, n):
        return "metadata"
    if re.match(ISSN_PATTERN, n):
        return "metadata"
    if re.match(URL_PATTERN, n):
        return "metadata"
    if re.match(ARXIV_PATTERN, n, re.I):
        return "metadata"
    if re.match(PMID_PATTERN, n, re.I):
        return "metadata"
    if re.match(NUMERIC_ONLY_PATTERN, n) and len(n) <= 5:
        return "metadata"
    if re.match(UUID_LIKE_PATTERN, n):
        return "metadata"
    
    # Entity: NER label, acronym, or proper case
    if ner_label and ner_label in ENTITY_NER_LABELS:
        return "entity"
    if re.match(ACRONYM_PATTERN, n):
        return "entity"
    if n[0].isupper() and len(n) > 1:  # capitalized (potential proper noun)
        return "entity"
    
    # Citation-only
    if any(kw in n.lower() for kw in CITATION_KEYWORDS):
        return "citation"
    
    # Concept: in allow-list or looks like abstract noun
    if n.lower() in CONCEPT_ALLOW_LIST:
        return "concept"
    
    # Default to concept if none of the above (conservative fallback)
    return "concept"
