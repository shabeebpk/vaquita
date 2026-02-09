"""
Content-based fingerprinting for paper identification.

Generates configurable hashes of paper content for deduplication and identification.
Supports multiple algorithms (MD5, SHA256) and handles partial abstracts/titles.

Separated from fetching: reusable for both fetched and uploaded documents.
"""
import hashlib
import logging
import os
from typing import Optional, Dict, Any

logger = logging.getLogger(__name__)


class FingerprintConfig:
    """Configuration for fingerprinting behavior."""
    
    def __init__(self):
        from app.config.admin_policy import admin_policy
        
        config = admin_policy.algorithm.deduplication_defaults
        
        # Algorithm: 'md5', 'sha256'
        self.algorithm = config.algorithm.lower()
        
        # Threshold: minimum similarity score (0.0-1.0)
        self.similarity_threshold = config.similarity_threshold
        
        # Components to include in fingerprint
        self.components = config.components
        
        logger.info(
            f"FingerprintConfig: algorithm={self.algorithm}, "
            f"components={self.components}, threshold={self.similarity_threshold}"
        )


def normalize_text(text: Optional[str]) -> str:
    """Normalize text for fingerprinting: lowercase, strip whitespace, remove punctuation."""
    if not text:
        return ""
    
    # Lowercase
    text = text.lower()
    # Strip leading/trailing whitespace
    text = text.strip()
    # Remove common punctuation (keep alphanumeric and spaces)
    text = "".join(c if c.isalnum() or c.isspace() else "" for c in text)
    # Collapse multiple spaces
    text = " ".join(text.split())
    
    return text


def compute_fingerprint(paper: Dict[str, Any], config: Optional[FingerprintConfig] = None) -> str:
    """
    Compute content-based fingerprint of a paper.
    
    Args:
        paper: Dict with keys like 'title', 'abstract', 'authors'
        config: FingerprintConfig instance (created if None)
    
    Returns:
        Hex string fingerprint using configured algorithm
    """
    if config is None:
        config = FingerprintConfig()
    
    # Extract and normalize components
    components_text = []
    
    if "title" in config.components and paper.get("title"):
        components_text.append(normalize_text(paper["title"]))
    
    if "abstract" in config.components and paper.get("abstract"):
        components_text.append(normalize_text(paper["abstract"]))
    
    if "authors" in config.components and paper.get("authors"):
        # Normalize author list (concatenate normalized author names)
        authors = paper["authors"]
        if isinstance(authors, list):
            author_names = " ".join(
                normalize_text(a.get("name", "") if isinstance(a, dict) else str(a))
                for a in authors
            )
            components_text.append(author_names)
        elif isinstance(authors, str):
            components_text.append(normalize_text(authors))
    
    # Combine normalized components
    combined = " | ".join(components_text)
    
    # Compute hash using configured algorithm
    if config.algorithm == "md5":
        hash_obj = hashlib.md5(combined.encode("utf-8"))
    else:  # Default to sha256
        hash_obj = hashlib.sha256(combined.encode("utf-8"))
    
    fingerprint = hash_obj.hexdigest()
    logger.debug(f"Computed fingerprint for paper '{paper.get('title', 'N/A')}': {fingerprint}")
    
    return fingerprint


def hamming_distance(fp1: str, fp2: str) -> int:
    """
    Compute Hamming distance between two hex fingerprints.
    Returns number of differing bits.
    """
    if len(fp1) != len(fp2):
        return max(len(fp1), len(fp2))  # Max distance if different lengths
    
    distance = 0
    for c1, c2 in zip(fp1, fp2):
        if c1 != c2:
            distance += 1
    
    return distance


def fingerprint_similarity(fp1: str, fp2: str) -> float:
    """
    Compute similarity (0.0-1.0) between two fingerprints.
    Uses Hamming distance normalized by fingerprint length.
    """
    if not fp1 or not fp2:
        return 0.0
    
    max_len = max(len(fp1), len(fp2))
    if max_len == 0:
        return 1.0
    
    distance = hamming_distance(fp1, fp2)
    similarity = 1.0 - (distance / (max_len * 4))  # Normalize to [0,1]
    
    return max(0.0, min(1.0, similarity))  # Clamp to [0,1]


def fingerprints_match(fp1: str, fp2: str, config: Optional[FingerprintConfig] = None) -> bool:
    """
    Check if two fingerprints are equivalent according to threshold.
    
    Args:
        fp1, fp2: Hex fingerprint strings
        config: FingerprintConfig (created if None)
    
    Returns:
        True if similarity >= threshold, False otherwise
    """
    if config is None:
        config = FingerprintConfig()
    
    similarity = fingerprint_similarity(fp1, fp2)
    return similarity >= config.similarity_threshold
