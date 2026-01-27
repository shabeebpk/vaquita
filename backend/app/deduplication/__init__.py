"""
Deduplication module: duplicate detection and fingerprinting.

Separated from fetching to be reusable for both fetched and uploaded documents.
Enforces ordered hierarchy: DOI → external IDs → fingerprints.
Never depends on fetching, signals, or learning logic.
"""
from app.deduplication.fingerprinting import (
    FingerprintConfig,
    normalize_text,
    compute_fingerprint,
    hamming_distance,
    fingerprint_similarity,
    fingerprints_match,
)
from app.deduplication.detector import (
    DuplicateDetectionResult,
    check_doi_duplicate,
    check_external_id_duplicate,
    check_fingerprint_duplicate,
    check_duplicate,
    persist_paper,
)

__all__ = [
    "FingerprintConfig",
    "normalize_text",
    "compute_fingerprint",
    "hamming_distance",
    "fingerprint_similarity",
    "fingerprints_match",
    "DuplicateDetectionResult",
    "check_doi_duplicate",
    "check_external_id_duplicate",
    "check_fingerprint_duplicate",
    "check_duplicate",
    "persist_paper",
]
