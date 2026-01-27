"""
Ordered duplicate detection hierarchy: DOI → external IDs → fingerprints.

Reusable across fetched papers and user-uploaded documents.
Rejected candidates are logged but never ingested.
"""
import logging
from typing import Dict, Any, Optional
from sqlalchemy.orm import Session

from app.deduplication.fingerprinting import (
    compute_fingerprint, fingerprints_match, FingerprintConfig
)
from app.storage.models import Paper

logger = logging.getLogger(__name__)


class DuplicateDetectionResult:
    """Result of duplicate detection check."""
    
    def __init__(
        self,
        is_duplicate: bool,
        match_type: Optional[str] = None,
        matched_paper_id: Optional[int] = None,
        confidence: float = 0.0,
        reason: str = ""
    ):
        self.is_duplicate = is_duplicate
        self.match_type = match_type  # 'doi', 'external_id', 'fingerprint', 'semantic'
        self.matched_paper_id = matched_paper_id  # ID of matching paper in DB
        self.confidence = confidence  # 0.0-1.0 confidence in match
        self.reason = reason  # Human-readable explanation


def check_doi_duplicate(
    candidate: Dict[str, Any],
    session: Session
) -> Optional[DuplicateDetectionResult]:
    """
    Check if candidate DOI matches an existing paper.
    
    Args:
        candidate: Paper dict with 'doi' field
        session: SQLAlchemy session
    
    Returns:
        DuplicateDetectionResult if match found, None otherwise
    """
    doi = candidate.get("doi")
    if not doi:
        return None
    
    doi = doi.strip().lower()
    existing = session.query(Paper).filter(
        Paper.doi.ilike(doi)
    ).first()
    
    if existing:
        logger.info(f"Found duplicate by DOI: {doi} (paper_id={existing.id})")
        return DuplicateDetectionResult(
            is_duplicate=True,
            match_type="doi",
            matched_paper_id=existing.id,
            confidence=1.0,
            reason=f"DOI match: {doi}"
        )
    
    return None


def check_external_id_duplicate(
    candidate: Dict[str, Any],
    session: Session
) -> Optional[DuplicateDetectionResult]:
    """
    Check if candidate external identifiers (arXiv, PubMed, etc.) match existing paper.
    
    Args:
        candidate: Paper dict with 'external_ids' field (dict of id_type -> id_value)
        session: SQLAlchemy session
    
    Returns:
        DuplicateDetectionResult if match found, None otherwise
    """
    external_ids = candidate.get("external_ids")
    if not external_ids or not isinstance(external_ids, dict):
        return None
    
    # Search for papers with matching external IDs
    for id_type, id_value in external_ids.items():
        if not id_value:
            continue
        
        id_value = str(id_value).strip().lower()
        
        # Query papers where external_ids contains this id_type:id_value
        existing_papers = session.query(Paper).all()  # Fetch all for JSON search
        
        for paper in existing_papers:
            if not paper.external_ids:
                continue
            
            paper_id_value = paper.external_ids.get(id_type, "")
            if isinstance(paper_id_value, str):
                paper_id_value = paper_id_value.lower()
            
            if str(paper_id_value) == id_value:
                logger.info(
                    f"Found duplicate by external ID: {id_type}={id_value} (paper_id={paper.id})"
                )
                return DuplicateDetectionResult(
                    is_duplicate=True,
                    match_type="external_id",
                    matched_paper_id=paper.id,
                    confidence=0.95,
                    reason=f"External ID match: {id_type}={id_value}"
                )
    
    return None


def check_fingerprint_duplicate(
    candidate: Dict[str, Any],
    session: Session,
    config: Optional[FingerprintConfig] = None
) -> Optional[DuplicateDetectionResult]:
    """
    Check if candidate content fingerprint matches existing paper.
    
    Args:
        candidate: Paper dict with 'title', 'abstract', 'authors'
        session: SQLAlchemy session
        config: FingerprintConfig (created if None)
    
    Returns:
        DuplicateDetectionResult if match found, None otherwise
    """
    if config is None:
        config = FingerprintConfig()
    
    candidate_fp = compute_fingerprint(candidate, config)
    if not candidate_fp:
        return None
    
    # Query existing papers and compare fingerprints
    existing_papers = session.query(Paper).filter(
        Paper.fingerprint.isnot(None)
    ).all()
    
    for paper in existing_papers:
        if fingerprints_match(candidate_fp, paper.fingerprint, config):
            logger.info(
                f"Found duplicate by fingerprint: {candidate_fp} ≈ {paper.fingerprint} "
                f"(paper_id={paper.id})"
            )
            return DuplicateDetectionResult(
                is_duplicate=True,
                match_type="fingerprint",
                matched_paper_id=paper.id,
                confidence=0.90,
                reason=f"Content fingerprint match"
            )
    
    return None


def check_duplicate(
    candidate: Dict[str, Any],
    session: Session,
    config: Optional[FingerprintConfig] = None
) -> DuplicateDetectionResult:
    """
    Unified duplicate detection using strict ordered hierarchy.
    
    Order:
    1. DOI matching
    2. External identifiers
    3. Content fingerprints
    (Semantic similarity can be added as optional step 4)
    
    Args:
        candidate: Paper dict
        session: SQLAlchemy session
        config: FingerprintConfig (created if None)
    
    Returns:
        DuplicateDetectionResult with is_duplicate=True if match, False otherwise
    """
    # Step 1: DOI
    result = check_doi_duplicate(candidate, session)
    if result:
        return result
    
    # Step 2: External IDs
    result = check_external_id_duplicate(candidate, session)
    if result:
        return result
    
    # Step 3: Fingerprint
    result = check_fingerprint_duplicate(candidate, session, config)
    if result:
        return result
    
    # No duplicate found
    logger.debug(f"No duplicate found for paper: {candidate.get('title', 'N/A')}")
    return DuplicateDetectionResult(
        is_duplicate=False,
        reason="No duplicate detected"
    )


def persist_paper(
    candidate: Dict[str, Any],
    session: Session,
    config: Optional[FingerprintConfig] = None
) -> Paper:
    """
    Store a paper in the database with computed fingerprint.
    Assumes deduplication check already passed.
    
    Args:
        candidate: Paper dict
        session: SQLAlchemy session
        config: FingerprintConfig (created if None)
    
    Returns:
        Persisted Paper model instance
    """
    if config is None:
        config = FingerprintConfig()
    
    # Compute fingerprint
    fingerprint = compute_fingerprint(candidate, config)
    
    # Extract fields
    paper = Paper(
        title=candidate.get("title", ""),
        abstract=candidate.get("abstract"),
        authors=candidate.get("authors"),
        year=candidate.get("year"),
        venue=candidate.get("venue"),
        doi=candidate.get("doi"),
        external_ids=candidate.get("external_ids"),
        fingerprint=fingerprint,
        source=candidate.get("source", "unknown"),
        pdf_url=candidate.get("pdf_url")
    )
    
    session.add(paper)
    session.flush()  # Flush to get paper.id without committing
    
    logger.info(f"Persisted paper: {paper.title[:50]}... (id={paper.id})")
    
    return paper
