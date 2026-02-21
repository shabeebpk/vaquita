"""Paper Definition and Metrics for the System.

This module defines what constitutes a "paper" in the system and how papers are counted
for various metrics and constraints.
"""

"""
PAPER DEFINITION
================

In this system, a "Paper" is a scholarly work (journal article, conference paper, preprint, etc.)
identified uniquely by:
- DOI (Digital Object Identifier) - unique globally when available
- External ID combination (ArXiv ID, PubMed ID, etc.) - when DOI unavailable
- Title + Authors + Year - fallback when neither DOI nor external ID available

Papers are stored in the `papers` table with metadata including:
- title, abstract, authors, year, venue, doi, external_ids, fingerprint, source, pdf_url


PAPER COUNTING METRICS
======================

1. TOTAL PAPERS PER JOB (JobPaperEvidence.count())
   -----------------------------------------------
   Definition: Count of unique papers associated with a job.
   
   Stored in: JobPaperEvidence table (one row per unique paper per job)
   Key field: paper_id (foreign key to papers.id)
   
   Constraint: SYSTEM_MAX_PAPERS_PER_JOB (environment variable, default: 100)
   
   When limit reached: System halts job with COMPLETED status (HALT_NO_HYPOTHESIS behavior)
   
   Usage:
   - Checked before setting FETCH_QUEUED in fetch_more_literature handler
   - Checked before falling back to FETCH_QUEUED in strategic_download handler
   - Enforces finite bounds on job resource consumption

   Query: SELECT COUNT(DISTINCT paper_id) FROM job_paper_evidence WHERE job_id = ?


2. PAPERS WITH CONTRIBUTION (JobPaperEvidence.evaluated = True)
   -------------------------------------------------------------
   Definition: Papers that have been downloaded and extracted (triples extracted).
   
   Represents: Papers that actively contributed knowledge to the job's hypotheses and graph.
   
   Stored in: JobPaperEvidence.evaluated = True
   
   Usage:
   - Metric for measuring "signal"
   - Indicates data quality / utility of fetched papers
   - Higher value = more useful papers; lower value = more noise/duplicates
   
   Query: SELECT COUNT(*) FROM job_paper_evidence 
          WHERE job_id = ? AND evaluated = True


3. NEW PAPERS IN CURRENT FETCH RUN (run-specific)
   -----------------------------------------------
   Definition: Papers fetched in current batch that are new to this job.
   
   Represents: Incremental progress; papers not seen before for this job.
   
   Stored in: JobPaperEvidence.run_id (links to SearchQueryRun)
   
   Uniqueness: Tracked via seen_ids set during fetch to ensure no duplicates within job.
   
   Usage:
   - Decision signal: "Did fetch produce new papers?"
   - If fetch returns 0 new papers (all duplicates): proceed to decision layer
   - If fetch returns new papers: may trigger strategic download instead of halt


4. UNDOWNLOADED PAPERS (JobPaperEvidence.evaluated = False)
   --------------------------------------------------------
   Definition: Papers in the ledger but not yet processed.
   
   Represents: Backlog of papers awaiting download/extraction.
   
   Usage:
   - Strategic download handler checks this to decide DOWNLOAD_QUEUED vs FETCH_QUEUED
   - High value = plenty of work to do; don't fetch more
   - Zero value + papers exist = need to fetch more


PAPER LIFECYCLE IN A JOB
========================

1. DISCOVERED via Fetch Provider (semantic scholar, etc.)
   - Paper record created in `papers` table
   - Deduplication check (fingerprint)
   - JobPaperEvidence entry created with: job_id, run_id, paper_id, evaluated=False

2. QUEUED FOR DOWNLOAD/EXTRACTION
   - In JobPaperEvidence with evaluated=False
   - Awaits ingestion pipeline

3. DOWNLOADED AND EXTRACTED
   - JobPaperEvidence.evaluated = True
   - Triples extracted and stored in `triples` table
   - Contributes to semantic graph

4. COUNTED IN METRICS
   - Total papers = rows in JobPaperEvidence for job_id
   - Contributing papers = JobPaperEvidence with evaluated=True
   - Contributes to measurements: passed_hypothesis_count, growth_score, etc.

5. AUDITED POST-JOB
   - JobPaperEvidence is append-only and immutable
   - Provides audit trail of all papers considered for a job


SYSTEM CONSTRAINTS
==================

SYSTEM_MAX_PAPERS_PER_JOB (default: 100)
- Hard limit on papers per job
- Enforced before FETCH_QUEUED is set
- Prevents runaway fetching
- When reached: job status = COMPLETED, decision = HALT_NO_HYPOTHESIS

This ensures jobs are bounded and predictable in resource consumption.
"""

def get_paper_count_for_job(job_id: int, session) -> int:
    """Get total paper count for a job.
    
    Args:
        job_id: Job ID
        session: SQLAlchemy session
    
    Returns:
        Count of distinct papers in JobPaperEvidence for this job
    """
    from app.storage.models import JobPaperEvidence
    return session.query(JobPaperEvidence).filter(
        JobPaperEvidence.job_id == job_id
    ).count()


def get_papers_with_contribution(job_id: int, session) -> int:
    """Get count of papers with extracted triples (evaluated).
    
    Args:
        job_id: Job ID
        session: SQLAlchemy session
    
    Returns:
        Count of papers marked as evaluated=True
    """
    from app.storage.models import JobPaperEvidence
    return session.query(JobPaperEvidence).filter(
        JobPaperEvidence.job_id == job_id,
        JobPaperEvidence.evaluated == True
    ).count()


def get_undownloaded_papers(job_id: int, session) -> int:
    """Get count of papers not yet downloaded/extracted.
    
    Args:
        job_id: Job ID
        session: SQLAlchemy session
    
    Returns:
        Count of papers marked as evaluated=False
    """
    from app.storage.models import JobPaperEvidence
    return session.query(JobPaperEvidence).filter(
        JobPaperEvidence.job_id == job_id,
        JobPaperEvidence.evaluated == False
    ).count()
