"""Strategic Paper Downloader: Targeted extraction of literature.

Handles streaming downloads of PDFs prioritized by impact score,
records them in the strategic ledger, and prepares them for ingestion.
"""

import logging
import os
import httpx
import time
from typing import List, Optional, Dict, Any
from pathlib import Path
from sqlalchemy.orm import Session
from sqlalchemy import desc

from app.storage.db import engine
from app.storage.models import (
    Job, Paper, File, IngestionSource, 
    JobPaperEvidence, IngestionSourceType
)
from app.ingestion.extractor import extract_text_from_file
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)


class PaperDownloader:
    """
    Orchestrator for strategic paper downloading.
    
    Responsibilities:
    1. Identify top-impact papers needing evaluation for a job.
    2. Stream PDF content from URLs with robust retries.
    3. Save to downloads/<jobid>/original/ with impact score naming.
    4. Register File and IngestionSource entries.
    5. Update Strategic Ledger status.
    """

    def __init__(self, base_storage_dir: str = "downloads"):
        self.base_dir = Path(base_storage_dir)
        self.max_retries = admin_policy.query_orchestrator.fetch_params.retry_attempts
        self.timeout = admin_policy.query_orchestrator.fetch_params.timeout_seconds

    def process_job_downloads(self, job_id: int):
        """
        Process pending downloads for a job, prioritized by impact score.
        """
        logger.info(f"Starting strategic download for job {job_id}")
        
        with Session(engine) as session:
            # 1. Fetch pending papers from Strategic Ledger
            # Prioritize by impact_score desc
            pending = session.query(JobPaperEvidence).filter(
                JobPaperEvidence.job_id == job_id,
                JobPaperEvidence.evaluated == False
            ).order_by(desc(JobPaperEvidence.impact_score)).all()

            if not pending:
                logger.info(f"No pending papers to download for job {job_id}")
                return 0

            downloaded_count = 0
            for evidence in pending:
                paper = session.query(Paper).get(evidence.paper_id)
                if not paper or not paper.pdf_url:
                    logger.warning(f"Paper {evidence.paper_id} has no URL or not found; skipping.")
                    evidence.evaluated = True # Mark as "processed" even if skipped to avoid infinite loops
                    session.commit()
                    continue

                success = self._download_and_register(session, job_id, paper, evidence)
                if success:
                    downloaded_count += 1
                
                # Mark as evaluated in the ledger
                evidence.evaluated = True
                session.commit()

            logger.info(f"Completed downloads for job {job_id}. Total: {downloaded_count}")
            return downloaded_count

    def _download_and_register(
        self, 
        session: Session, 
        job_id: int, 
        paper: Paper, 
        evidence: JobPaperEvidence
    ) -> bool:
        """
        Internal helper to download, store, and register a single paper.
        """
        # Create storage directory
        job_dir = self.base_dir / str(job_id) / "original"
        job_dir.mkdir(parents=True, exist_ok=True)

        # File naming convention: <impact_score>_<paper_id>.pdf
        safe_title = "".join([c if c.isalnum() else "_" for c in paper.title[:30]])
        filename = f"{int(evidence.impact_score)}_{paper.id}_{safe_title}.pdf"
        file_path = job_dir / filename

        # Download with retries
        try:
            download_success = self._stream_download(paper.pdf_url, str(file_path))
            if not download_success:
                return False

            # Extract text
            try:
                raw_text = extract_text_from_file(str(file_path), "pdf")
            except Exception as e:
                logger.error(f"Text extraction failed for {file_path}: {e}")
                raw_text = "" # Fallback to empty if extraction fails but download succeeded

            # Register File record
            file_record = File(
                job_id=job_id,
                paper_id=paper.id,
                origin_type="paper_download",
                stored_path=str(file_path),
                original_filename=filename,
                file_type="pdf"
            )
            session.add(file_record)
            session.flush()

            # Register IngestionSource
            source = IngestionSource(
                job_id=job_id,
                source_type=IngestionSourceType.PDF_TEXT.value,
                source_ref=f"file:{file_record.id}",
                raw_text=raw_text,
                processed=False
            )
            session.add(source)
            
            logger.info(f"Registered paper {paper.id} as File {file_record.id} and IngestionSource {source.id}")
            return True

        except Exception as e:
            logger.error(f"Failed to process download for paper {paper.id}: {e}")
            return False

    def _stream_download(self, url: str, target_path: str) -> bool:
        """Stream download from URL to file with retries."""
        for attempt in range(self.max_retries):
            try:
                with httpx.stream("GET", url, follow_redirects=True, timeout=self.timeout) as response:
                    if response.status_code != 200:
                        logger.warning(f"Failed to download {url}: Status {response.status_code} (Attempt {attempt+1})")
                        time.sleep(2)
                        continue

                    with open(target_path, "wb") as f:
                        for chunk in response.iter_bytes(chunk_size=8192):
                            f.write(chunk)
                
                logger.info(f"Successfully downloaded {url} to {target_path}")
                return True

            except Exception as e:
                logger.warning(f"Download error for {url}: {e} (Attempt {attempt+1})")
                time.sleep(2)

        logger.error(f"Max retries exceeded for {url}")
        return False


def get_paper_downloader() -> PaperDownloader:
    """Helper to get downloader instance."""
    return PaperDownloader()
