"""
Ingestion service: processes IngestionSource rows in a loop for a job.

Responsibility: Read unprocessed IngestionSource rows, apply in-memory normalization,
segment into TextBlock rows with provenance, mark source as processed, and update job
status. No aggregation, no intent understanding, no dependencies on downstream phases.
This service is idempotent and safe to retry after failures.
"""
import logging
from typing import Optional
from sqlalchemy.orm import Session
from sqlalchemy import asc

from app.storage.models import Job, IngestionSource, TextBlock, File
from app.storage.db import engine
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Precision Ingestion Service.
    
    Responsibility: Coordinate the 4-layer Ingestion Pipeline:
    1. Adapter Layer (DLA/Physical Extraction) → returns regions only
    2. Refinery Layer (LLM Text Cleaning) → cleans region text
    3. RAW_TEXT ENFORCEMENT → concatenate and store to IngestionSource.raw_text (canonical)
    4. Slicing Layer (Sentence-aware Blocking) → slice from raw_text only
    5. Storage Layer (Persistence) → create TextBlock rows
    
    CONTRACT ENFORCEMENT:
    All extracted text MUST be written to IngestionSource.raw_text before slicing occurs.
    No adapter, caller, or refinery may bypass this column. This ensures a single source
    of truth for ingested content and maintains data integrity.
    """

    @staticmethod
    def ingest_job(job_id: int) -> dict:
        """
        Process all unprocessed IngestionSource rows for a job.
        
        This method strictly uses the AdminPolicy for all configurations.
        """
        logger.info(f"Starting Precision Ingestion loop for job {job_id}")

        with Session(engine) as session:
            # Verify job exists and status is exactly READY_TO_INGEST
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            if job.status != "READY_TO_INGEST":
                raise RuntimeError(f"Cannot ingest job {job_id}: status is '{job.status}', expected 'READY_TO_INGEST'.")

            sources_processed = 0
            blocks_created = 0

            while True:
                unprocessed_source = (
                    session.query(IngestionSource)
                    .filter(
                        IngestionSource.job_id == job_id,
                        IngestionSource.processed == False
                    )
                    .order_by(asc(IngestionSource.created_at))
                    .first()
                )

                if not unprocessed_source:
                    break

                logger.info(f"IngestionService: Processing Source {unprocessed_source.id} ({unprocessed_source.source_type}).")

                try:
                    # 1. Adapter Layer: Physical Extraction / DLA
                    from app.ingestion.adapters.factory import get_adapter_for_source
                    adapter = get_adapter_for_source(unprocessed_source.source_type, unprocessed_source.source_ref)
                    
                    # Decide input for adapter (Resolve file path or use raw text)
                    if "file:" in unprocessed_source.source_ref:
                        file_id_str = unprocessed_source.source_ref.replace("file:", "")
                        file_row = session.query(File).filter(File.id == int(file_id_str)).first()
                        if not file_row:
                            raise FileNotFoundError(f"Source {unprocessed_source.id} references missing file {file_id_str}.")
                        input_data = file_row.stored_path
                    else:
                        # For 'paper:ID' or 'user_text_...', we use the pre-extracted raw_text
                        input_data = unprocessed_source.raw_text or ""

                    regions = adapter.extract_regions(input_data, admin_policy.algorithm.extraction)
                    
                    # 2. Refinery Layer: LLM Polishing (Conditional)
                    refinery_config = admin_policy.algorithm.refinery
                    should_refine = unprocessed_source.source_type in refinery_config.needs_refinement_types
                    
                    refined_parts = []
                    if should_refine:
                        from app.ingestion.refinery.service import TextRefineryService
                        refinery = TextRefineryService()
                        for reg in regions:
                            word_count = len(reg.text.split())
                            logger.info(f"IngestionService: Refining gathered {reg.region_type} region ({word_count} words).")
                            logger.info(f"IngestionService: RAW CONTENT: {reg.text[:500]}...")
                            
                            clean_text = refinery.refine_text(reg.text)
                            if clean_text:
                                logger.info(f"IngestionService: CLEAN CONTENT: {clean_text[:500]}...")
                                refined_parts.append(clean_text)
                            else:
                                logger.warning(f"IngestionService: Refinery rejected {reg.region_type} span (Noise?).")
                    else:
                        logger.info(f"IngestionService: Skipping refinement for clean source type: {unprocessed_source.source_type}.")
                        refined_parts = [reg.text for reg in regions]
                    
                    full_text = "\n\n".join(refined_parts)

                    # ENFORCE: Write extracted/refined text back to raw_text (canonical storage)
                    # All adapters and extractors must populate this column before slicing
                    unprocessed_source.raw_text = full_text
                    session.add(unprocessed_source)
                    logger.info(f"IngestionService: Stored extracted text ({len(full_text)} chars) to raw_text for source {unprocessed_source.id}")

                    # 3. Slicing Layer: Sentence Integrity (reads from canonical raw_text)
                    from app.ingestion.slicing.service import SentenceSlicingService
                    slicer = SentenceSlicingService()
                    blocks = slicer.slice_text(unprocessed_source.raw_text)

                    # 4. Storage Layer: Persistence
                    for idx, b_text in enumerate(blocks, 1):
                        block = TextBlock(
                            job_id=job_id,
                            ingestion_source_id=unprocessed_source.id,
                            block_text=b_text,
                            block_order=idx,
                            block_type="text_block",
                            segmentation_strategy=admin_policy.algorithm.slicing.strategy,
                            triples_extracted=False
                        )
                        session.add(block)
                        blocks_created += 1

                    unprocessed_source.processed = True
                    session.add(unprocessed_source)
                    session.commit()
                    sources_processed += 1

                except Exception as e:
                    logger.error(f"IngestionService: Source {unprocessed_source.id} failed: {e}.", exc_info=True)
                    session.rollback()
                    continue

            return {
                "job_id": job_id,
                "sources_processed": sources_processed,
                "blocks_created": blocks_created
            }

    @staticmethod
    def get_blocks_for_job(job_id: int) -> list:
        """Retrieve all TextBlock rows for a job."""
        with Session(engine) as session:
            blocks = session.query(TextBlock).filter(
                TextBlock.job_id == job_id
            ).order_by(TextBlock.block_order).all()

            return [
                {
                    "id": b.id,
                    "block_text": b.block_text,
                    "block_order": b.block_order,
                    "ingestion_source_id": b.ingestion_source_id
                }
                for b in blocks
            ]

    @staticmethod
    def get_job_status(job_id: int) -> Optional[str]:
        """
        Retrieve current job status.
        
        Returns:
            status string or None if job not found
        """
        with Session(engine) as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            return job.status if job else None

