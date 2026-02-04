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

from app.ingestion.normalizer import TextNormalizer
from app.ingestion.segmenter import TextSegmenter
from app.storage.models import Job, IngestionSource, TextBlock
from app.storage.db import engine

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Processes IngestionSource rows in a loop per job.
    
    Workflow:
    1. Query database for all unprocessed IngestionSource rows for a job
    2. For each source (in creation order):
       a. Read raw_text
       b. Normalize in-memory (not stored separately)
       c. Segment normalized text into blocks
       d. Create TextBlock rows linked to the source
       e. Mark source as processed = true
    3. Repeat until no unprocessed sources remain
    4. Set job status to INGESTED
    
    This design ensures idempotency and supports iterative expansion
    (new sources added later trigger another ingestion pass).
    """

    @staticmethod
    def ingest_job(
        job_id: int,
        segmentation_strategy: str = "sentences",
        segmentation_kwargs: Optional[dict] = None,
        normalization_kwargs: Optional[dict] = None,
    ) -> dict:
        """
        Process all unprocessed IngestionSource rows for a job in a loop.
        
        Responsibility: Execute text processing only. Do not decide pipeline flow,
        trigger other phases, or infer next actions. Caller (runner.py) controls
        when this is invoked and what happens next.
        
        Args:
            job_id: ID of the job to ingest
            segmentation_strategy: 'sentences', 'paragraphs', 'length', 'sections'
            segmentation_kwargs: strategy-specific parameters
            normalization_kwargs: configuration for text normalization
        
        Returns:
            dict with summary:
                - job_id
                - sources_processed: count of IngestionSource rows processed
                - blocks_created: total TextBlock rows created
        
        Raises:
            ValueError: if job not found
            RuntimeError: if job status is not exactly READY_TO_INGEST
        """
        if segmentation_kwargs is None:
            segmentation_kwargs = {"sentences_per_block": 3}
        if normalization_kwargs is None:
            normalization_kwargs = {}

        logger.info(f"Starting ingestion loop for job {job_id}")

        with Session(engine) as session:
            # Verify job exists and status is exactly READY_TO_INGEST (hard fail)
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")
            
            if job.status != "READY_TO_INGEST":
                raise RuntimeError(
                    f"Cannot ingest job {job_id}: status is '{job.status}', expected 'READY_TO_INGEST'. "
                    "Caller must ensure job is in correct state before invoking ingestion."
                )
            
            # Load defaults from job_config if not provided
            ingest_config = job.job_config.get("expert_settings", {}).get("ingestion", {})
            
            if segmentation_kwargs is None:
                segmentation_kwargs = {
                    "sentences_per_block": int(ingest_config.get("sentences_per_block", 3))
                }
            
            if normalization_kwargs is None:
                normalization_kwargs = {
                    "apply_lexical_repair": bool(ingest_config.get("enable_lexical_repair", False))
                }

            sources_processed = 0
            blocks_created = 0

            # Ingestion loop: process unprocessed sources in order
            while True:
                # Query for the next unprocessed source (order by creation time)
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
                    # No more unprocessed sources; ingestion is complete
                    logger.info(f"No more unprocessed sources for job {job_id}")
                    break

                logger.info(
                    f"Processing IngestionSource {unprocessed_source.id} "
                    f"(source_type={unprocessed_source.source_type}, "
                    f"source_ref={unprocessed_source.source_ref})"
                )

                try:
                    # Step 1: Normalize in-memory (no storage)
                    normalized_text, extracted_urls = TextNormalizer.normalize(
                        unprocessed_source.raw_text,
                        **normalization_kwargs
                    )
                    logger.debug(
                        f"Normalized {len(unprocessed_source.raw_text)} chars → "
                        f"{len(normalized_text)} chars, "
                        f"extracted {len(extracted_urls)} URLs"
                    )

                    # Step 2: Segment normalized text into blocks
                    blocks = IngestionService._segment_text(
                        normalized_text,
                        segmentation_strategy,
                        **segmentation_kwargs
                    )
                    logger.info(
                        f"Segmented into {len(blocks)} blocks using '{segmentation_strategy}'"
                    )

                    # Step 3: Create TextBlock rows with provenance
                    for block_index, block_text in enumerate(blocks, 1):
                        text_block = TextBlock(
                            job_id=job_id,
                            ingestion_source_id=unprocessed_source.id,
                            block_text=block_text,
                            block_order=block_index,
                            block_type="text_block",
                            segmentation_strategy=segmentation_strategy,
                            triples_extracted=False
                        )
                        session.add(text_block)
                        blocks_created += 1

                    # Step 4: Mark source as processed and commit immediately
                    unprocessed_source.processed = True
                    session.add(unprocessed_source)
                    session.commit()
                    sources_processed += 1

                    logger.info(
                        f"IngestionSource {unprocessed_source.id} processed: "
                        f"{len(blocks)} blocks created and committed"
                    )

                except Exception as e:
                    logger.error(
                        f"Failed to process IngestionSource {unprocessed_source.id}: {str(e)}",
                        exc_info=True
                    )
                    session.rollback()
                    # Continue to next source rather than crashing
                    continue

            # Ingestion complete. Caller (runner.py) decides next status and phase.
            logger.info(
                f"Ingestion complete for job {job_id}: "
                f"{sources_processed} sources processed, {blocks_created} blocks created"
            )

            return {
                "job_id": job_id,
                "sources_processed": sources_processed,
                "blocks_created": blocks_created
            }

    @staticmethod
    def _segment_text(
        text: str,
        strategy: str = "sentences",
        **kwargs
    ) -> list:
        """
        Delegate to TextSegmenter based on strategy.
        
        Args:
            text: normalized text
            strategy: 'sentences', 'paragraphs', 'length', 'sections'
            **kwargs: strategy-specific parameters
        
        Returns:
            list of text blocks
        
        Raises:
            ValueError: if strategy not supported
        """
        if strategy == "sentences":
            return TextSegmenter.segment_by_sentences(
                text,
                sentences_per_block=kwargs.get("sentences_per_block", 3)
            )
        elif strategy == "paragraphs":
            return TextSegmenter.segment_by_paragraphs(
                text,
                min_para_length=kwargs.get("min_para_length", 50)
            )
        elif strategy == "length":
            return TextSegmenter.segment_by_length(
                text,
                block_length=kwargs.get("block_length", 300),
                overlap=kwargs.get("overlap", 50)
            )
        elif strategy == "sections":
            return TextSegmenter.segment_by_sections(
                text,
                section_markers=kwargs.get("section_markers", None)
            )
        else:
            raise ValueError(f"Unknown segmentation strategy: {strategy}")

    @staticmethod
    def get_blocks_for_job(job_id: int) -> list:
        """
        Retrieve all TextBlock rows for a job.
        
        Returns:
            list of dicts with keys: id, block_text, block_order, block_type, source_id
        """
        with Session(engine) as session:
            blocks = session.query(TextBlock).filter(
                TextBlock.job_id == job_id
            ).order_by(TextBlock.block_order).all()

            return [
                {
                    "id": b.id,
                    "block_text": b.block_text,
                    "block_order": b.block_order,
                    "block_type": b.block_type,
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

