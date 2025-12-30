"""
Unified ingestion service: orchestrates extraction, aggregation, normalization, and segmentation.
This is the single gate for all text processing in the system.
"""
import logging
from typing import List, Tuple, Optional
from sqlalchemy.orm import Session

from app.ingestion.extractors import DocumentExtractor
from app.ingestion.normalizer import TextNormalizer
from app.ingestion.aggregator import TextAggregator
from app.ingestion.segmenter import TextSegmenter
from app.ingestion.lexical import lexical_repair
from app.storage.models import IngestionSource, NormalizedText, TextBlock
from app.storage.db import engine

logger = logging.getLogger(__name__)


class IngestionService:
    """
    Unified ingestion service: extract → aggregate → normalize → segment.
    All text enters here; exits as canonical normalized blocks in DB.
    """

    @staticmethod
    def ingest_job(
        job_id: int,
        user_text: str,
        file_paths: List[Tuple[str, str]],  # [(path, file_type), ...]
        segmentation_strategy: str = "sentences",
        segmentation_kwargs: Optional[dict] = None,
        normalization_kwargs: Optional[dict] = None,
    ) -> dict:
        """
        Full ingestion pipeline for a job.
        
        Args:
            job_id: ID of the job
            user_text: user's query/instruction
            file_paths: list of (file_path, file_type) tuples
                E.g., [('/path/to/paper.pdf', 'pdf'), ('/path/to/doc.docx', 'docx')]
            segmentation_strategy: 'sentences', 'paragraphs', 'length', 'sections'
            segmentation_kwargs: strategy-specific params (sentences_per_block, block_length, etc.)
            normalization_kwargs: config for TextNormalizer.normalize()
        
        Returns:
            dict with keys:
                - job_id
                - user_text
                - ingestion_sources (count)
                - canonical_text_length
                - text_blocks (count)
                - blocks (sample of first 3)
        """
        if segmentation_kwargs is None:
            segmentation_kwargs = {}
        if normalization_kwargs is None:
            normalization_kwargs = {}

        logger.info(f"Starting ingestion for job {job_id}")

        with Session(engine) as session:
            # Step 1: Extract text from all files
            logger.info(f"Extracting text from {len(file_paths)} files")
            extracted_texts = []
            for file_path, file_type in file_paths:
                try:
                    extracted_chunks = DocumentExtractor.extract_from_file(file_path, file_type)
                    # Combine chunks from same file into one text
                    combined_text = ' '.join([chunk[0] for chunk in extracted_chunks])

                    # Run conservative lexical repair to fix layout-induced splits
                    try:
                        combined_text = lexical_repair(combined_text)
                    except Exception as e:
                        logger.debug(f"Lexical repair failed for {file_path}: {e}")
                    source_ref = file_path.split('/')[-1]  # filename only
                    extracted_texts.append((file_type, source_ref, combined_text))
                    
                    logger.info(f"Extracted {len(combined_text)} chars from {source_ref}")
                except Exception as e:
                    logger.error(f"Failed to extract {file_path}: {str(e)}")
                    continue

            # Step 2: Store raw extracted texts in DB
            for source_type, source_ref, raw_text in extracted_texts:
                source = IngestionSource(
                    job_id=job_id,
                    source_type=source_type,
                    source_ref=source_ref,
                    raw_text=raw_text
                )
                session.add(source)
            session.commit()

            # Step 3: Normalize & Segment each source independently and store blocks with provenance
            logger.info("Normalizing and segmenting each source separately")

            # Refresh sources from DB so we have their IDs
            sources = session.query(IngestionSource).filter_by(job_id=job_id).all()

            normalized_sources = []  # list of (source_type, source_ref, normalized_text)
            global_block_count = 0

            for src in sources:
                try:
                    norm_text, extracted_urls = TextNormalizer.normalize(src.raw_text, **normalization_kwargs)
                    # Persist extracted URLs with the source for provenance
                    try:
                        src.extracted_urls = extracted_urls
                        session.add(src)
                    except Exception:
                        logger.debug("Could not persist extracted_urls on source row")

                    normalized_sources.append((src.source_type, src.source_ref, norm_text))

                    # Segment per-source normalized text
                    blocks = TextSegmenter.segment(
                        norm_text,
                        strategy=segmentation_strategy,
                        **segmentation_kwargs
                    )

                    # Persist blocks with source_id linkage
                    for rel_order, block_text in enumerate(blocks, 1):
                        global_block_count += 1
                        text_block = TextBlock(
                            job_id=job_id,
                            block_text=block_text,
                            block_order=global_block_count,
                            source_id=src.id,
                            block_type="text_block",
                            segmentation_strategy=segmentation_strategy
                        )
                        session.add(text_block)
                except Exception as e:
                    logger.error(f"Failed to normalize/segment source {src.source_ref}: {str(e)}")
                    continue

            session.commit()

            logger.info(f"Ingestion complete: {global_block_count} blocks created across {len(sources)} sources")

            # Step 4: Aggregate normalized source texts (plus user text) into canonical text
            canonical_text, agg_metadata = TextAggregator.aggregate_with_metadata(
                user_text,
                normalized_sources
            )

            # Store canonical normalized text (one per job)
            norm_config = TextNormalizer.get_normalization_config(**normalization_kwargs)
            normalized = NormalizedText(
                job_id=job_id,
                canonical_text=canonical_text,
                source_count=len(normalized_sources),
                normalization_config=norm_config
            )
            session.add(normalized)
            session.commit()

            # Return summary
            return {
                "job_id": job_id,
                "user_text": user_text[:100] + "..." if len(user_text) > 100 else user_text,
                "ingestion_sources": len(extracted_texts),
                "canonical_text_length": len(canonical_text),
                "text_blocks": global_block_count,
                "segmentation_strategy": segmentation_strategy
            }

    @staticmethod
    def get_blocks_for_job(job_id: int) -> List[dict]:
        """
        Retrieve all text blocks for a job.
        
        Returns:
            list of dicts with keys: id, block_text, block_order, block_type
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
                    "block_type": b.block_type
                }
                for b in blocks
            ]

    @staticmethod
    def get_canonical_text(job_id: int) -> Optional[str]:
        """Retrieve canonical normalized text for a job."""
        with Session(engine) as session:
            normalized = session.query(NormalizedText).filter(
                NormalizedText.job_id == job_id
            ).first()
            
            return normalized.canonical_text if normalized else None
