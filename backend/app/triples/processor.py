"""Processor to run triple extraction for all blocks of a job and persist triples.

This module is intentionally minimal: it iterates `TextBlock` rows for a job,
calls `TripleExtractor.extract`, and inserts rows into the `triples` table.
If an extraction returns None, it logs and moves on.
"""
import logging
from typing import List
from sqlalchemy.orm import Session
from datetime import datetime

from app.triples.extractor import TripleExtractor
from app.storage.db import engine
from app.storage.models import TextBlock, Triple

logger = logging.getLogger(__name__)


def process_job_triples(job_id: int, provider: str = None) -> dict:
    """Run extraction for each TextBlock of `job_id` where triples_extracted == false.

    Only processes blocks that haven't been extracted yet.
    Marks all processed blocks as triples_extracted = true (success or failure).
    Returns a summary dict: {job_id, blocks_processed, triples_created, failures}
    """
    extractor = TripleExtractor(provider_name=provider)
    created = 0
    failures = 0
    blocks_processed = 0

    with Session(engine) as session:
        # Query only unprocessed blocks
        blocks = session.query(TextBlock).filter(
            TextBlock.job_id == job_id,
            TextBlock.triples_extracted == False
        ).order_by(TextBlock.block_order).all()

        for block in blocks:
            blocks_processed += 1
            try:
                result = extractor.extract(block.block_text)
                logger.info(f"result to be saved in triple : {result}")
            except Exception as e:
                logger.error("Extraction exception for block %s: %s", block.id, e)
                result = None

            # If extraction succeeded, insert triples
            if result:
                triples = result.get("triples", [])
                for t in triples:
                    triple_row = Triple(
                        job_id=job_id,
                        block_id=block.id,
                        ingestion_source_id=block.ingestion_source_id,
                        subject=t["subject"].strip(),
                        predicate=t["predicate"].strip(),
                        object=t["object"].strip(),
                        extractor_name=extractor.provider_name or "llm",
                        created_at=datetime.utcnow()
                    )
                    session.add(triple_row)
                    created += 1
            else:
                failures += 1
                logger.info("No valid triples for block %s", block.id)

            # Mark block as extracted regardless of success or failure
            block.triples_extracted = True
            session.add(block)

        session.commit()

    summary = {
        "job_id": job_id,
        "blocks_processed": blocks_processed,
        "triples_created": created,
        "failures": failures,
    }
    logger.info("Triple extraction summary for job %s: %s", job_id, summary)
    return summary
