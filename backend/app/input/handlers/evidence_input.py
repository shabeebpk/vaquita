"""Evidence Input Handler: Data ingestion."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler
from app.storage.models import IngestionSource, IngestionSourceType

logger = logging.getLogger(__name__)

@register_classifier_handler("EVIDENCE_INPUT")
class EvidenceInputHandler(ClassifierHandler):
    """Handles factual data input meant for the knowledge graph."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        raw_text = payload.get("raw_text", "")
        content_type = payload.get("content_type", "unknown")
        
        logger.info(f"Filing evidence input for job {job_id} (type: {content_type})")
        
        # Create IngestionSource row
        # Note: We don't have the source_ref here (it was message:ID in chat.py)
        # The caller of the handler or chat.py will handle specific source_ref if needed.
        # For pure handler logic, we just record the intent/creation.
        source = IngestionSource(
            job_id=job_id,
            source_type=IngestionSourceType.USER_TEXT,
            source_ref="classifier:evidence_input", # Placeholder ref
            raw_text=raw_text,
            processed=False
        )
        session.add(source)
        # 2. Set Job.status to READY_TO_INGEST
        from app.storage.models import Job
        job = session.query(Job).get(job_id)
        if job:
            job.status = "READY_TO_INGEST"
            logger.info(f"Job {job_id} status updated to READY_TO_INGEST for user evidence flow.")

        session.flush()
        
        return ClassifierHandlerResult(
            status="ok",
            message=f"Created IngestionSource {source.id} from user evidence.",
            action_taken="queued_ingestion",
            next_step="trigger_extraction"
        )
