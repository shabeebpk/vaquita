"""Research Seed Handler: Initial intent."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler
from app.storage.models import Job

logger = logging.getLogger(__name__)

@register_classifier_handler("RESEARCH_SEED")
class ResearchSeedHandler(ClassifierHandler):
    """Handles the minimal statement of research intent."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        topic = payload.get("topic")
        entities = payload.get("entities", [])
        raw_text = payload.get("raw_text", "")
        
        logger.info(f"Initializing research seed for job {job_id}: {topic or raw_text}")
        
        # Update job metadata or create a 'ResearchIntent' record if one existed.
        # For now, we'll signify this by updating the job state updates dict.
        # In a real system, we might create a Hypothesis scaffold here.
        
        job_updates = {
            "research_topic": topic,
            "seed_entities": entities,
            "seed_raw_text": raw_text
        }
        
        # Note: We don't perform fetch or reasoning here per rules.
        
        return ClassifierHandlerResult(
            status="ok",
            message="Research intent initialized.",
            action_taken="initialized_seed",
            next_step="plan_exploration",
            job_state_updates=job_updates
        )
