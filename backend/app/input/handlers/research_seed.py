"""Research Seed Handler: Initial intent."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler
from app.storage.models import Job

logger = logging.getLogger(__name__)

@register_classifier_handler("RESEARCH_SEED")
class ResearchSeedHandler(ClassifierHandler):
    """Handles initialization of research topic and entity vanguard search."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        entities = payload.get("entities", [])
        domain = payload.get("domain")
        topic = payload.get("topic", "General Research")
        
        if not entities and not topic:
            logger.warning(f"ResearchSeedHandler: No entities or topic found in seed for job {job_id}")
            return ClassifierHandlerResult(
                status="insufficient_data",
                message="Please provide a valid research topic or entities.",
                action_taken="rejected_seed",
                next_step="request_clarification"
            )

        # 1. Determine Endpoints vs Focus Areas
        # Use first entity as source, last as target. If only 1, use topic as target.
        source = entities[0] if len(entities) > 0 else topic
        target = entities[-1] if len(entities) > 1 else topic
        focus_areas = entities[1:-1] if len(entities) > 2 else []
        
        logger.info(f"Seed Ignition: {source} -> {target} (Focus: {focus_areas}) for Job {job_id}")

        # 2. Create Vanguard SearchQuery
        from app.fetching.query_orchestrator import get_or_create_search_query
        
        # Prepare a minimal hypothesis dict for signature generation
        hypo_dict = {
            "source": source,
            "target": target,
            "domain": domain
        }
        
        search_query = get_or_create_search_query(
            hypo_dict,
            job_id,
            session,
            focus_areas=focus_areas,
            entities=entities
        )
        
        # 3. Flip Job Status to ignite the Fetch Pipeline
        job = session.query(Job).get(job_id)
        if job:
            job.status = "FETCH_QUEUED"
            logger.info(f"Job {job_id} status updated to FETCH_QUEUED for vanguard ignite.")
        
        session.flush() # Ensure SearchQuery ID is visible

        return ClassifierHandlerResult(
            status="ok",
            message=f"Research ignited for {source} and {target}. SearchQuery {search_query.id} created.",
            action_taken="vanguard_ignited",
            next_step="wait_for_fetch"
        )
