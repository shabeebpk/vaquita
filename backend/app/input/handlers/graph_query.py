"""Graph Query Handler: Exploration."""
import logging
from typing import Dict, Any
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import register_classifier_handler

logger = logging.getLogger(__name__)

@register_classifier_handler("GRAPH_QUERY")
class GraphQueryHandler(ClassifierHandler):
    """Handles read-only exploration requests."""

    def handle(
        self,
        job_id: int,
        payload: Dict[str, Any],
        session: Any
    ) -> ClassifierHandlerResult:
        query_type = payload.get("query_type")
        entities = payload.get("entities", [])
        raw_text = payload.get("raw_text", "")
        
        logger.info(f"Processing graph query for job {job_id}: {query_type}")
        
        # This would typically query the semantic graph and return a summary.
        return ClassifierHandlerResult(
            status="ok",
            message=f"Graph query '{query_type}' acknowledged.",
            action_taken="read_graph",
            next_step="generate_summary",
            data={
                "query_info": {
                    "type": query_type,
                    "entities": entities,
                    "raw": raw_text
                }
            }
        )
