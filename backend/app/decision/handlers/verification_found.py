"""Verification Found Handler: Connection established between entities.

Stores verification result when source and target entities are confirmed connected.
No further pipeline execution occurs for this verification job.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, VerificationResult, JobPaperEvidence, Paper, SearchQuery
from app.path_reasoning.persistence import get_job_papers

logger = logging.getLogger(__name__)


def _get_search_queries(job_id: int, session: Session) -> List[Dict[str, Any]]:
    """Return query texts used to search for this job."""
    queries = session.query(SearchQuery).filter(SearchQuery.job_id == job_id).all()
    return [
        {"query_text": q.query_text, "status": q.status, "entities": q.entities_used}
        for q in queries
    ]


@register_handler("verification_found")
class VerificationFoundHandler(Handler):
    """Completes verification job with positive result."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute verification found handler.
        
        - Stores verification result as found in verification_results table
        - Updates job status to COMPLETED with full conclusion data
        - Returns result for UI
        """
        try:
            # Get verification context from job_metadata
            source = job_metadata.get("verification_source")
            target = job_metadata.get("verification_target")
            verification_result = job_metadata.get("verification_result", {})
            
            if not source or not target:
                return HandlerResult(
                    status="error",
                    message="Missing verification_source or verification_target in job_metadata",
                )
            
            connection_type = verification_result.get("type", "unknown")  # 'direct' or 'indirect'
            path_evidence = verification_result.get("supporting_papers", [])
            path = verification_result.get("path")  # List of nodes in the path
            explanation = verification_result.get("explanation", "")  # Human-readable explanation
            
            # Store result in database
            with Session(engine) as session:
                # Collect papers fetched for this job (for conclusion)
                fetched_papers = get_job_papers(job_id, session)
                search_queries_used = _get_search_queries(job_id, session)

                # Create verification result record
                vr = VerificationResult(
                    job_id=job_id,
                    source=source,
                    target=target,
                    connection_found=True,
                    connection_type=connection_type,
                    path=path,
                    explanation=explanation,
                    supporting_papers=path_evidence,
                    created_at=datetime.utcnow(),
                    updated_at=datetime.utcnow(),
                )
                session.add(vr)
                
                # Update job status and result
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "COMPLETED"
                    job.result = {
                        "verification_status": "found",
                        "source": source,
                        "target": target,
                        "connection_type": connection_type,
                        "path": path,
                        "explanation": explanation,
                        # Path-level supporting papers (edges proven by specific papers)
                        "path_supporting_papers": path_evidence,
                        # All papers fetched during this job's search phase
                        "fetched_papers": fetched_papers,
                        "fetched_papers_count": len(fetched_papers),
                        # Queries used to search
                        "search_queries": search_queries_used,
                        "completed_at": datetime.utcnow().isoformat(),
                    }
                    session.commit()
                    logger.info(
                        f"Job {job_id} verification FOUND: {source} -> {target} "
                        f"({connection_type}), {len(fetched_papers)} papers fetched"
                    )
                else:
                    logger.warning(f"Job {job_id} not found for status update")
                    session.rollback()
                    return HandlerResult(
                        status="error",
                        message=f"Job {job_id} not found",
                    )
            
            final_output = {
                "job_id": job_id,
                "status": "verification_found",
                "source": source,
                "target": target,
                "connection_type": connection_type,
                "path": path,
                "explanation": explanation,
                "path_supporting_papers": path_evidence,
                "fetched_papers": fetched_papers,
                "fetched_papers_count": len(fetched_papers),
                "search_queries": search_queries_used,
                "completed_at": datetime.utcnow().isoformat(),
            }
            
            return HandlerResult(
                status="ok",
                message=f"Verification complete: {source} and {target} are connected ({connection_type})",
                next_action="show_verification_result",
                data=final_output,
            )
        
        except Exception as e:
            logger.error(f"VerificationFoundHandler failed for job {job_id}: {e}", exc_info=True)
            return HandlerResult(
                status="error",
                message=f"Failed to complete verification: {str(e)}",
                next_action="notify_user",
            )
