"""Halt Confident Handler: Finalization.

Freezes the job when the system is confident in the best hypothesis.
Selects the dominant hypothesis pair and top K alternative pairs, prepares 
final output with projected graph and fetched papers, and updates job 
status to COMPLETED. No further pipeline execution occurs.
"""

import logging
from typing import Dict, Any, List
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, JobPaperEvidence, Paper
from app.path_reasoning.persistence import project_hypotheses_to_graph, get_job_papers, group_top_hypotheses

logger = logging.getLogger(__name__)


@register_handler("halt_confident")
class HaltConfidentHandler(Handler):
    """Finalizes a job by selecting the leading hypotheses and marking complete."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute finalization.
        
        - Groups hypotheses by source/target pair.
        - Identifies dominant pair and top K alternatives.
        - Generates projected graph from hypotheses.
        - Collects all fetched papers.
        - Stores rich result in job.result.
        """
        try:
            from app.config.admin_policy import admin_policy
            
            # 1. Group and rank hypotheses
            limit = admin_policy.algorithm.decision_thresholds.top_k_hypotheses_to_store
            
            ranked_pairs = group_top_hypotheses(hypotheses, limit=limit)
            
            dominant_pair = ranked_pairs[0] if ranked_pairs else None
            top_k_pairs = ranked_pairs[1:] if len(ranked_pairs) > 1 else []
            
            # 2. Get active hypothesis version and measurements snapshot
            hypo_version = hypotheses[0].get("version", 1) if hypotheses else 1
            measurements = decision_result.get("measurements", {})
            total_cycles = measurements.get("decision_cycle_count", 1)  # If tracked, otherwise 1
            
            # 3. Generate projected graph from active hypotheses
            projected_graph = project_hypotheses_to_graph(job_id, semantic_graph, version=None)
            
            # 4. Fetch all papers considered for this job
            with Session(engine) as session:
                fetched_papers = get_job_papers(job_id, session)

            # Build final output structure
            final_output = {
                "job_id": job_id,
                "status": "completed",
                "conclusion": "Hypothesis Found (Confident)",
                "dominant_pair": dominant_pair,
                "top_k_alternatives": top_k_pairs,
                "metrics": {
                    "last_graph_version": semantic_graph.get("version", 1),
                    "hypothesis_version": hypo_version,
                    "total_cycles": total_cycles,
                    "measurements_snapshot": measurements
                },
                "projected_graph": projected_graph,
                "fetched_papers": fetched_papers,
                "fetched_papers_count": len(fetched_papers),
                "finalized_at": datetime.utcnow().isoformat(),
            }
            
            # Update job status to COMPLETED and save result
            with Session(engine) as session:
                job = session.query(Job).filter(Job.id == job_id).first()
                if job:
                    job.status = "COMPLETED"
                    job.result = final_output
                    session.commit()
                    logger.info(f"Job {job_id} marked COMPLETED by HaltConfidentHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            res_msg = "Job finalized with confident dominant hypothesis."
            if dominant_pair:
                res_msg = f"Job finalized: {dominant_pair['source']} -> {dominant_pair['target']} (conf={dominant_pair['max_confidence']:.2f})"
                
            return HandlerResult(
                status="ok",
                message=res_msg,
                next_action="show_final_result",
                data=final_output,
            )
        
        except Exception as e:
            logger.error(f"HaltConfidentHandler failed for job {job_id}: {e}", exc_info=True)
            return HandlerResult(
                status="error",
                message=f"Failed to finalize job: {str(e)}",
                next_action="notify_user",
            )
