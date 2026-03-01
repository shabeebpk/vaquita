"""Halt No Hypothesis Handler: Finalization.

Records that no viable dominant hypothesis path exists and halts execution.
Groups any existing non-dominant paths into top K alternatives, prepares 
final output with projected graph and fetched papers, and updates job 
status to COMPLETED with NO_HYPOTHESIS outcome. No further pipeline execution occurs.
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
from presentation.events import push_presentation_event

logger = logging.getLogger(__name__)


@register_handler("halt_no_hypothesis")
class HaltNoHypothesisHandler(Handler):
    """Handles termination when no viable dominant hypothesis path exists."""
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """Execute halt with no hypothesis.
        
        - Notes no dominant path was found.
        - Groups any existing paths into top K alternatives.
        - Generates projected graph.
        - Collects all fetched papers.
        - Updates job status to COMPLETED and stores precise outcome.
        """
        try:
            from app.config.admin_policy import admin_policy
            
            measurements = decision_result.get("measurements", {})
            reason = (
                f"No viable hypothesis paths detected. "
                f"Passed hypotheses: {measurements.get('passed_hypothesis_count', 0)}, "
                f"Growth score: {measurements.get('growth_score', 0):.3f}, "
                f"Max paths per pair: {measurements.get('max_paths_per_pair', 0)}"
            )

            # 1. Group and rank hypotheses (there is no 'dominant' here, only alternatives)
            limit = admin_policy.algorithm.decision_thresholds.top_k_hypotheses_to_store
            ranked_pairs = group_top_hypotheses(hypotheses, limit=limit)
            
            # 2. Get active hypothesis version and measurements snapshot
            hypo_version = hypotheses[0].get("version", 1) if hypotheses else 1
            measurements = decision_result.get("measurements", {})
            total_cycles = measurements.get("decision_cycle_count", 1)
            
            # 3. Generate projected graph from active hypotheses
            projected_graph = project_hypotheses_to_graph(job_id, semantic_graph, version=None)
            
            # 4. Fetch all papers considered for this job
            with Session(engine) as session:
                fetched_papers = get_job_papers(job_id, session)

            # Build final output structure
            final_output = {
                "job_id": job_id,
                "status": "completed",
                "conclusion": "No Hypothesis Found (Stagnant/Ambiguous)",
                "reason": reason,
                "top_k_alternatives": ranked_pairs,  # In missing-dominant case, provide the runner-ups
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
                    logger.info(f"Job {job_id} marked COMPLETED (NO_HYPOTHESIS) by HaltNoHypothesisHandler")
                else:
                    logger.warning(f"Job {job_id} not found for status update")
            
            logger.info(f"Job {job_id} halted: no hypothesis paths. {reason}")
            
            # Emit presentation event
            push_presentation_event(
                job_id=job_id,
                phase="DECISION",
                status="nohypo",
                result={
                    "conclusion": final_output.get("conclusion", "No Hypothesis Found"),
                    "reason": final_output.get("reason", ""),
                    "top_k_count": len(ranked_pairs),
                    "papers_used": final_output.get("fetched_papers_count", 0),
                    "total_cycles": final_output.get("metrics", {}).get("total_cycles", 1),
                },
                metric=final_output.get("metrics", {}).get("measurements_snapshot"),
                payload={
                    "top_k_hypotheses": ranked_pairs,
                    "papers": final_output.get("fetched_papers", []),
                },
            )
            
            return HandlerResult(
                status="ok",
                message="Job terminated: no viable dominant hypothesis paths detected",
                next_action="show_final_result",
                data=final_output,
            )
        
        except Exception as e:
            logger.error(f"HaltNoHypothesisHandler failed for job {job_id}: {e}", exc_info=True)
            return HandlerResult(
                status="error",
                message=f"Handler execution failed: {str(e)}",
                next_action="notify_admin",
            )
