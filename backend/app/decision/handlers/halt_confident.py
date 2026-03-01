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
from app.path_reasoning.persistence import project_hypotheses_to_graph, get_job_papers, group_top_hypotheses, get_dominant_pair
from presentation.events import push_presentation_event

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
            
            # 1. Determine if dominant pair is clear from measurements
            measurements = decision_result.get("measurements", {})
            is_dominant_clear = measurements.get("is_dominant_clear", False)
            dominant_pair_ids = measurements.get("dominant_pair_id", [])  # e.g. ["openai", "thought"]
            
            # 2. Build dominant pair with intermediates
            dominant_pair = None
            exclude_pair = None
            if is_dominant_clear and dominant_pair_ids:
                dominant_pair = get_dominant_pair(hypotheses, dominant_pair_ids)
                exclude_pair = tuple(dominant_pair_ids)
            
            # 3. Group top-K pairs, excluding dominant
            limit = admin_policy.algorithm.decision_thresholds.top_k_hypotheses_to_store
            top_k_pairs = group_top_hypotheses(hypotheses, limit=limit, exclude_pair=exclude_pair)
            
            # 4. Get hypothesis version and cycle count
            hypo_version = hypotheses[0].get("version", 1) if hypotheses else 1
            total_cycles = measurements.get("decision_cycle_count", 1)
            
            # 5. Generate projected graph
            projected_graph = project_hypotheses_to_graph(job_id, semantic_graph, version=None)
            
            # 6. Fetch papers and resolve evidence text for dominant pair
            with Session(engine) as session:
                fetched_papers = get_job_papers(job_id, session)
                
                # Resolve evidence snippets by querying Hypothesis table for dominant pair
                from app.path_reasoning.persistence import resolve_triple_evidence_text
                final_evidence = []
                if dominant_pair:
                    from app.storage.models import Hypothesis
                    dom_src = dominant_pair["source"]
                    dom_tgt = dominant_pair["target"]
                    dom_triple_ids = []
                    dom_rows = session.query(Hypothesis).filter(
                        Hypothesis.job_id == job_id,
                        Hypothesis.source == dom_src,
                        Hypothesis.target == dom_tgt,
                    ).all()
                    for row in dom_rows:
                        if row.triple_ids:
                            dom_triple_ids.extend(row.triple_ids)
                    final_evidence = resolve_triple_evidence_text(list(set(dom_triple_ids)), session)

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
                "final_evidence": final_evidence,
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
            
            # Emit presentation event
            push_presentation_event(
                job_id=job_id,
                phase="DECISION",
                status="haltconfident",
                result={
                    "conclusion": final_output.get("conclusion", "Hypothesis Found"),
                    "dominant_hypothesis": f"{dominant_pair['source']} -> [{', '.join(dominant_pair.get('intermediates', []))}] -> {dominant_pair['target']}" if dominant_pair else "None",
                    "top_k_count": len(top_k_pairs),
                    "papers_used": final_output.get("fetched_papers_count", 0),
                    "total_cycles": final_output.get("metrics", {}).get("total_cycles", 1),
                    "final_evidence": "\n\n".join(final_evidence[:5]),
                },
                metric=final_output.get("metrics", {}).get("measurements_snapshot"),
                payload={
                    "dominant": dominant_pair,
                    "top_k_hypotheses": top_k_pairs,
                    "papers": final_output.get("fetched_papers", []),
                    "evidence_snippets": final_evidence,
                },
            )
                
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
