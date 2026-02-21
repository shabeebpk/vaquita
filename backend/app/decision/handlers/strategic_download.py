"""Strategic Download Handler: Gating mechanism for paper extraction.

Checks if any undownloaded papers exist for the job in JobPaperEvidence.
If they exist: Sets status to DOWNLOAD_QUEUED.
If not exist: Falls back to FETCH_QUEUED (need more data) unless max papers reached.

Before setting FETCH_QUEUED, checks if max papers limit is reached.
If max papers reached, halts with NO_HYPOTHESIS status instead.
"""

import logging
from typing import Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, JobPaperEvidence
from app.config.system_settings import system_settings

logger = logging.getLogger(__name__)


@register_handler("strategic_download_targeted")
class StrategicDownloadHandler(Handler):
    """
    Gating mechanism to decide if the next step is downloading or fetching.
    
    If growth is detected, we want to download the promising papers.
    If we've already downloaded/extracted all papers in the ledger, we must fetch more.
    Before fetching, checks if max papers limit is reached.
    """
    
    def handle(
        self,
        job_id: int,
        decision_result: Dict[str, Any],
        semantic_graph: Dict[str, Any],
        hypotheses: list,
        job_metadata: Dict[str, Any],
    ) -> HandlerResult:
        """
        Check for undownloaded papers and set job status accordingly.
        Enforces max papers limit before setting FETCH_QUEUED.
        """
        try:
            # Get max papers limit from system settings
            max_papers = system_settings.SYSTEM_MAX_PAPERS_PER_JOB
            
            with Session(engine) as session:
                # 1. Check for undownloaded papers in the Strategic Ledger
                # evaluated=False means they are in the ledger but not yet processed (extracted)
                undownloaded_count = session.query(JobPaperEvidence).filter(
                    JobPaperEvidence.job_id == job_id,
                    JobPaperEvidence.evaluated == False
                ).count()
                
                # 2. Count total papers for this job
                total_paper_count = session.query(JobPaperEvidence).filter(
                    JobPaperEvidence.job_id == job_id
                ).count()
                
                job = session.query(Job).filter(Job.id == job_id).first()
                if not job:
                    return HandlerResult(status="error", message=f"Job {job_id} not found", next_action="notify_user")

                if undownloaded_count > 0:
                    # Papers to download
                    job.status = "DOWNLOAD_QUEUED"
                    message = f"Strategic Growth detected. Found {undownloaded_count} papers to extract. Marked DOWNLOAD_QUEUED."
                    status_label = "DOWNLOAD_QUEUED"
                    session.commit()
                    
                    logger.info(f"Job {job_id}: {message}")
                    
                    return HandlerResult(
                        status="deferred",
                        message=message,
                        next_action="show_status_update",
                        data={
                            "undownloaded_count": undownloaded_count,
                            "total_papers": total_paper_count,
                            "max_papers": max_papers,
                            "final_status": status_label
                        }
                    )
                
                # No undownloaded papers - need to fetch more
                # But first check if we've reached max papers limit
                if total_paper_count >= max_papers:
                    job.status = "COMPLETED"
                    session.commit()
                    logger.info(
                        f"Job {job_id} reached max papers limit ({total_paper_count}/{max_papers}). "
                        f"Halting with NO_HYPOTHESIS instead of fetching more."
                    )
                    
                    return HandlerResult(
                        status="ok",
                        message=f"Maximum papers limit reached ({total_paper_count}/{max_papers}). Job completed.",
                        next_action="show_termination_reason",
                        data={
                            "outcome": "max_papers_reached",
                            "current_paper_count": total_paper_count,
                            "max_papers": max_papers,
                            "reason": "System reached maximum number of papers for this job"
                        }
                    )
                
                # Limit not reached - proceed with fetch fallback
                job.status = "FETCH_QUEUED"
                session.commit()
                message = (
                    f"Strategic Growth detected but no undownloaded papers in ledger. "
                    f"Falling back to FETCH_QUEUED (current: {total_paper_count}/{max_papers} papers)"
                )
                status_label = "FETCH_QUEUED"
                
                logger.info(f"Job {job_id}: {message}")
                
                return HandlerResult(
                    status="deferred",
                    message=message,
                    next_action="show_status_update",
                    data={
                        "undownloaded_count": undownloaded_count,
                        "total_papers": total_paper_count,
                        "max_papers": max_papers,
                        "final_status": status_label
                    }
                )

        except Exception as e:
            logger.error(f"StrategicDownloadHandler failed for job {job_id}: {e}")
            return HandlerResult(
                status="error",
                message=f"StrategicDownloadHandler failed: {str(e)}",
                next_action="notify_user",
            )


# StrategicDownloadHandler is now registered via decorator above.
