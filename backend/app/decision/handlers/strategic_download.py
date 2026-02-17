"""Strategic Download Handler: Gating mechanism for paper extraction.

Checks if any undownloaded papers exist for the job in JobPaperEvidence.
If they exist: Sets status to DOWNLOAD_QUEUED.
If not exist: Falls back to FETCH_QUEUED (need more data).
"""

import logging
from typing import Dict, Any
from datetime import datetime

from sqlalchemy.orm import Session

from app.decision.handlers.base import Handler, HandlerResult
from app.decision.handlers.registry import register_handler
from app.storage.db import engine
from app.storage.models import Job, JobPaperEvidence

logger = logging.getLogger(__name__)


class StrategicDownloadHandler(Handler):
    """
    Gating mechanism to decide if the next step is downloading or fetching.
    
    If growth is detected, we want to download the promising papers.
    If we've already downloaded/extracted all papers in the ledger, we must fetch more.
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
        """
        try:
            with Session(engine) as session:
                # 1. Check for undownloaded papers in the Strategic Ledger
                # evaluated=False means they are in the ledger but not yet processed (extracted)
                undownloaded_count = session.query(JobPaperEvidence).filter(
                    JobPaperEvidence.job_id == job_id,
                    JobPaperEvidence.evaluated == False
                ).count()
                
                job = session.query(Job).filter(Job.id == job_id).first()
                if not job:
                    return HandlerResult(status="error", message=f"Job {job_id} not found", next_action="notify_user")

                if undownloaded_count > 0:
                    job.status = "DOWNLOAD_QUEUED"
                    message = f"Strategic Growth detected. Found {undownloaded_count} papers to extract. Marked DOWNLOAD_QUEUED."
                    status_label = "DOWNLOAD_QUEUED"
                else:
                    # Fallback to fetching if we have nothing left to download
                    job.status = "FETCH_QUEUED"
                    message = "Strategic Growth detected but no undownloaded papers in ledger. Falling back to FETCH_QUEUED."
                    status_label = "FETCH_QUEUED"
                
                session.commit()
                logger.info(f"Job {job_id}: {message}")
                
                return HandlerResult(
                    status="deferred",
                    message=message,
                    next_action="show_status_update",
                    data={
                        "undownloaded_count": undownloaded_count,
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


# Register this handler matching the space.py label
register_handler("strategic_download_targeted", StrategicDownloadHandler)
