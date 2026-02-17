
import logging
from fastapi import APIRouter, HTTPException
from sqlalchemy.orm import Session
from app.storage.db import engine
from app.storage.models import Job
from worker.stage_tasks import (
    ingest_stage, 
    structural_graph_stage, 
    path_reasoning_stage, 
    fetch_stage,
    download_stage
)

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/test", tags=["test"])

@router.post("/trigger")
async def trigger_stage(job_id: int, status: str):
    """
    Simple Test API: Manually update job status and trigger the corresponding Celery stage.
    Supported statuses: READY_TO_INGEST, TRIPLES_EXTRACTED, GRAPH_SEMANTIC_MERGED, FETCH_QUEUED, DOWNLOAD_QUEUED
    """
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        # 1. Update Status
        job.status = status.upper()
        session.commit()
        
        # 2. Dispatch based on status
        task_map = {
            "READY_TO_INGEST": ingest_stage,
            "TRIPLES_EXTRACTED": structural_graph_stage,
            "GRAPH_SEMANTIC_MERGED": path_reasoning_stage,
            "FETCH_QUEUED": fetch_stage,
            "DOWNLOAD_QUEUED": download_stage
        }
        
        target_task = task_map.get(job.status)
        if not target_task:
            return {
                "job_id": job_id, 
                "status": job.status, 
                "message": "Status updated, but no automated trigger associated with this state."
            }
            
        target_task.delay(job_id)
        
        return {
            "job_id": job_id,
            "status": job.status,
            "triggered_task": target_task.name,
            "message": f"Pipeline resumed from {status}"
        }
