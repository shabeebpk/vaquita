
import logging
import os
import uuid
import json
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import Optional, List

from app.storage.db import engine
from app.storage.models import Job, File as FileModel, FileOriginType
from worker.stage_tasks import classify_stage, extract_stage, mark_ready_stage
from celery import chord

router = APIRouter(prefix="/chat", tags=["chat"])

@router.post("/")
async def unified_chat(
    job_id: Optional[int] = Form(None),
    content: Optional[str] = Form(None),
    files: Optional[List[UploadFile]] = File(None)
):
    """
    Unified Endpoint for Literature Review.
    Handles both text input and multiple file uploads.
    """
    with Session(engine) as session:
        # 1. Lazy Job Creation
        if job_id is None:
            # Load default job config
            try:
                config_path = os.path.join(os.path.dirname(__file__), "../config/default_job_config.json")
                with open(config_path, "r") as f:
                    default_config = json.load(f)
            except Exception as e:
                logger.error(f"Failed to load default job configuration: {e}")
                default_config = {}

            job = Job(status="CREATED", job_config=default_config)
            session.add(job)
            session.flush()
            job_id = job.id
            session.commit()
            logger.info(f"Lazy-created new job {job_id} with default configuration")
        else:
            job = session.query(Job).get(job_id)
            if not job:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        responses = []

        # 2. Dispatch Text Processing (Background)
        if content and content.strip():
            classify_stage.delay(job_id, content)
            responses.append(f"Text processing queued.")

        # 3. Dispatch File Processing (Background Chord)
        if files:
            extraction_tasks = []
            uploaded_names = []
            
            # Job-specific original folder
            job_upload_dir = os.path.join("uploads", f"{job_id}", "original")
            os.makedirs(job_upload_dir, exist_ok=True)
            
            for file in files:
                # Save file to disk - Use UUID to prevent name collisions in the job folder
                ext = file.filename.split(".")[-1].lower() if "." in file.filename else "bin"
                unique_filename = f"{uuid.uuid4()}.{ext}"
                stored_path = os.path.join(job_upload_dir, unique_filename)
                
                content_bytes = await file.read()
                if not content_bytes:
                    continue
                    
                with open(stored_path, "wb") as f:
                    f.write(content_bytes)
                
                # Create File record
                file_record = FileModel(
                    job_id=job_id,
                    origin_type=FileOriginType.USER_UPLOAD.value,
                    stored_path=stored_path,
                    original_filename=file.filename,
                    file_type=ext
                )
                session.add(file_record)
                session.flush() # Get file ID
                
                extraction_tasks.append(extract_stage.s(job_id, file_record.id))
                uploaded_names.append(file.filename)
            
            session.commit()
            
            if extraction_tasks:
                # Chord: Run all extractions in parallel, then mark job as READY_TO_INGEST
                chord(extraction_tasks)(mark_ready_stage.s(job_id))
                responses.append(f"Queued {len(extraction_tasks)} files: {', '.join(uploaded_names)}")

        if not responses:
            return {"error": "No content or files provided"}

        return {
            "job_id": job_id,
            "status": "queued",
            "details": responses
        }
