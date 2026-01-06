"""
Upload API: Handle file uploads for document processing.

This endpoint:
1. Accepts files + optional job_id
2. Saves files to storage and creates File rows
3. Enqueues extraction task (text extraction from PDFs/documents)
4. Returns immediately without waiting for extraction
5. Extraction happens in background worker, which creates IngestionSource rows
6. Never directly extracts text or triggers ingestion

Separation of concerns:
- Upload endpoint: stores files, enqueues extraction task
- Background worker (extraction): reads files, extracts text, creates IngestionSource
- Ingestion service: processes IngestionSource rows
- Runner: orchestrates status transitions
"""

import logging
from fastapi import APIRouter, UploadFile, File as Upload, Form, HTTPException
from sqlalchemy.orm import Session
from typing import Optional, List

from app.storage.db import engine
from app.storage.models import Job, File, FileOriginType
from app.ingestion.files import save_file
from app.schemas.ingestion import UploadResponse
from app.core.queues import job_queue, extraction_queue

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/upload", tags=["upload"])


@router.post("/files", response_model=UploadResponse)
async def upload_files(
    files: List[UploadFile] = Upload(...),
    job_id: Optional[int] = Form(None)
):
    """
    Accept file uploads and enqueue extraction task.
    
    Workflow:
    1. Create or fetch job
    2. Save each file to storage
    3. Create File row for each uploaded file
    4. Enqueue extraction task (will extract text, create IngestionSource)
    5. Return immediately (don't wait for extraction)
    6. Background worker will process extraction and set job status to READY_TO_INGEST
    
    Args:
        files: List of uploaded files (PDF, DOCX, TXT, etc.)
        job_id: Optional existing job ID (new job created if not provided)
    
    Returns:
        UploadResponse with job_id, uploaded_files, extraction_enqueued, next_expected_action
    
    Raises:
        HTTPException: If job not found, no files provided, or file save fails
    """
    if not files or len(files) == 0:
        raise HTTPException(status_code=400, detail="At least one file must be provided")
    
    # Validate file types (basic check)
    allowed_extensions = {'.pdf', '.txt', '.docx', '.doc', '.xlsx', '.xls'}
    for f in files:
        ext = f.filename.split('.')[-1].lower() if f.filename else ""
        if f"." + ext not in allowed_extensions:
            logger.warning(f"Rejected file {f.filename} with extension .{ext}")
            raise HTTPException(
                status_code=400,
                detail=f"File type .{ext} not supported. Allowed: PDF, TXT, DOCX, XLSX"
            )
    
    with Session(engine) as session:
        # Create or fetch job
        if job_id:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        else:
            # Create new job
            job = Job(status="CREATED")
            session.add(job)
            session.flush()
        
        uploaded_filenames = []
        file_ids = []
        
        # Save each file and create File row
        for uploaded_file in files:
            try:
                # Save file to disk
                stored_path = await save_file(job.id, uploaded_file)
                
                # Create File row
                file_row = File(
                    job_id=job.id,
                    paper_id=None,  # Not linked to a specific paper yet
                    origin_type=FileOriginType.USER_UPLOAD,
                    stored_path=stored_path,
                    original_filename=uploaded_file.filename,
                    file_type=uploaded_file.filename.split('.')[-1].lower() if uploaded_file.filename else "unknown"
                )
                session.add(file_row)
                session.flush()
                
                uploaded_filenames.append(uploaded_file.filename)
                file_ids.append(file_row.id)
                
                logger.info(f"File {uploaded_file.filename} saved to {stored_path} with File ID {file_row.id}")
            
            except Exception as e:
                logger.error(f"Failed to save file {uploaded_file.filename}: {e}")
                raise HTTPException(
                    status_code=500,
                    detail=f"Failed to save file {uploaded_file.filename}: {str(e)}"
                )
        
        # Commit file rows
        session.commit()
        
        # Enqueue extraction task for each file
        # The background extraction worker will:
        # 1. Read the file from disk
        # 2. Extract text
        # 3. Create IngestionSource row with source_type=pdf_text, source_ref=file:{id}
        # 4. Update job status to READY_TO_INGEST when done
        for file_id in file_ids:
            extraction_queue.put({
                "job_id": job.id,
                "file_id": file_id,
                "task_type": "extract_text"
            })
        
        logger.info(
            f"Enqueued {len(file_ids)} extraction tasks for job {job.id}; "
            f"files: {uploaded_filenames}"
        )
        
        # Enqueue job for runner to monitor
        job_queue.put(job.id)
        
        return UploadResponse(
            job_id=job.id,
            uploaded_files=uploaded_filenames,
            extraction_enqueued=True,
            next_expected_action="Files are being extracted. Ingestion will process the extracted text automatically."
        )


@router.get("/job/{job_id}/files")
async def get_job_files(job_id: int):
    """
    Retrieve all uploaded files for a job.
    
    Args:
        job_id: Job ID
    
    Returns:
        List of File rows with metadata
    """
    with Session(engine) as session:
        job = session.query(Job).filter(Job.id == job_id).first()
        if not job:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")
        
        files = session.query(File).filter(File.job_id == job_id).all()
        
        return {
            "job_id": job_id,
            "file_count": len(files),
            "files": [
                {
                    "id": f.id,
                    "original_filename": f.original_filename,
                    "file_type": f.file_type,
                    "origin_type": f.origin_type.value if f.origin_type else None,
                    "created_at": f.created_at.isoformat() if f.created_at else None
                }
                for f in files
            ]
        }
