
import logging
import os
import uuid
import json
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from sqlalchemy.orm import Session
from typing import Optional, List, Union, Any

from app.storage.db import engine
from app.storage.models import Job, File as FileModel, FileOriginType, ConversationMessage, MessageType
from worker.stage_tasks import extract_stage, mark_ready_stage, fetch_stage, ingest_stage
from celery import chord

from app.config.loader import load_default_job_config

# ...
router = APIRouter()

@router.post("/")
async def unified_chat(
    job_id: Optional[Union[int, str]] = Form(None),
    content: Optional[str] = Form(None),
    files: Optional[List[UploadFile]] = File(None)
):
    """
    Unified Endpoint for Literature Review.
    Handles both text input and multiple file uploads.
    """
    with Session(engine) as session:
        # 1. Sanitize job_id (FastAPI/Swagger often sends empty strings or "string" placeholders)
        if isinstance(job_id, str):
            if not job_id.strip() or job_id == "string":
                job_id = None
            else:
                try:
                    job_id = int(job_id)
                except ValueError:
                    raise HTTPException(status_code=400, detail=f"Invalid job_id: {job_id}")

        # 2. Lazy Job Creation
        if job_id is None:
            # Load default job config via centralized loader
            try:
                default_config = load_default_job_config()
            except Exception as e:
                logger.error(f"Failed to create job: {e}")
                raise HTTPException(status_code=500, detail="Configuration error")

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

        # 3. Handle multiple file uploads
        actual_files = []
        if files:
            for f in files:
                if f.filename and f.filename != "string":
                    actual_files.append(f)
        files = actual_files

        responses = []
        is_conversational = False
        conversational_answer = ""

        # Tracking for combined CREATION event
        content_types = []
        creation_result = {
            "job_type": job.mode if hasattr(job, "mode") else "discovery",
        }
        triggered_extraction = False
        triggered_fetch = False
        triggered_ingestion = False
        prep_tasks = []

        # 2. Dispatch Text Processing (Synchronous Classification for API)
        if content and content.strip():
            from app.input.classifier import get_classifier, ClassificationLabel
            
            # Save User Message
            msg = ConversationMessage(
                job_id=job_id,
                role="user",
                message_type=MessageType.TEXT.value,
                content=content.strip()
            )
            session.add(msg)
            session.flush()

            classifier = get_classifier()
            classification = classifier.classify(content, job_id=job_id, session=session)
            session.commit()
            
            label = classification.label
            
            if label == ClassificationLabel.CONVERSATIONAL:
                is_conversational = True
                conversational_answer = classification.payload.get("answer", "I understood your message, but I don't have a specific answer right now.")
                responses.append("Conversational intent processed.")
                
            elif label == ClassificationLabel.RESEARCH_SEED:
                content_types.append("topic/seed")
                if job.mode == "verification":
                    from app.storage.models import VerificationResult
                    vr = session.query(VerificationResult).filter(VerificationResult.job_id == job_id).first()
                    if vr:
                        creation_result["source"] = vr.source
                        creation_result["target"] = vr.target
                
                logger.info(f"Classified as SEED; queuing fetch stage for job {job_id}")
                prep_tasks.append(fetch_stage.s(job_id, wait_for_chord=True))
                triggered_fetch = True
                responses.append("Research Seed identified. Fetching literature.")
                
            elif label == ClassificationLabel.EVIDENCE_INPUT:
                content_types.append("text evidence")
                triggered_ingestion = True
                responses.append("Evidence Input identified. Queuing for ingestion.")

        # 3. Dispatch File Processing (Background Chord)
        if files:
            content_types.append("file upload")
            extraction_tasks = []
            uploaded_names = []
            
            # Job-specific original folder
            job_upload_dir = os.path.join("uploads", f"{job_id}", "original")
            os.makedirs(job_upload_dir, exist_ok=True)
            
            for file in files:
                # Save file to disk
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
                prep_tasks.extend(extraction_tasks)
                triggered_extraction = True
                responses.append(f"Queued {len(extraction_tasks)} files: {', '.join(uploaded_names)}")

        # 4. Final Consolidated Pipeline Trigger
        if prep_tasks:
            from celery import chord
            from worker.stage_tasks import mark_ready_stage
            chord(prep_tasks)(mark_ready_stage.s(job_id))
        elif triggered_ingestion:
            # Only text evidence provided, no background prep needed
            # We skip the chord and go straight to ingestion
            from worker.stage_tasks import ingest_stage
            ingest_stage.delay(job_id)

        # 5. Final Consolidated CREATION Event
        if content_types:
            from presentation.events import push_presentation_event
            creation_result["content_types"] = content_types
            
            # Determine next action priority: fetch > ingest
            nxt = "fetch" if triggered_fetch else "ingest"
            
            push_presentation_event(
                job_id=job_id,
                phase="CREATION",
                status=None,
                result=creation_result,
                next_action=nxt,
            )

        if not responses:
            return {"error": "No content or files provided"}

        # Determine what workflow (if any) was actually started so the frontend
        # can decide whether to start polling / show a loading state.
        if triggered_extraction or triggered_fetch:
            workflow = "fetch"
        elif triggered_ingestion:
            workflow = "ingestion"
        else:
            workflow = None  # pure conversation, nothing queued

        out = {
            "job_id": job_id,
            "status": "queued" if not is_conversational else "conversational",
            "workflow_triggered": workflow,
            "details": responses
        }
        
        if is_conversational:
            out["answer"] = conversational_answer
            
        return out
