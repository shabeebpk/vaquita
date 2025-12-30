from fastapi import APIRouter, UploadFile, File as Upload, Form
from sqlalchemy.orm import Session

from app.storage.db import engine
from app.storage.models import Job, File
from app.ingestion.files import save_file
from app.schemas.ingestion import IngestionResponse

from app.core.queues import job_queue

router = APIRouter()

@router.post("/upload", response_model=IngestionResponse)
async def upload(
    user_text: str = Form(...),
    files: list[UploadFile] = Upload(...)
):
    with Session(engine) as session:
        job = Job(
            user_text=user_text,
            status="CREATED"
        )
        session.add(job)
        session.commit()
        session.refresh(job)

        uploaded = []

        for f in files:
            path = await save_file(job.id, f)

            file_row = File(
                job_id=job.id,
                original_filename=f.filename,
                stored_path=path,
                file_type=path.split(".")[-1]
            )
            session.add(file_row)
            uploaded.append(f.filename)

        session.commit()
        job_queue.put(job.id)

    return IngestionResponse(
        job_id=job.id,
        uploaded_files=uploaded
    )
