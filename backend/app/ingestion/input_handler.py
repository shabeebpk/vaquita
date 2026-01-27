"""
Input handler: creates IngestionSource rows when new input arrives.

Responsibility: When a user provides text, uploads a file, or a paper abstract
is fetched from an API, this module creates the corresponding IngestionSource rows
and sets the job status to READY_TO_INGEST. It handles no processing logic;
it only prepares inputs for the ingestion loop.

This is the entry point for all new text entering the system.
"""
import logging
from sqlalchemy.orm import Session
from datetime import datetime

from app.storage.models import Job, IngestionSource, File, Paper
from app.storage.db import engine

logger = logging.getLogger(__name__)


class InputHandler:
    """
    Handles new input arrival and creates IngestionSource rows.
    
    Entry points:
    - add_user_text(): User provides text via chat
    - add_uploaded_file(): User uploads a file (extracted and stored separately)
    - add_paper_abstract(): System fetches abstract from an API
    - add_pdf_text(): System downloads and extracts PDF text
    """

    @staticmethod
    def add_user_text(job_id: int, text: str) -> dict:
        """
        Create IngestionSource for user-provided text.
        
        Args:
            job_id: ID of the job
            text: raw text from user
        
        Returns:
            dict with source info: id, source_type, source_ref, raw_text_length
        
        Raises:
            ValueError: if job not found
        """
        logger.info(f"Adding user text input to job {job_id}")

        with Session(engine) as session:
            # Verify job exists
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")

            # Create IngestionSource for user text
            source = IngestionSource(
                job_id=job_id,
                source_type="user_text",
                source_ref=f"user_text_{datetime.utcnow().isoformat()}",
                raw_text=text,
                processed=False
            )
            session.add(source)

            # Set job status to READY_TO_INGEST
            job.status = "READY_TO_INGEST"
            session.add(job)
            session.commit()

            logger.info(
                f"Created IngestionSource {source.id} for user text "
                f"({len(text)} chars); job status set to READY_TO_INGEST"
            )

            return {
                "id": source.id,
                "source_type": source.source_type,
                "source_ref": source.source_ref,
                "raw_text_length": len(text)
            }

    @staticmethod
    def add_uploaded_file(
        job_id: int,
        file_path: str,
        original_filename: str,
        raw_text: str,
        file_type: str
    ) -> dict:
        """
        Create File record and IngestionSource for uploaded file.
        
        Args:
            job_id: ID of the job
            file_path: system path to stored file
            original_filename: original name of file
            raw_text: extracted text from file
            file_type: 'pdf', 'docx', 'txt', etc.
        
        Returns:
            dict with file and source info
        
        Raises:
            ValueError: if job not found
        """
        logger.info(f"Adding uploaded file to job {job_id}: {original_filename}")

        with Session(engine) as session:
            # Verify job exists
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")

            # Create File record (physical file storage)
            file_record = File(
                job_id=job_id,
                paper_id=None,
                origin_type="user_upload",
                stored_path=file_path,
                original_filename=original_filename,
                file_type=file_type
            )
            session.add(file_record)
            session.flush()  # Ensure file_record has an id

            # Create IngestionSource for extracted text
            source = IngestionSource(
                job_id=job_id,
                source_type="pdf_text" if file_type == "pdf" else "api_text",
                source_ref=f"file:{file_record.id}",
                raw_text=raw_text,
                processed=False
            )
            session.add(source)

            # Set job status to READY_TO_INGEST
            job.status = "READY_TO_INGEST"
            session.add(job)
            session.commit()

            logger.info(
                f"Created File {file_record.id} and IngestionSource {source.id} "
                f"for uploaded file; job status set to READY_TO_INGEST"
            )

            return {
                "file_id": file_record.id,
                "file_type": file_type,
                "original_filename": original_filename,
                "source_id": source.id,
                "source_ref": source.source_ref,
                "raw_text_length": len(raw_text)
            }

    @staticmethod
    def add_paper_abstract(
        job_id: int,
        paper_id: int,
        abstract: str
    ) -> dict:
        """
        Create IngestionSource for paper abstract from database.
        
        Args:
            job_id: ID of the job
            paper_id: ID of the paper (from papers table)
            abstract: abstract text
        
        Returns:
            dict with source info
        
        Raises:
            ValueError: if job or paper not found
        """
        logger.info(f"Adding paper abstract {paper_id} to job {job_id}")

        with Session(engine) as session:
            # Verify job exists
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")

            # Verify paper exists
            paper = session.query(Paper).filter(Paper.id == paper_id).first()
            if not paper:
                raise ValueError(f"Paper {paper_id} not found")

            # Create IngestionSource for abstract
            source = IngestionSource(
                job_id=job_id,
                source_type="paper_abstract",
                source_ref=f"paper:{paper_id}",
                raw_text=abstract or "",
                processed=False
            )
            session.add(source)

            # Set job status to READY_TO_INGEST
            job.status = "READY_TO_INGEST"
            session.add(job)
            session.commit()

            logger.info(
                f"Created IngestionSource {source.id} for paper abstract "
                f"({len(abstract or '')} chars); job status set to READY_TO_INGEST"
            )

            return {
                "id": source.id,
                "source_type": source.source_type,
                "source_ref": source.source_ref,
                "paper_id": paper_id,
                "raw_text_length": len(abstract or "")
            }

    @staticmethod
    def add_pdf_text(
        job_id: int,
        paper_id: int,
        file_path: str,
        raw_text: str
    ) -> dict:
        """
        Create File record and IngestionSource for downloaded PDF.
        
        Args:
            job_id: ID of the job
            paper_id: ID of the paper (from papers table)
            file_path: system path to downloaded PDF
            raw_text: extracted text from PDF
        
        Returns:
            dict with file and source info
        
        Raises:
            ValueError: if job or paper not found
        """
        logger.info(f"Adding downloaded PDF from paper {paper_id} to job {job_id}")

        with Session(engine) as session:
            # Verify job and paper exist
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                raise ValueError(f"Job {job_id} not found")

            paper = session.query(Paper).filter(Paper.id == paper_id).first()
            if not paper:
                raise ValueError(f"Paper {paper_id} not found")

            # Create File record (PDF download)
            file_record = File(
                job_id=job_id,
                paper_id=paper_id,
                origin_type="paper_download",
                stored_path=file_path,
                original_filename=paper.title[:50] + ".pdf" if paper.title else "paper.pdf",
                file_type="pdf"
            )
            session.add(file_record)
            session.flush()

            # Create IngestionSource for extracted PDF text
            source = IngestionSource(
                job_id=job_id,
                source_type="pdf_text",
                source_ref=f"file:{file_record.id}",
                raw_text=raw_text,
                processed=False
            )
            session.add(source)

            # Set job status to READY_TO_INGEST
            job.status = "READY_TO_INGEST"
            session.add(job)
            session.commit()

            logger.info(
                f"Created File {file_record.id} and IngestionSource {source.id} "
                f"for downloaded PDF; job status set to READY_TO_INGEST"
            )

            return {
                "file_id": file_record.id,
                "paper_id": paper_id,
                "source_id": source.id,
                "source_ref": source.source_ref,
                "raw_text_length": len(raw_text)
            }

    @staticmethod
    def get_job_input_status(job_id: int) -> dict:
        """
        Get summary of inputs for a job.
        
        Returns:
            dict with:
                - job_status: current job status
                - ingestion_source_count: total IngestionSource rows
                - processed_count: already processed
                - pending_count: awaiting processing
        """
        with Session(engine) as session:
            job = session.query(Job).filter(Job.id == job_id).first()
            if not job:
                return {"error": f"Job {job_id} not found"}

            total_sources = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id
            ).count()

            processed_sources = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id,
                IngestionSource.processed == True
            ).count()

            pending_sources = total_sources - processed_sources

            return {
                "job_id": job_id,
                "job_status": job.status,
                "ingestion_source_count": total_sources,
                "processed_count": processed_sources,
                "pending_count": pending_sources
            }
