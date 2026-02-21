"""
Celery Stage Tasks: Orchestrates job pipeline stages as discrete tasks.

Replaces the monolithic runner logic with modular, retry-aware tasks.
Each task is responsible for exactly one stage and transitions to the next.
"""
import logging
import os
from sqlalchemy.orm import Session
from celery_app import celery_app
from celery.exceptions import MaxRetriesExceededError

from app.storage.db import engine
from app.storage.models import Job, IngestionSource, IngestionSourceType, DecisionResult, ConversationMessage, MessageRole, MessageType
from app.config.job_config import JobConfig
from events import publish_event

logger = logging.getLogger(__name__)


def verify_fetch_sources_ready(job_id: int, session: Session) -> bool:
    """Verify that all fetched IngestionSource rows are ready for ingestion."""
    # Count unprocessed sources for this job (created by fetch)
    unprocessed_count = session.query(IngestionSource).filter(
        IngestionSource.job_id == job_id,
        IngestionSource.processed == False,
        IngestionSource.source_type.in_([
            IngestionSourceType.PAPER_ABSTRACT.value,
            IngestionSourceType.API_TEXT.value
        ])
    ).count()
    
    logger.info(f"Job {job_id} fetch readiness check: {unprocessed_count} unprocessed fetched sources")
    return unprocessed_count > 0


# Configs (mirrored from runner.py)
from app.config.admin_policy import admin_policy
_INGESTION_SEGMENTATION_STRATEGY = admin_policy.algorithm.ingestion_defaults.segmentation_strategy
_INGESTION_SENTENCES_PER_BLOCK = admin_policy.algorithm.ingestion_defaults.sentences_per_block
_SEMANTIC_SIMILARITY_THRESHOLD = admin_policy.algorithm.decision_thresholds.semantic_similarity_threshold
_PATH_REASONING_MAX_HOPS = admin_policy.algorithm.path_reasoning_defaults.max_hops

# Stage -1: Classification and Routing
# ============================================================================

@celery_app.task(
    name="stage.classify",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 5}
)
def classify_stage(self, job_id: int, text: str, role: str = "user"):
    """Classifies user input and routes to ingestion if needed."""
    from app.input.classifier import get_classifier, ClassificationLabel
    
    publish_event({"job_id": job_id, "stage": "classification", "status": "started"})
    
    with Session(engine) as session:
        # 1. Store the message
        msg = ConversationMessage(
            job_id=job_id,
            role=role,
            message_type=MessageType.TEXT.value,
            content=text.strip()
        )
        session.add(msg)
        session.flush()
        
        # 2. Classify and Handle
        classifier = get_classifier()
        classification = classifier.classify(text, job_id=job_id, session=session)
        
        logger.info(f"Job {job_id} message {msg.id} classified as {classification.label.value}")
        
        # 3. Finalize
        session.commit()
        
        publish_event({
            "job_id": job_id, 
            "stage": "classification", 
            "status": "completed", 
            "label": classification.label.value,
            "message_id": msg.id
        })

    return {"message_id": msg.id, "classification": classification.label.value}


# ============================================================================
# Stage 0: Text Extraction (from uploaded files)
# ============================================================================

@celery_app.task(
    name="stage.extract",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 20}
)
def extract_stage(self, job_id: int, file_id: int):
    """Extraction of text from a single File and saves it as an IngestionSource."""
    from app.storage.models import File, IngestionSource, IngestionSourceType
    from app.ingestion.extractor import extract_text_from_file
    
    publish_event({"job_id": job_id, "stage": "extraction", "file_id": file_id, "status": "started"})
    
    with Session(engine) as session:
        file_row = session.query(File).filter(File.id == file_id).first()
        if not file_row:
            logger.error(f"File {file_id} not found for extraction")
            return
            
        try:
            # 1. Perform extraction (reusing centralized logic)
            text = extract_text_from_file(file_row.stored_path, file_row.file_type)
            
            if not text or not text.strip():
                logger.warning(f"No text extracted from file {file_id}")
                publish_event({"job_id": job_id, "stage": "extraction", "file_id": file_id, "status": "empty_content"})
                return
            
            # 2. Create IngestionSource
            source = IngestionSource(
                job_id=job_id,
                source_type=IngestionSourceType.PDF_TEXT.value,
                source_ref=f"file:{file_id}",
                raw_text=text,
                processed=False
            )
            session.add(source)
            session.commit()
            
            publish_event({"job_id": job_id, "stage": "extraction", "file_id": file_id, "status": "completed"})
            logger.info(f"Extraction completed for file {file_id}")
            return True
            
        except Exception as e:
            logger.error(f"Extraction failed for file {file_id}: {e}")
            publish_event({"job_id": job_id, "stage": "extraction", "file_id": file_id, "status": "failed", "error": str(e)})
            raise


@celery_app.task(name="stage.mark_ready")
def mark_ready_stage(result_list, job_id: int):
    """
    Callback task triggered when all extractions are done.
    Sets status to READY_TO_INGEST and starts the pipeline.
    """
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if job:
            job.status = "READY_TO_INGEST"
            session.commit()
            logger.info(f"Job {job_id} is now READY_TO_INGEST (Extraction Batch Done)")
            publish_event({"job_id": job_id, "stage": "extraction", "status": "all_files_ready"})
            
    # Now trigger ingestion
    ingest_stage.delay(job_id)


# ============================================================================
# Stage 1: Ingestion
# ============================================================================

@celery_app.task(
    name="stage.ingest",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30},
    retry_backoff=True
)
def ingest_stage(self, job_id: int):
    """Processes all unprocessed IngestionSource rows into TextBlocks."""
    from app.ingestion.service import IngestionService
    
    publish_event({"job_id": job_id, "stage": "ingestion", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found for ingestion.")
            publish_event({"job_id": job_id, "stage": "ingestion", "status": "failed", "error": "Job not found"})
            return

    try:
        IngestionService.ingest_job(
            job_id=job_id,
            segmentation_strategy=_INGESTION_SEGMENTATION_STRATEGY,
            segmentation_kwargs={"sentences_per_block": _INGESTION_SENTENCES_PER_BLOCK}
        )
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "INGESTED"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "ingestion", "status": "completed"})
        triple_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Ingestion failed for job {job_id}: {e}")
        # Mark failed on last retry
        if self.request.retries >= self.max_retries:
             with Session(engine) as session:
                job = session.query(Job).get(job_id)
                job.status = "FAILED"
                session.commit()
        raise


# ============================================================================
# Stage 2: Triple Extraction
# ============================================================================

@celery_app.task(
    name="stage.triples",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 60},
    retry_backoff=True
)
def triple_stage(self, job_id: int):
    """Triple extraction of facts from ingested TextBlocks."""
    from app.triples.processor import process_job_triples
    
    publish_event({"job_id": job_id, "stage": "triples", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "INGESTED":
            return

    try:
        process_job_triples(job_id)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "TRIPLES_EXTRACTED"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "triples", "status": "completed"})
        structural_graph_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Triple extraction failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 3: Structural Graph Building
# ============================================================================

@celery_app.task(
    name="stage.structural_graph",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 10}
)
def structural_graph_stage(self, job_id: int):
    """Compresses triples into a structural graph."""
    from app.graphs.structural import project_structural_graph
    from app.graphs.cache import set_structural_graph
    
    publish_event({"job_id": job_id, "stage": "structural_graph", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "TRIPLES_EXTRACTED":
            return
        
        # Load job config for excluded_entities
        job_config = None
        if job.job_config:
            from app.config.job_config import JobConfig
            job_config = JobConfig(**job.job_config) if isinstance(job.job_config, dict) else job.job_config
        
        excluded_entities = set(job_config.graph_config.excluded_entities) if job_config else set()

    try:
        proj = project_structural_graph(job_id, excluded_entities=excluded_entities)
        set_structural_graph(job_id, proj)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "STRUCTURAL_GRAPH_BUILT"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "structural_graph", "status": "completed"})
        sanitization_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Structural graph building failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 4: Graph Sanitization
# ============================================================================

@celery_app.task(
    name="stage.sanitization",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 10}
)
def sanitization_stage(self, job_id: int):
    """Cleans and validates the structural graph."""
    from app.graphs.sanitize import sanitize_graph
    from app.graphs.cache import get_structural_graph, delete_structural_graph, set_structural_graph
    
    publish_event({"job_id": job_id, "stage": "sanitization", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "STRUCTURAL_GRAPH_BUILT":
            return

    try:
        cached = get_structural_graph(job_id)
        if not cached:
            raise ValueError("Structural graph not found in cache")
            
        structural_data = cached.get("graph") if isinstance(cached, dict) and "graph" in cached else cached
        sanitized = sanitize_graph(structural_data)
        
        delete_structural_graph(job_id)
        set_structural_graph(job_id, sanitized) # Reuse key for next stage
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "GRAPH_SANITIZED"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "sanitization", "status": "completed"})
        semantic_merging_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Graph sanitization failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 5: Semantic Merging
# ============================================================================

@celery_app.task(
    name="stage.semantic_merging",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 60}
)
def semantic_merging_stage(self, job_id: int):
    """Merges nodes based on semantic similarity."""
    from app.graphs.semantic import merge_semantically
    from app.graphs.incremental import incremental_merge_semantically
    from app.graphs.cache import get_structural_graph, delete_structural_graph
    from app.graphs.persistence import persist_semantic_graph
    
    publish_event({"job_id": job_id, "stage": "semantic_merging", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "GRAPH_SANITIZED":
            return

    try:
        cached = get_structural_graph(job_id)
        if not cached:
            raise ValueError("Sanitized graph not found in cache")
            
        # Prefer incremental merge when possible to avoid full re-embedding
        semantic_graph = incremental_merge_semantically(
            job_id,
            cached,
            embedding_provider_name="sentence-transformers",
            similarity_threshold=_SEMANTIC_SIMILARITY_THRESHOLD,
        )
        
        delete_structural_graph(job_id)
        persist_semantic_graph(job_id, semantic_graph)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "GRAPH_SEMANTIC_MERGED"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "semantic_merging", "status": "completed"})
        path_reasoning_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Semantic merging failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 6: Path Reasoning
# ============================================================================

@celery_app.task(
    name="stage.path_reasoning",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 120},
    retry_backoff=True
)
def path_reasoning_stage(self, job_id: int):
    """Finds and filters logical paths (hypotheses)."""
    from app.graphs.persistence import get_semantic_graph, get_active_semantic_version
    from app.path_reasoning import run_path_reasoning
    from app.path_reasoning.persistence import persist_hypotheses, delete_all_hypotheses_for_job
    from app.path_reasoning.filtering import filter_hypotheses
    
    publish_event({"job_id": job_id, "stage": "path_reasoning", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "GRAPH_SEMANTIC_MERGED":
            return

    try:
        persisted_graph = get_semantic_graph(job_id)
        if not persisted_graph:
            raise ValueError("Semantic graph not found for reasoning")
            
        # Compute affected nodes (new canonical nodes) by comparing versions
        affected_nodes = set()
        try:
            active_ver = get_active_semantic_version(job_id)
            if active_ver and active_ver > 1:
                prev_graph = get_semantic_graph(job_id, version=active_ver - 1)
                prev_texts = set(n.get("text") for n in (prev_graph.get("nodes", []) if prev_graph else [] ) if isinstance(n, dict))
                curr_texts = set(n.get("text") for n in persisted_graph.get("nodes", []) if isinstance(n, dict))
                affected_nodes = curr_texts - prev_texts
        except Exception:
            affected_nodes = set()

        # Load and validate Job Configuration
        job_config = JobConfig(**(job.job_config or {}))
            
        # Reasoning params: System invariants from AdminPolicy, user tuning from JobConfig
        seeds = job_config.path_reasoning_config.seeds
        stoplist = set(job_config.path_reasoning_config.stoplist)
        preferred_predicates = job_config.hypothesis_config.preferred_predicates
        allow_len3 = admin_policy.algorithm.path_reasoning_defaults.allow_len3
        boost_factor = admin_policy.algorithm.path_reasoning_defaults.preferred_predicate_boost_factor

        # Expand seeds to include affected nodes so reasoning focuses on changed area
        effective_seeds = list(set(seeds or []) | set(affected_nodes))

        hypotheses = run_path_reasoning(
            persisted_graph,
            reasoning_mode="explore",
            seeds=effective_seeds,
            max_hops=_PATH_REASONING_MAX_HOPS,
            allow_len3=allow_len3,
            stoplist=stoplist,
            preferred_predicates=preferred_predicates,
            preferred_predicate_boost_factor=boost_factor,
        )

        hypotheses = filter_hypotheses(hypotheses, persisted_graph)

        # Persist incrementally: only deactivate hypotheses touching affected nodes
        persist_hypotheses(job_id, hypotheses, affected_nodes=affected_nodes if affected_nodes else None)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "PATH_REASONING_DONE"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "path_reasoning", "status": "completed"})
        decision_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Path reasoning failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 7: Decision
# ============================================================================

@celery_app.task(
    name="stage.decision",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30}
)
def decision_stage(self, job_id: int):
    """Analyzes measurements to decide next actions."""
    from app.graphs.persistence import get_semantic_graph
    from app.path_reasoning.persistence import get_hypotheses
    from app.decision.controller import get_decision_controller
    
    publish_event({"job_id": job_id, "stage": "decision", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "PATH_REASONING_DONE":
            return

    try:
        persisted_graph = get_semantic_graph(job_id)
        hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0)
        
        job_metadata = {
            "id": job.id,
            "status": job.status,
            "created_at": job.created_at.isoformat() if job.created_at else None,
        }

        controller = get_decision_controller()
        controller.decide(
            job_id=job_id,
            semantic_graph=persisted_graph,
            hypotheses=hypotheses,
            job_metadata=job_metadata,
        )
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "DECISION_MADE"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "decision", "status": "completed"})
        signal_evaluation_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Decision logic failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 8: Signal Evaluation
# ============================================================================

@celery_app.task(
    name="stage.signal_evaluation",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 2, "countdown": 15}
)
def signal_evaluation_stage(self, job_id: int):
    """Computes learning outcomes based on measurement deltas."""
    from app.signals.evaluator import find_pending_run_for_evaluation, get_last_decision_before_run, compute_measurement_delta
    from app.signals.applier import classify_signal, apply_signal_result
    
    publish_event({"job_id": job_id, "stage": "signal_evaluation", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "DECISION_MADE":
            return

    try:
        with Session(engine) as session:
            decision_record = session.query(DecisionResult).filter(
                DecisionResult.job_id == job_id
            ).order_by(DecisionResult.created_at.desc()).first()
            
            if not decision_record:
                logger.warning(f"No decision result found for job {job_id}, skipping signal evaluation")
            else:
                snapshot = {
                    "job_id": job_id,
                    "created_at": decision_record.created_at,
                    "measurements": decision_record.measurements_snapshot
                }
                
                pending_runs = find_pending_run_for_evaluation(job_id, snapshot, session)
                if pending_runs:
                    anchor_run = pending_runs[-1]
                    before_decision = get_last_decision_before_run(job_id, anchor_run, session)
                    
                    if before_decision:
                        delta = compute_measurement_delta(before_decision["measurements"], snapshot["measurements"])
                        val, status = classify_signal(delta)
                        for run in pending_runs:
                            apply_signal_result(run, val, status, session)
                            
        # Always proceed to handlers
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "RUNNING_HANDLERS"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "signal_evaluation", "status": "completed"})
        handler_execution_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Signal evaluation failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 9: Handler Execution
# ============================================================================

@celery_app.task(
    name="stage.handlers",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 30}
)
def handler_execution_stage(self, job_id: int):
    """Executes physical actions decided in Stage 7."""
    from app.graphs.persistence import get_semantic_graph
    from app.path_reasoning.persistence import get_hypotheses
    from app.decision.handlers.controller import get_handler_controller
    
    publish_event({"job_id": job_id, "stage": "handlers", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "RUNNING_HANDLERS":
            return

    try:
        graph = get_semantic_graph(job_id) or {}
        hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0) or []
        
        decision_record = session.query(DecisionResult).filter(
            DecisionResult.job_id == job_id
        ).order_by(DecisionResult.created_at.desc()).first()

        if not decision_record:
            return

        metadata = {"id": job.id, "status": job.status, "created_at": job.created_at.isoformat() if job.created_at else None}
        decision_data = {
            "decision_label": decision_record.decision_label,
            "provider_used": decision_record.provider_used,
            "measurements": decision_record.measurements_snapshot or {},
            "fallback_used": decision_record.fallback_used,
            "fallback_reason": decision_record.fallback_reason,
        }

        # Invoke handler controller
        # NOTE: Handlers themselves often transition the job to DONE, NEED_MORE_INPUT, or FETCH_QUEUED
        result = get_handler_controller().execute_handler(
            decision_label=decision_data["decision_label"],
            job_id=job_id,
            decision_result=decision_data,
            semantic_graph=graph,
            hypotheses=hypotheses,
            job_metadata=metadata,
        )
        
        # 10. Route based on the updated job status
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            status = job.status
            
            publish_event({"job_id": job_id, "stage": "handlers", "status": "completed", "outcome": status})
            
            if status == "FETCH_QUEUED":
                logger.info(f"Handler for job {job_id} requested FETCH_QUEUED; triggering fetch stage.")
                fetch_stage.delay(job_id)
            
            elif status == "DOWNLOAD_QUEUED":
                logger.info(f"Handler for job {job_id} requested DOWNLOAD_QUEUED; triggering download stage.")
                download_stage.delay(job_id)
            
            elif status == "COMPLETED":
                logger.info(f"Job {job_id} reached terminal state: COMPLETED")
                # Optimization: No next task needed
                
            elif status == "WAITING_FOR_USER":
                logger.info(f"Job {job_id} paused: WAITING_FOR_USER (Clarification needed)")
                
            elif status == "NEEDS_EXPERT_REVIEW":
                logger.info(f"Job {job_id} paused: NEEDS_EXPERT_REVIEW (Escalated)")
                
            elif status == "NEED_MORE_INPUT":
                logger.info(f"Job {job_id} paused: NEED_MORE_INPUT (Insufficient signal)")
                
            elif status == "MANUAL_REVIEW":
                logger.info(f"Job {job_id} paused: MANUAL_REVIEW (System uncertain)")
            
            else:
                logger.error(f"Job {job_id} transitioned to unhandled status: {status}. Halting task chain.")
    except Exception as e:
        logger.error(f"Handler execution failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 10: Fetching
# ============================================================================

@celery_app.task(
    name="stage.fetch",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 120},
    retry_backoff=True
)
def fetch_stage(self, job_id: int):
    """Executes the literature fetch pipeline."""
    from app.graphs.persistence import get_semantic_graph
    from app.path_reasoning.persistence import get_hypotheses
    from app.fetching.service import FetchService
    from app.llm import get_llm_service
    
    publish_event({"job_id": job_id, "stage": "fetch", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "FETCH_QUEUED":
            return

    try:
        graph = get_semantic_graph(job_id)
        if not graph:
            raise ValueError("Semantic graph missing for fetch")
            
        hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0) or []

        with Session(engine) as session:
            from app.fetching.service import get_fetch_service
            fetch_service = get_fetch_service()
            fetch_service.execute_fetch_stage(job_id, hypotheses, session)
            
            # CRITICAL: Commit the session to persist all fetched data
            session.commit()
        
        with Session(engine) as session:
            if verify_fetch_sources_ready(job_id, session):
                job = session.query(Job).get(job_id)
                job.status = "READY_TO_INGEST"
                session.commit()
            
                publish_event({"job_id": job_id, "stage": "fetch", "status": "completed", "outcome": "sources_ready"})
                # Chain back to start
                ingest_stage.delay(job_id)
            else:
                publish_event({"job_id": job_id, "stage": "fetch", "status": "completed", "outcome": "no_sources"})
                # No new papers; return to decision stage to reconsider strategy (e.g. try different query or halt)
                job = session.query(Job).get(job_id)
                if job:
                    job.status = "PATH_REASONING_DONE"
                    session.commit()
                    decision_stage.delay(job_id)
    
    except Exception as e:
        logger.error(f"Fetch stage failed for job {job_id}: {e}")
        raise



# ============================================================================
# Stage 11: Strategic Downloading
# ============================================================================

@celery_app.task(
    name="stage.download",
    bind=True,
    autoretry_for=(Exception,),
    retry_kwargs={"max_retries": 3, "countdown": 60},
    retry_backoff=True
)
def download_stage(self, job_id: int):
    """Downloads papers prioritized by impact score."""
    from app.fetching.downloader import get_paper_downloader
    
    publish_event({"job_id": job_id, "stage": "download", "status": "started"})
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "DOWNLOAD_QUEUED":
            logger.warning(f"Job {job_id} not in DOWNLOAD_QUEUED state (status={job.status if job else 'None'})")
            return

    try:
        downloader = get_paper_downloader()
        count = downloader.process_job_downloads(job_id)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            # After downloading, we are ready to ingest the new PDFs
            job.status = "READY_TO_INGEST"
            session.commit()
            
        publish_event({"job_id": job_id, "stage": "download", "status": "completed", "papers_downloaded": count})
        ingest_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Download stage failed for job {job_id}: {e}")
        raise
