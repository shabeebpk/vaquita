"""
Worker: processes one job stage per queue event.
Reads job status from DB, executes appropriate pipeline stage, updates status.
No loops, no recursion, no ingestion logic here.
"""
import logging
from sqlalchemy.orm import Session

from app.core.queues import job_queue, get_event_queue
from app.core.events import make_event
from app.storage.db import engine
from app.storage.models import Job
from app.ingestion.files import save_file
from app.ingestion.service import IngestionService

logger = logging.getLogger(__name__)


def process_job_stage(job_id: int) -> None:
    """
    Process one stage of the job pipeline based on job.status in DB.
    
    - CREATED → run ingestion
    - INGESTED → ready for next stage (external/agent decision)
    - NEED_MORE_INPUT → pause
    - DONE → exit
    """
    with Session(engine) as session:
        job = session.query(Job).filter(Job.id == job_id).first()
        
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        eq = get_event_queue(job_id)
        logger.info(f"Processing job {job_id}, status: {job.status}")
        
        # Status: CREATED → Run ingestion
        if job.status == "CREATED":
            emit_event(eq, "status", f"Starting ingestion for job {job_id}")
            
            try:
                # Get file paths from files table
                files = session.query(File).filter(File.job_id == job_id).all()
                file_paths = [(f.stored_path, f.file_type) for f in files]
                
                # Run ingestion service
                result = IngestionService.ingest_job(
                    job_id=job_id,
                    user_text=job.user_text,
                    file_paths=file_paths,
                    segmentation_strategy="sentences",
                    segmentation_kwargs={"sentences_per_block": 3},
                    normalization_kwargs={}
                )
                
                emit_event(eq, "ingestion", result)
                
                # Update job status
                job.status = "INGESTED"
                session.commit()
                
                emit_event(eq, "status", f"Ingestion complete: {result['text_blocks']} blocks created")
                logger.info(f"Job {job_id} ingestion complete")
                
            except Exception as e:
                emit_event(eq, "error", str(e))
                job.status = "FAILED"
                session.commit()
                logger.error(f"Job {job_id} ingestion failed: {str(e)}")
        
        # Status: INGESTED → Run triple extraction stage (minimal integration)
        elif job.status == "INGESTED":
            emit_event(eq, "status", "Starting triple extraction for job")
            logger.info(f"Job {job_id} is INGESTED — running triple extraction")
            try:
                # Import here to avoid circular imports at module load
                from app.triples.processor import process_job_triples

                provider = None  # provider decided via env inside TripleExtractor
                summary = process_job_triples(job_id, provider=provider)
                emit_event(eq, "triples", summary)

                # Update job status to reflect triples have been extracted
                job.status = "TRIPLES_EXTRACTED"
                session.commit()
                emit_event(eq, "status", f"Triple extraction complete: {summary.get('triples_created', 0)} triples")
                logger.info(f"Job {job_id} triples extracted: {summary}")
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Triple extraction failed for job {job_id}: {e}")
        
        # Status: TRIPLES_EXTRACTED → Run evidence aggregation
        elif job.status == "TRIPLES_EXTRACTED":
            emit_event(eq, "status", "Starting evidence aggregation for job")
            logger.info(f"Job {job_id} is TRIPLES_EXTRACTED — running evidence aggregation")
            try:
                # Import here to avoid circular imports at module load
                from app.graphs.aggregator import aggregate_evidence_for_job

                # Default threshold is 2; can be configured via env later
                threshold = 2
                result = aggregate_evidence_for_job(job_id, threshold=threshold)
                emit_event(eq, "aggregation", result)

                # Update job status to reflect aggregation is complete
                job.status = "GRAPH_AGGREGATED"
                session.commit()
                emit_event(eq, "status", f"Evidence aggregation complete: {result.get('filtered_groups', 0)} groups above threshold")
                logger.info(f"Job {job_id} evidence aggregated: {result}")
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Evidence aggregation failed for job {job_id}: {e}")
        
        # Status: GRAPH_AGGREGATED -> Run structural compression (Phase 2)
        elif job.status == "GRAPH_AGGREGATED":
            emit_event(eq, "status", "Starting structural compression for job")
            logger.info(f"Job {job_id} is GRAPH_AGGREGATED — running structural compression")
            try:
                # Import here to avoid circular imports at module load
                from app.graphs.structural import project_structural_graph
                from app.graphs.cache import set_structural_graph

                proj = project_structural_graph(job_id)
                emit_event(eq, "structural", proj)

                # Cache the Phase-2 structural graph result for Phase-2.5
                try:
                    set_structural_graph(job_id, proj)
                except Exception:
                    logger.warning("Failed to cache structural graph for job %s", job_id)

                # Update job status to reflect structural graph is built
                job.status = "STRUCTURAL_GRAPH_BUILT"
                session.commit()
                emit_event(eq, "status", f"Structural compression complete: {proj.get('projected_groups', 0)} groups")
                logger.info(f"Job {job_id} structural compression complete: {proj}")

                job_queue.put(6)
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Structural compression failed for job {job_id}: {e}")
        
        # Status: STRUCTURAL_GRAPH_BUILT -> Run graph sanitization (Phase 2.5)
        elif job.status == "STRUCTURAL_GRAPH_BUILT":
            emit_event(eq, "status", "Starting graph sanitization for job")
            logger.info(f"Job {job_id} is STRUCTURAL_GRAPH_BUILT — running sanitization")
            try:
                # Import here to avoid circular imports at module load
                from app.graphs.sanitize import sanitize_graph
                from app.graphs.cache import get_structural_graph, delete_structural_graph

                # Retrieve the Phase-2 structural graph from cache (do not recompute)
                cached = get_structural_graph(job_id)
                if not cached:
                    msg = (
                        f"Structural graph for job {job_id} not found in cache. "
                        "Phase-2 must populate the cache before Phase-2.5."
                    )
                    emit_event(eq, "error", msg)
                    job.status = "FAILED"
                    session.commit()
                    logger.error(msg)
                    return

                structural_graph = cached.get("graph") if isinstance(cached, dict) and "graph" in cached else cached

                # Apply Phase 2.5 sanitization (read-only)
                sanitized = sanitize_graph(structural_graph)

                logger.info(f"\n\n{sanitized}\n\n")
                # Optionally remove cached structural graph now that sanitization ran
                try:
                    delete_structural_graph(job_id)
                except Exception:
                    logger.debug("Could not delete cached structural graph for job %s", job_id)
                emit_event(eq, "sanitization", sanitized)

                # Cache sanitized graph for Phase-3
                from app.graphs.cache import set_structural_graph
                try:
                    set_structural_graph(job_id, sanitized)
                except Exception:
                    logger.warning("Failed to cache sanitized graph for job %s", job_id)

                # Update job status to reflect sanitization is complete
                job.status = "GRAPH_SANITIZED"
                session.commit()
                emit_event(eq, "status", f"Graph sanitization complete: {sanitized.get('summary', {}).get('total_nodes_after', 0)} nodes after cleanup")
                logger.info(f"Job {job_id} graph sanitized: {sanitized['summary']}")

                job_queue.put(6)
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Graph sanitization failed for job {job_id}: {e}")
        
        # Status: GRAPH_SANITIZED -> Run semantic merging (Phase 3)
        elif job.status == "GRAPH_SANITIZED":
            emit_event(eq, "status", "Starting semantic merging for job")
            logger.info(f"Job {job_id} is GRAPH_SANITIZED — running semantic merging")
            try:
                from app.graphs.semantic import merge_semantically
                from app.graphs.cache import get_structural_graph, delete_structural_graph
                from app.graphs.persistence import persist_semantic_graph

                # Retrieve cached sanitized graph
                cached = get_structural_graph(job_id)
                if not cached:
                    msg = (
                        f"Sanitized graph for job {job_id} not found in cache. "
                        "Phase-2.5 must populate the cache before Phase-3."
                    )
                    emit_event(eq, "error", msg)
                    job.status = "FAILED"
                    session.commit()
                    logger.error(msg)
                    return

                # Apply Phase-3 semantic merging
                # Use default similarity_threshold=0.85 (safe merging)
                semantic_graph = merge_semantically(
                    cached,
                    embedding_provider_name="sentence-transformers",
                    similarity_threshold=0.85,
                )

                logger.info(f"\n\n\n(me): semantic graph: {semantic_graph}\n\n\n")

                emit_event(eq, "semantic", semantic_graph)

                # Clean up cached sanitized graph
                try:
                    delete_structural_graph(job_id)
                except Exception:
                    logger.debug("Could not delete cached sanitized graph for job %s", job_id)

                # Persist Phase-3 semantic graph to database
                try:
                    persist_semantic_graph(job_id, semantic_graph)
                    logger.info(f"Persisted semantic graph for job {job_id}")
                except Exception as e:
                    logger.error(f"Failed to persist semantic graph for job {job_id}: {e}")
                    emit_event(eq, "error", f"Failed to persist semantic graph: {str(e)}")
                    job.status = "FAILED"
                    session.commit()
                    return

                # Update job status to reflect semantic merging is complete
                job.status = "GRAPH_SEMANTIC_MERGED"
                session.commit()
                emit_event(eq, "status", "Semantic merging complete")
                logger.info(f"Job {job_id} semantic merging complete")
                # Do NOT trigger path reasoning here. GRAPH_SEMANTIC_MERGED strictly
                # indicates Phase-3 completed and the semantic graph persisted.
                # Transition into PATH_REASONING_RUNNING must be done explicitly
                # (e.g. by UI/operator) to start Phase-4.
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Semantic merging failed for job {job_id}: {e}")
        
        # Status: GRAPH_SEMANTIC_MERGED -> Run path reasoning (Phase 4)
        elif job.status == "GRAPH_SEMANTIC_MERGED":
            emit_event(eq, "status", "Starting path reasoning for job")
            logger.info(f"Job {job_id} is PATH_REASONING_RUNNING — running path reasoning")
            try:
                from app.graphs.persistence import get_semantic_graph
                from app.path_reasoning import run_path_reasoning
                from app.path_reasoning.persistence import persist_hypotheses
                import os

                persisted = get_semantic_graph(job_id)
                if not persisted:
                    msg = (
                        f"Persisted semantic graph for job {job_id} not found. "
                        "Phase-3 must persist the semantic graph before Phase-4."
                    )
                    emit_event(eq, "error", msg)
                    job.status = "FAILED"
                    session.commit()
                    logger.error(msg)
                    return

                # When a job reaches GRAPH_SEMANTIC_MERGED we must run exactly one
                # automatic explore precomputation. Force explore mode here to
                # ensure deterministic, read-only behavior and to persist
                # hypotheses with query_id = NULL.
                reasoning_mode = "explore"
                seeds_env = os.getenv("PATH_REASONING_SEEDS", "")
                seeds = [s.strip() for s in seeds_env.split(",") if s.strip()] if seeds_env else None
                allow_len3 = os.getenv("PATH_REASONING_ALLOW_LEN3", "0") == "1"
                stoplist_env = os.getenv("PATH_REASONING_STOPLIST", "")
                stoplist = set(s.strip().lower() for s in stoplist_env.split(",") if s.strip()) if stoplist_env else None

                from app.path_reasoning.filtering import filter_hypotheses

                hypotheses = run_path_reasoning(
                    persisted,
                    reasoning_mode=reasoning_mode,
                    seeds=seeds,
                    max_hops=2,
                    allow_len3=allow_len3,
                    stoplist=stoplist,
                )

                # Phase-4.5: Filtering
                # Modifies hypotheses dicts in-place with passed_filter/filter_reason keys
                hypotheses = filter_hypotheses(hypotheses, persisted)

                inserted = persist_hypotheses(job_id, hypotheses)

                emit_event(eq, "path_reasoning", {"mode": reasoning_mode, "hypotheses_count": len(hypotheses), "inserted": inserted})
                emit_event(eq, "path_reasoning_results", hypotheses)

                job.status = "PATH_REASONING_DONE"
                session.commit()

                passed = 0
                for o in hypotheses:
                    if o.get("passed_filter"):
                        passed += 1

                logger.info(f"Job {job_id} path reasoning complete: {len(hypotheses)} hypotheses, {passed} passed filter, {inserted} persisted")
                job_queue.put(6) #delete this
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Path reasoning failed for job {job_id}: {e}")
        

        # Status: PATH_REASONING_DONE → Run decision & control (Phase-5)
        elif job.status == "PATH_REASONING_DONE":
            emit_event(eq, "status", "Starting decision & control for job")
            logger.info(f"Job {job_id} is PATH_REASONING_DONE — running decision & control")
            try:
                from app.graphs.persistence import get_semantic_graph
                from app.path_reasoning.persistence import get_hypotheses
                from app.decision.controller import get_decision_controller

                # Load persisted semantic graph
                persisted_graph = get_semantic_graph(job_id)
                if not persisted_graph:
                    msg = (
                        f"Persisted semantic graph for job {job_id} not found. "
                        "Phase-3 must persist the semantic graph before Phase-5."
                    )
                    emit_event(eq, "error", msg)
                    job.status = "FAILED"
                    session.commit()
                    logger.error(msg)
                    return

                # Load persisted hypotheses (both explore and query modes)
                hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0)

                # Prepare job metadata
                job_metadata = {
                    "id": job.id,
                    "status": job.status,
                    "user_text": job.user_text or "",
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                }

                # Invoke decision controller
                controller = get_decision_controller()
                decision_result = controller.decide(
                    job_id=job_id,
                    semantic_graph=persisted_graph,
                    hypotheses=hypotheses,
                    job_metadata=job_metadata,
                )

                emit_event(eq, "decision", decision_result)
                logger.info(f"Job {job_id} decision complete: {decision_result['decision_label']} and \n\n decisions: {decision_result} \n\n")

                # Update job status to reflect decision-making is complete
                job.status = "DECISION_MADE"
                session.commit()
                emit_event(eq, "status", f"Decision made: {decision_result['decision_label']}")

            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Decision & control failed for job {job_id}: {e}")
        
        # Status: DECISION_MADE → Run decision handlers (Control Layer)
        elif job.status == "DECISION_MADE":
            emit_event(eq, "status", "Starting decision handler execution")
            logger.info(f"Job {job_id} is DECISION_MADE — running decision handlers")
            try:
                from app.graphs.persistence import get_semantic_graph
                from app.path_reasoning.persistence import get_hypotheses
                from app.decision.handlers.controller import get_handler_controller

                # Load persisted artifacts for handler execution
                persisted_graph = get_semantic_graph(job_id)
                if not persisted_graph:
                    logger.warning(f"Semantic graph for job {job_id} not found for handler execution")
                    persisted_graph = {}

                hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0) or []

                # Prepare job metadata
                job_metadata = {
                    "id": job.id,
                    "status": job.status,
                    "user_text": job.user_text or "",
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                }

                # Load the decision result from DB for handler
                from app.storage.models import DecisionResult
                decision_record = session.query(DecisionResult).filter(
                    DecisionResult.job_id == job_id
                ).order_by(DecisionResult.created_at.desc()).first()

                if not decision_record:
                    msg = f"No decision result found for job {job_id}"
                    emit_event(eq, "error", msg)
                    logger.error(msg)
                    return

                decision_result = {
                    "decision_label": decision_record.decision_label,
                    "provider_used": decision_record.provider_used,
                    "measurements": decision_record.measurements_snapshot or {},
                    "fallback_used": decision_record.fallback_used,
                    "fallback_reason": decision_record.fallback_reason,
                }

                # Invoke handler controller
                handler_controller = get_handler_controller()
                handler_result = handler_controller.execute_handler(
                    decision_label=decision_result["decision_label"],
                    job_id=job_id,
                    decision_result=decision_result,
                    semantic_graph=persisted_graph,
                    hypotheses=hypotheses,
                    job_metadata=job_metadata,
                )

                emit_event(eq, "handler_result", {
                    "decision_label": decision_result["decision_label"],
                    "handler_status": handler_result.status,
                    "message": handler_result.message,
                    "next_action": handler_result.next_action,
                })

                logger.info(
                    f"Job {job_id} handler execution complete: "
                    f"decision={decision_result['decision_label']}, "
                    f"status={handler_result.status}, "
                    f"message={handler_result.message}"
                )

                # Emit handler data if available
                if handler_result.data:
                    emit_event(eq, "handler_data", handler_result.data)

            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Handler execution failed for job {job_id}: {e}")

        # Status: NEED_MORE_INPUT → Pause
        elif job.status == "NEED_MORE_INPUT":
            emit_event(eq, "status", "Job paused, waiting for user input")
            logger.info(f"Job {job_id} paused, waiting for user input")
        
        # Status: DONE → Exit
        elif job.status == "DONE":
            emit_event(eq, "done", {"job_id": job_id, "status": "DONE"})
            logger.info(f"Job {job_id} is complete")
        
        else:
            logger.warning(f"Job {job_id} has unknown status: {job.status}")


def emit_event(event_queue, event_type: str, data) -> None:
    """Helper: emit event to job's event queue."""
    event = make_event(event_type, data)
    event_queue.put(event)


def start_worker() -> None:
    """
    Main worker loop: blocking queue.get(), process one stage per job.
    """
    logger.info("Worker started")
    
    while True:
        job_id = job_queue.get()  # blocks until job available
        logger.info(f"Received job {job_id} from queue")
        
        try:
            process_job_stage(job_id=6)
        except Exception as e:
            logger.error(f"Unexpected error processing job {job_id}: {str(e)}")


# Import File after function definitions to avoid circular import
from app.storage.models import File
