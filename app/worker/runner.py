"""
Worker: orchestrates job pipeline stages.
Polls job status from DB, invokes appropriate service, handles success/failure,
updates status, and emits events. Orchestration logic only; services contain execution logic.
"""
import logging
import os
from sqlalchemy.orm import Session

from app.core.queues import job_queue, get_event_queue
from app.core.events import make_event
from app.storage.db import engine
from app.storage.models import Job, IngestionSource, File
from app.ingestion.service import IngestionService
from app.ingestion.input_handler import InputHandler

logger = logging.getLogger(__name__)

# Load configuration from environment
_INGESTION_SEGMENTATION_STRATEGY = os.getenv("INGESTION_SEGMENTATION_STRATEGY", "sentences")
_INGESTION_SENTENCES_PER_BLOCK = int(os.getenv("INGESTION_SENTENCES_PER_BLOCK", "3"))
_SEMANTIC_SIMILARITY_THRESHOLD = float(os.getenv("SEMANTIC_SIMILARITY_THRESHOLD", "0.85"))
_PATH_REASONING_MAX_HOPS = int(os.getenv("PATH_REASONING_MAX_HOPS", "2"))


def emit_event(event_queue, event_type: str, data) -> None:
    """Emit an event to the job's event queue."""
    try:
        event = make_event(event_type, data)
        event_queue.put(event)
    except Exception as e:
        logger.warning(f"Failed to emit event {event_type}: {e}")


def verify_fetch_sources_ready(job_id: int, session: Session) -> bool:
    """
    Verify that all fetched IngestionSource rows are ready for ingestion.
    
    STRICT TWO-PHASE MODEL:
    - Phase 1 (FETCH_QUEUED): FetchService creates IngestionSource rows with processed=false
    - Phase 2: This check verifies sources were created successfully
    - Then: Transition to READY_TO_INGEST so next process_job_stage() calls IngestionService.ingest_job()
    - Finally: IngestionService processes those sources as TextBlocks
    
    This ensures fetched abstracts follow the SAME lifecycle as PDFs and user text:
    1. Create IngestionSource row (processed=false)
    2. Wait for READY_TO_INGEST status
    3. IngestionService processes it to TextBlocks
    4. Continue with triple extraction, graph building, etc.
    
    Args:
        job_id: Job ID to check
        session: SQLAlchemy session
    
    Returns:
        True if unprocessed sources exist (ready for ingestion), False otherwise
    """
    from app.storage.models import IngestionSource, IngestionSourceType
    
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


def process_job_stage(job_id: int) -> None:
    """
    Orchestrate one stage of the job pipeline based on job.status in DB.
    
    This runner is the sole orchestrator: it decides when each service is invoked,
    handles success/failure explicitly, updates job status, and transitions to
    next phases. Services contain only execution logic, not orchestration.
    
    Status transitions:
    - READY_TO_INGEST → call IngestionService.ingest_job() → INGESTED or FAILED
    - INGESTED → call TripleExtractor → TRIPLES_EXTRACTED or FAILED
    - ... (and so on for other phases)
    """
    logger.info("running : {job.status}")
    with Session(engine) as session:
        job = session.query(Job).filter(Job.id == job_id).first()
        
        if not job:
            logger.error(f"Job {job_id} not found")
            return
        
        eq = get_event_queue(job_id)
        logger.info(f"Processing job {job_id}, status: {job.status}")
        
        # Status: READY_TO_INGEST → Run ingestion (pull IngestionSource rows and process)
        if job.status == "READY_TO_INGEST":
            emit_event(eq, "status", f"Starting ingestion for job {job_id}")
            logger.info(f"Job {job_id} is READY_TO_INGEST — invoking IngestionService")
            
            try:
                # Ingestion service handles all unprocessed IngestionSource rows
                result = IngestionService.ingest_job(
                    job_id=job_id,
                    segmentation_strategy=_INGESTION_SEGMENTATION_STRATEGY,
                    segmentation_kwargs={"sentences_per_block": _INGESTION_SENTENCES_PER_BLOCK},
                    normalization_kwargs={}
                )
                
                # Success: emit result and update status
                emit_event(eq, "ingestion", result)
                job.status = "INGESTED"
                session.commit()
                
                emit_event(eq, "status", f"Ingestion complete: {result['blocks_created']} blocks created")
                logger.info(f"Job {job_id} ingestion succeeded: {result}")
                
                job_queue.put(job_id)
            except RuntimeError as e:
                # Status mismatch or other orchestration error
                emit_event(eq, "error", str(e))
                job.status = "FAILED"
                session.commit()
                logger.error(f"Job {job_id} ingestion failed (status mismatch): {str(e)}")
                
            except Exception as e:
                # Ingestion execution error
                emit_event(eq, "error", str(e))
                job.status = "FAILED"
                session.commit()
                logger.error(f"Job {job_id} ingestion failed (execution error): {str(e)}", exc_info=True)
        
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

                job_queue.put(job_id)
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Triple extraction failed for job {job_id}: {e}")
        
        # Status: TRIPLES_EXTRACTED → Run structural compression (Phase 2)
        elif job.status == "TRIPLES_EXTRACTED":
            emit_event(eq, "status", "Starting structural compression for job")
            logger.info(f"Job {job_id} is TRIPLES_EXTRACTED — running structural compression")
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

                job_queue.put(job_id)
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

                # logger.info(f"\n\n{sanitized}\n\n")
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

                job_queue.put(job_id)
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
                # Use similarity_threshold from env (default 0.85 = safe merging)
                semantic_graph = merge_semantically(
                    cached,
                    embedding_provider_name="sentence-transformers",
                    similarity_threshold=_SEMANTIC_SIMILARITY_THRESHOLD,
                )

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

                job_queue.put(job_id)
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
                from app.path_reasoning.persistence import persist_hypotheses, delete_all_hypotheses_for_job
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
                    max_hops=_PATH_REASONING_MAX_HOPS,
                    allow_len3=allow_len3,
                    stoplist=stoplist,
                )

                # Phase-4.5: Filtering
                # Modifies hypotheses dicts in-place with passed_filter/filter_reason keys
                hypotheses = filter_hypotheses(hypotheses, persisted)

                # SINGLE-ACTIVE-STATE: Delete all existing hypotheses for this job before inserting fresh ones
                deleted_count = delete_all_hypotheses_for_job(job_id)
                
                inserted = persist_hypotheses(job_id, hypotheses)

                emit_event(eq, "path_reasoning", {"mode": reasoning_mode, "hypotheses_count": len(hypotheses), "inserted": inserted})
                emit_event(eq, "path_reasoning_results", hypotheses)

                job.status = "PATH_REASONING_DONE"
                session.commit()

                passed = 0
                for o in hypotheses:
                    if o.get("passed_filter"):
                        passed += 1

                logger.info(f"\n\n\nJob {job_id} path reasoning complete: {len(hypotheses)} hypotheses, {passed} passed filter, {inserted} persisted")
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

                job_queue.put(job_id)
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Decision & control failed for job {job_id}: {e}")

        # Status: DECISION_MADE → Evaluate signal proceed to handlers
        elif job.status == "DECISION_MADE":
            emit_event(eq, "status", "Evaluating signal from last fetch (if any)")
            logger.info(f"Job {job_id} is DECISION_MADE — evaluating signal")
            
            try:
                from app.storage.models import SearchQueryRun, DecisionResult
                from app.signals.evaluator import (
                    find_pending_run_for_evaluation, 
                    get_last_decision_before_run, 
                    compute_measurement_delta
                )
                from app.signals.applier import classify_signal, apply_signal_result
                
                # 1. Retrieve the Current Decision (the one that just occurred)
                decision_record = session.query(DecisionResult).filter(
                    DecisionResult.job_id == job_id
                ).order_by(DecisionResult.created_at.desc()).first()
                
                if not decision_record:
                    logger.error(f"No decision result found for job {job_id}")
                    return

                # Create snapshot dict for the evaluator
                current_decision_snapshot = {
                    "job_id": job_id,
                    "created_at": decision_record.created_at,
                    "measurements": decision_record.measurements_snapshot
                }

                # 2. Find pending SearchQueryRun strictly between previous decision and this one
                pending_run = find_pending_run_for_evaluation(
                    job_id=job_id,
                    current_decision=current_decision_snapshot,
                    session=session
                )
                
                if pending_run:
                    logger.info(f"Refactoring Signal: Found pending SearchQueryRun {pending_run.id}")
                    
                    # 3. Get the decision strictly BEFORE this run
                    before_decision = get_last_decision_before_run(job_id, pending_run, session)
                    
                    if before_decision:
                        # 4. Compute Delta (Current - Previous)
                        delta = compute_measurement_delta(
                            before_decision["measurements"],
                            current_decision_snapshot["measurements"]
                        )
                        logger.info(f"Signal delta: {delta:.3f}")
                        
                        # 5. Classify
                        signal_value, new_status = classify_signal(delta)
                        
                        # 6. Apply Signal (Attribution)
                        apply_signal_result(pending_run, signal_value, new_status, session)
                        session.commit()
                        
                        emit_event(eq, "signal", {
                            "delta": delta,
                            "signal_value": signal_value,
                            "status": new_status,
                            "search_query_id": pending_run.search_query_id
                        })
                        
                        logger.info(f"Signal applied to Run {pending_run.id}: value={signal_value}, status={new_status}")
                    else:
                        logger.warning(f"Could not find decision before SearchQueryRun {pending_run.id}; skipping signal.")
                else:
                    logger.debug("No pending SearchQueryRun found in strict timing window (Signal skipped).")
                
                # Proceed to handlers for other processes
                decision_label = decision_record.decision_label
                logger.info(f"Decision outcome: {decision_label}")
                
                job.status = "RUNNING_HANDLERS"
                session.commit()
                    
                job_queue.put(job_id)

            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"Signal evaluation or decision routing failed for job {job_id}: {e}", exc_info=True)

        # Status: FETCH_QUEUED → Execute fetch pipeline (Phase 1: Create IngestionSources only)
        elif job.status == "FETCH_QUEUED":
            emit_event(eq, "status", "Starting FETCH_MORE literature pipeline")
            logger.info(f"Job {job_id} is FETCH_QUEUED — executing fetch service")
            try:
                from app.graphs.persistence import get_semantic_graph
                from app.path_reasoning.persistence import get_hypotheses
                from app.fetching.service import FetchService
                from app.llm.service import get_llm_service

                # Load persisted artifacts
                persisted_graph = get_semantic_graph(job_id)
                if not persisted_graph:
                    msg = f"Semantic graph for job {job_id} not found for FETCH_MORE"
                    emit_event(eq, "error", msg)
                    job.status = "FAILED"
                    session.commit()
                    logger.error(msg)
                    return

                hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0) or []
                passed_hypotheses = [h for h in hypotheses if h.get("passed_filter", False)]

                if not passed_hypotheses:
                    logger.warning(f"No passed hypotheses for job {job_id}, cannot fetch")
                    job.status = "GRAPH_SEMANTIC_MERGED"
                    session.commit()
                    return

                # Execute FETCH_MORE (Phase 1: Creates IngestionSource rows only, never ingests)
                llm_client = get_llm_service()
                fetch_service = FetchService(llm_client=llm_client)
                fetch_result = fetch_service.execute_fetch_more(
                    job_id=job_id,
                    hypotheses=passed_hypotheses,
                    session=session
                )

                emit_event(eq, "fetch_result", fetch_result)
                logger.info(f"FETCH_MORE completed: created {len(fetch_result['ingestion_sources'])} IngestionSource rows")

                # Phase 2: Check if all fetched sources are ready, then transition to READY_TO_INGEST
                if len(fetch_result["ingestion_sources"]) > 0:
                    # Verify all sources for this fetch cycle have been created
                    sources_ready = verify_fetch_sources_ready(job_id, session)
                    if sources_ready:
                        job.status = "READY_TO_INGEST"
                        session.commit()
                        emit_event(eq, "status", f"Fetched abstracts ready: {fetch_result['ingestion_sources']} IngestionSource rows created, job ready for ingestion")
                        logger.info(f"Job {job_id} transitioned to READY_TO_INGEST after fetch (will process next cycle)")
                        job_queue.put(job_id)
                    else:
                        logger.warning(f"Fetch sources not ready for job {job_id}, keeping in FETCH_QUEUED")
                else:
                    # No sources fetched; go back to decision phase
                    job.status = "DECISION_MADE"
                    session.commit()
                    emit_event(eq, "status", "No sources fetched, returning to decision phase")
                    logger.info(f"Job {job_id} returned to DECISION_MADE (no sources fetched)")
            except Exception as e:
                emit_event(eq, "error", str(e))
                logger.error(f"FETCH_MORE pipeline failed for job {job_id}: {e}", exc_info=True)
                job.status = "FAILED"
                session.commit()

        # Status: RUNNING_HANDLERS → Execute decision handlers
        elif job.status == "RUNNING_HANDLERS":
            emit_event(eq, "status", "Starting decision handler execution")
            logger.info(f"Job {job_id} is RUNNING_HANDLERS — running decision handlers")
            try:
                from app.graphs.persistence import get_semantic_graph
                from app.path_reasoning.persistence import get_hypotheses
                from app.decision.handlers.controller import get_handler_controller
                from app.storage.models import DecisionResult

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
                    "created_at": job.created_at.isoformat() if job.created_at else None,
                }

                # Load the decision result from DB for handler
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

                job_queue.put(job_id)
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


def start_worker() -> None:
    """
    Main worker loop: blocking queue.get(), process one stage per job.
    """
    logger.info("Worker started")
    job_queue.put(1)
    
    while True:
        job_id = job_queue.get()  # blocks until job available
        logger.info(f"Received job {job_id} from queue")
        
        try:
            process_job_stage(job_id=1)
        except Exception as e:
            logger.error(f"Unexpected error processing job {job_id}: {str(e)}")


# Import File after function definitions to avoid circular import
from app.storage.models import File
