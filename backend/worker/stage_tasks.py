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

from app.storage.models import Job, File, IngestionSource, IngestionSourceType, ConversationMessage, MessageRole, MessageType, DecisionResult, VerificationResult
from app.storage.db import engine
from app.config.job_config import JobConfig
from events import publish_event
from presentation.events import push_presentation_event

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
# Legacy extraction constants removed
_SEMANTIC_SIMILARITY_THRESHOLD = admin_policy.algorithm.graph_merging.similarity_threshold
_MIN_NODE_TEXT_LENGTH = admin_policy.algorithm.graph_merging.min_node_text_length

# (classify_stage removed; classification is now handled synchronously in app/api/chat.py)


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
    
    with Session(engine) as session:
        file_row = session.query(File).filter(File.id == file_id).first()
        if not file_row:
            logger.error(f"File {file_id} not found for extraction")
            return
            
        try:
            # New Precision Flow: We don't extract raw text here.
            # We just create an IngestionSource pointing to the file.
            # The IngestionService will run the layout-aware DLA later.
            
            source = IngestionSource(
                job_id=job_id,
                source_type=IngestionSourceType.PDF_TEXT.value if file_row.file_type == "pdf" else IngestionSourceType.API_TEXT.value,
                source_ref=f"file:{file_id}",
                raw_text="", # Will be populated via Precision Extraction in Ingest stage
                processed=False
            )
            session.add(source)
            session.commit()
            logger.info(f"File {file_id} registered for precision ingestion.")
            return True
            
        except Exception as e:
            logger.error(f"Extraction failed for file {file_id}: {e}")
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
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job:
            logger.warning(f"Job {job_id} not found for ingestion.")
            return
        
        # No safety gate: Reliance on chord/trigger synchronization.
        # Ensure status is at least READY_TO_INGEST if called unexpectedly.
        if job.status != "READY_TO_INGEST":
             logger.info(f"Job {job_id} is in status '{job.status}'. Skipping ingestion trigger.")
             return

    try:
        ingest_result = IngestionService.ingest_job(job_id=job_id) or {}

        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "INGESTED"
            session.commit()

            # Gather ingestion metrics from DB
            from app.storage.models import IngestionSource, IngestionSourceType, TextBlock
            total_ingested = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id
            ).count()
            paper_count = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id,
                IngestionSource.source_type == IngestionSourceType.PAPER_ABSTRACT.value
            ).count()
            upload_count = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id,
                IngestionSource.source_type == IngestionSourceType.PDF_TEXT.value
            ).count()
            total_blocks = session.query(TextBlock).filter(
                TextBlock.job_id == job_id
            ).count()

        push_presentation_event(
            job_id=job_id,
            phase="INGESTION",
            status=None,
            result={
                "total_ingested": total_ingested,
                "paper_count": paper_count,
                "upload_count": upload_count,
                "abstract_only_count": paper_count,
            },
            metric={
                "total_blocks": total_blocks,
                "llms_used": admin_policy.algorithm.refinery.model,
            },
            next_action="triple_extraction",
        )

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

        # Query triple count here
        from app.storage.models import Triple, TextBlock
        with Session(engine) as session:
            total_triples = session.query(Triple).filter(Triple.job_id == job_id).count()
            total_blocks = session.query(TextBlock).filter(TextBlock.job_id == job_id).count()

        push_presentation_event(
            job_id=job_id,
            phase="TRIPLES",
            status=None,
            result={
                "total_triples": total_triples,
                "blocks_processed": total_blocks,
                "avg_triples_per_block": round(total_triples / total_blocks, 2) if total_blocks else 0,
            },
            next_action="graph_building",
        )
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
            embedding_provider_name="sentence_transformers",
            similarity_threshold=_SEMANTIC_SIMILARITY_THRESHOLD,
        )
        
        delete_structural_graph(job_id)
        persist_semantic_graph(job_id, semantic_graph)

        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "GRAPH_SEMANTIC_MERGED"
            session.commit()

        _nodes = semantic_graph.get("nodes") or []
        _edges = semantic_graph.get("edges") or []
        _version = semantic_graph.get("version", 1)
        _merges = semantic_graph.get("merge_count", len(_nodes))

        push_presentation_event(
            job_id=job_id,
            phase="GRAPH",
            status=None,
            result={
                "node_count": len(_nodes),
                "edge_count": len(_edges),
                "graph_version": _version,
                "semantic_merges": _merges,
            },
            next_action="path_reasoning",
        )
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
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "GRAPH_SEMANTIC_MERGED":
            return
        job_mode = job.mode or "discovery"
        # parse job config for both modes
        job_config = None
        if job.job_config and isinstance(job.job_config, dict):
            from app.config.job_config import JobConfig
            job_config = JobConfig(**job.job_config)

    try:
        persisted_graph = get_semantic_graph(job_id)
        if not persisted_graph:
            raise ValueError("Semantic graph not found for reasoning")

        # If this job is verification mode, bypass normal hypothesis generation
        if job_mode == "verification":
            # Obtain source/target from VerificationResult table (not job_config)
            src = None
            tgt = None
            with Session(engine) as session:
                vr = session.query(VerificationResult).filter(VerificationResult.job_id == job_id).first()
                if vr:
                    src = vr.source
                    tgt = vr.target
            
            if not src or not tgt:
                logger.error(f"Job {job_id} verification mode but no source/target found in VerificationResult table")
                raise ValueError("Verification job missing source/target from VerificationResult table")
            
            from app.path_reasoning.reasoning import run_path_reasoning_verification
            result = run_path_reasoning_verification(
                persisted_graph,
                src,
                tgt,
                stoplist=set(job_config.path_reasoning_config.stoplist) if job_config else None,
            )
            # persist the verification result on the job
            with Session(engine) as session2:
                job2 = session2.query(Job).get(job_id)
                job2.result = {
                    "verification_result": result,
                    "verification_source": src,
                    "verification_target": tgt,
                }
                job2.status = "PATH_REASONING_DONE"
                session2.commit()
            push_presentation_event(
                job_id=job_id,
                phase="PATHREASONING",
                status="VERIFICATION",
                result={
                    "hypothesis_count": 1,
                    "passed_count": 1,
                    "hubs_suppressed": 0,
                    "hub_suppression_summary": "Verification bypasses hub suppression.",
                },
                next_action="decision",
            )
            decision_stage.delay(job_id)
            return

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
        boost_factor = admin_policy.algorithm.path_reasoning_defaults.preferred_predicate_boost_factor
        job_domain = job_config.domain

        # Expand seeds to include affected nodes so reasoning focuses on changed area
        effective_seeds = list(set(seeds or []) | set(affected_nodes))

        hypotheses = run_path_reasoning(
            persisted_graph,
            reasoning_mode="explore",
            seeds=effective_seeds,
            stoplist=stoplist,
            preferred_predicates=preferred_predicates,
            preferred_predicate_boost_factor=boost_factor,
            job_domain=job_domain,
        )

        passed_hypotheses, failed_hypotheses = filter_hypotheses(hypotheses, persisted_graph)

        # Persist incrementally: only deactivate hypotheses touching affected nodes
        persist_hypotheses(job_id, passed_hypotheses, affected_nodes=affected_nodes if affected_nodes else None)
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "PATH_REASONING_DONE"
            session.commit()

        _passed = [h for h in passed_hypotheses if isinstance(h, dict)]
        # Project hypotheses to graph for presentation payload
        from app.path_reasoning.persistence import project_hypotheses_to_graph
        projected_graph = project_hypotheses_to_graph(job_id, persisted_graph)

        push_presentation_event(
            job_id=job_id,
            phase="PATHREASONING",
            status=None,
            result={
                "hypothesis_count": len(hypotheses),
                "survived_permanent_filter": len(passed_hypotheses),
                "permanently_rejected": len(failed_hypotheses),
                "hub_suppression_summary": f"Permanently rejected {len(failed_hypotheses)} hypotheses (hub/structural rules). {len(passed_hypotheses)} survived for decision.",
            },
            payload={"graph": projected_graph},
            next_action="decision",
        )
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
            "mode": job.mode,
            "verification_source": None,
            "verification_target": None,
            "verification_result": None,
        }
        # attach any previously computed verification details
        if job.mode == "verification":
            # Read verification entities from VerificationResult table (not job_config)
            vr = session.query(VerificationResult).filter(VerificationResult.job_id == job_id).first()
            if vr:
                job_metadata["verification_source"] = vr.source
                job_metadata["verification_target"] = vr.target
            if job.result and isinstance(job.result, dict):
                job_metadata["verification_result"] = job.result.get("verification_result")

        controller = get_decision_controller()
        decision_result = controller.decide(
            job_id=job_id,
            semantic_graph=persisted_graph,
            hypotheses=hypotheses,
            job_metadata=job_metadata,
        )
        decision_label = decision_result.get("decision_label", "unknown")
        
        with Session(engine) as session:
            job = session.query(Job).get(job_id)
            job.status = "RUNNING_HANDLERS"
            session.commit()
            
        # Placeholder for top-level decision summary (Detailed events come from individual handlers)
        push_presentation_event(
            job_id=job_id,
            phase="DECISION",
            status=None,
            result={
                "message": f"Decision reached: {decision_label}. Executing strategy.",
                "decision": decision_label
            },
            next_action=decision_label,
        )

        handler_execution_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Decision logic failed for job {job_id}: {e}")
        raise


# ============================================================================
# Stage 8: Handler Execution (Previously Stage 9)
# Note: Removed signal evaluation stage - signals are no longer computed
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

        metadata = {
            "id": job.id,
            "status": job.status,
            "mode": job.mode,
            "created_at": job.created_at.isoformat() if job.created_at else None,
            "verification_source": None,
            "verification_target": None,
            "verification_result": None,
        }
        # For verification mode, attach source/target/result so handlers can use them
        if job.mode == "verification":
            vr_row = session.query(VerificationResult).filter(VerificationResult.job_id == job_id).first()
            if vr_row:
                metadata["verification_source"] = vr_row.source
                metadata["verification_target"] = vr_row.target
            if job.result and isinstance(job.result, dict):
                metadata["verification_result"] = job.result.get("verification_result")

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
            
            if status == "FETCH_QUEUED":
                logger.info(f"Handler for job {job_id} requested FETCH_QUEUED; triggering fetch stage.")
                fetch_stage.delay(job_id)

            elif status == "DOWNLOAD_QUEUED":
                logger.info(f"Handler for job {job_id} requested DOWNLOAD_QUEUED; triggering download stage.")
                download_stage.delay(job_id)

            elif status == "NEED_MORE_INPUT":
                logger.info(f"Job {job_id} paused: NEED_MORE_INPUT (Insufficient signal)")

            elif status == "COMPLETED":
                logger.info(f"Job {job_id} reached terminal state: COMPLETED")

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
def fetch_stage(self, job_id: int, wait_for_chord: bool = False):
    """Executes the literature fetch pipeline."""
    from app.graphs.persistence import get_semantic_graph
    from app.path_reasoning.persistence import get_hypotheses
    from app.fetching.service import FetchService
    from app.llm import get_llm_service
    
    with Session(engine) as session:
        job = session.query(Job).get(job_id)
        if not job or job.status != "FETCH_QUEUED":
            return

    try:
        # For initial ignition (RESEARCH_SEED), the graph and hypotheses will be empty.
        # This is expected behavior for the "Universal Fetch" model.
        hypotheses = get_hypotheses(job_id=job_id, limit=10000, offset=0) or []

        with Session(engine) as session:
            from app.fetching.service import get_fetch_service
            fetch_service = get_fetch_service()
            
            # Get job mode and verification entities for fetch service
            job_obj = session.query(Job).get(job_id)
            job_mode = job_obj.mode if job_obj else "discovery"
            verification_entities = None
            
            # For verification mode, get source and target from VerificationResult
            if job_mode == "verification":
                vr = session.query(VerificationResult).filter(VerificationResult.job_id == job_id).first()
                if vr:
                    verification_entities = (vr.source, vr.target)
            
            fetch_service.execute_fetch_stage(
                job_id, 
                hypotheses, 
                session, 
                job_mode=job_mode,
                verification_entities=verification_entities
            )
            
            # CRITICAL: Commit the session to persist all fetched data
            session.commit()

            logger.info("\n\n\n\n---fetch---\n\n\n\n")
        
        with Session(engine) as session:
            # Calculate fetch metrics for presentation
            from app.storage.models import SearchQuery, SearchQueryRun, JobPaperEvidence, IngestionSource, IngestionSourceType
            _fetch_searches = session.query(SearchQueryRun).filter(SearchQueryRun.job_id == job_id).count()
            _fetch_queries = session.query(SearchQuery).filter(SearchQuery.job_id == job_id).count()
            _fetch_papers = session.query(JobPaperEvidence).filter(JobPaperEvidence.job_id == job_id).count()
            _fetch_abstract_only = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id,
                IngestionSource.source_type == IngestionSourceType.PAPER_ABSTRACT.value
            ).count()

            if verify_fetch_sources_ready(job_id, session):
                job = session.query(Job).get(job_id)
                job.status = "READY_TO_INGEST"
                session.commit()
            
                # Fetch structured event
                push_presentation_event(
                    job_id=job_id,
                    phase="FETCH",
                    status=None,
                    result={
                        "searches_run": _fetch_searches,
                        "queries_created": _fetch_queries,
                        "papers_retrieved": _fetch_papers,
                        "abstract_only_count": _fetch_abstract_only,
                    },
                    next_action="ingestion",
                )
                # Chain back to start
                if not wait_for_chord:
                    # Chaining requires status alignment
                    job = session.query(Job).get(job_id)
                    job.status = "READY_TO_INGEST"
                    session.commit()
                    ingest_stage.delay(job_id)
            else:
                push_presentation_event(
                    job_id=job_id,
                    phase="FETCH",
                    status=None,
                    result={
                        "searches_run": _fetch_searches,
                        "queries_created": _fetch_queries,
                        "papers_retrieved": 0,
                        "abstract_only_count": 0,
                    },
                    next_action="decision" if not wait_for_chord else "waiting",
                )
                # No new papers; return to decision stage
                job = session.query(Job).get(job_id)
                if job and not wait_for_chord:
                    job.status = "PATH_REASONING_DONE"
                    session.commit()
                    decision_stage.delay(job_id)
            
            return True # Success for chord
    
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
            
        # Gather download metrics
        from app.storage.models import JobPaperEvidence, Paper
        with Session(engine) as session:
            _paper_evidence = session.query(JobPaperEvidence).filter(
                JobPaperEvidence.job_id == job_id,
                JobPaperEvidence.evaluated == True,
            ).all()
            _impact_scores = [e.impact_score for e in _paper_evidence if e.impact_score is not None]
            _impact_range = f"{min(_impact_scores):.2f}–{max(_impact_scores):.2f}" if _impact_scores else "N/A"

        push_presentation_event(
            job_id=job_id,
            phase="DOWNLOAD",
            status=None,
            result={
                "papers_downloaded": count,
            },
            metric={
                "impact_score_range": _impact_range,
                "papers_evaluated": len(_paper_evidence),
            },
            next_action="ingestion",
        )
        ingest_stage.delay(job_id)
        
    except Exception as e:
        logger.error(f"Download stage failed for job {job_id}: {e}")
        raise
