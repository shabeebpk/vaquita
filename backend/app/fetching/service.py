"""
FetchService: Singleton orchestrator for domain-aware paper fetching.
"""
import logging
import hashlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.storage.models import (
    Job, SearchQuery, SearchQueryRun, Paper, IngestionSource, 
    IngestionSourceType
)
from app.fetching.selection import select_top_diverse_leads
from app.fetching.query_orchestrator import (
    get_or_create_search_query, should_run_query, 
    record_search_run, QueryOrchestratorConfig,
    get_all_fetched_ids_for_job
)
from app.config.admin_policy import admin_policy
from app.config.system_settings import system_settings
from app.fetching.providers import PROVIDER_REGISTRY
from app.fetching.providers.base import BaseFetchProvider
from app.deduplication import check_duplicate, persist_paper
from app.deduplication.fingerprinting import FingerprintConfig

logger = logging.getLogger(__name__)

class FetchServiceError(Exception):
    """Raised when all configured providers fail for a domain."""
    pass

class FetchService:
    """
    Singleton service that manages fetch providers and pipeline orchestration.
    """
    _instance = None
    
    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(FetchService, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self.providers: Dict[str, BaseFetchProvider] = {}
        self.fingerprint_config = FingerprintConfig()
        self._initialize_providers()
        self._initialized = True
        logger.info("FetchService singleton initialized")

    def _initialize_providers(self):
        """Instantiate all active providers from AdminPolicy."""
        # fetch_apis.providers is a dict like {"semantic_scholar": {"active": True}}
        active_config = admin_policy.fetch_apis.providers
        
        for name, policy in active_config.items():
            if not policy.active:
                continue
                
            provider_class = PROVIDER_REGISTRY.get(name)
            if not provider_class:
                logger.warning(f"FetchService: Provider '{name}' requested in policy not found in registry")
                continue
                
            # Get credentials from SystemSettings based on name
            credentials = self._get_credentials_for_provider(name)
            
            try:
                self.providers[name] = provider_class(credentials=credentials)
                logger.info(f"FetchService: Provider '{name}' successfully initialized")
            except Exception as e:
                logger.error(f"FetchService: Failed to initialize provider '{name}': {e}")

    def _get_credentials_for_provider(self, name: str) -> Dict[str, Any]:
        """Map system_settings secrets to provider credentials."""
        if name == "semantic_scholar":
            return {
                "api_key": system_settings.SEMANTIC_SCHOLAR_API_KEY,
                "base_url": system_settings.SEMANTIC_SCHOLAR_URL
            }
        return {}

    def execute_fetch_stage(self, job_id: int, hypotheses: List[Dict[str, Any]], session: Session, job_mode: str = "discovery", verification_entities: tuple = None):
        """
        Main entry point for the Fetch Stage (Universal Lead Orchestration).
        
        Handles both discovery mode (hypothesis-based) and verification mode (entity-based).
        
        Harmonizes:
        1. Hypotheses (Machine Discovered) - for discovery mode
        2. Entity pairs (User Intent via verification mode) - for verification mode
        3. SearchQuery status='new' queries (Vanguard Seeds)
        """
        logger.info(f"FetchService: Starting fetch stage for Job {job_id}, mode={job_mode}")
        
        # 1. Load Thresholds from AdminPolicy (No Hardcoding)
        query_config = QueryOrchestratorConfig()
        
        if job_mode == "verification" and verification_entities:
            return self._execute_verification_fetch(job_id, verification_entities, session, query_config)
        else:
            return self._execute_discovery_fetch(job_id, hypotheses, session, query_config)
    
    def _execute_discovery_fetch(self, job_id: int, hypotheses: List[Dict[str, Any]], session: Session, query_config: QueryOrchestratorConfig):
        """Discovery mode fetch: based on hypotheses."""
        top_k = int(admin_policy.query_orchestrator.top_k_hypotheses)
        batch_size = int(admin_policy.query_orchestrator.fetch_batch_size)
        
        # 2. Harmonize Lead Selection
        # 2a. Machine Leads (Grouped Diversity)
        from app.fetching.selection import select_top_diverse_leads
        machine_leads = select_top_diverse_leads(session, job_id, top_k, hypotheses)
        
        # 2b. Human Leads (Vanguard Queries with status='new')
        from app.storage.models import SearchQuery, Job
        vanguard_leads = session.query(SearchQuery).filter(
            SearchQuery.job_id == job_id,
            SearchQuery.status == "new"
        ).all()
        
        if vanguard_leads:
            logger.info(f"FetchService: Found {len(vanguard_leads)} vanguard seeds to ignite job {job_id}")

        if not machine_leads and not vanguard_leads:
            logger.warning("FetchService: No leads (Machine or Human) to fetch for")
            return

        # 3. Load IDs for job-level dedup
        from app.fetching.query_orchestrator import get_all_fetched_ids_for_job
        seen_ids = set(get_all_fetched_ids_for_job(job_id, session))
        
        # 4. Create Unified Target List
        # Format: (origin_type, SearchQuery)
        all_targets: List[Tuple[str, SearchQuery]] = []
        
        # Vanguard Seeds (Highest Priority)
        for v_query in vanguard_leads:
            all_targets.append(("vanguard", v_query))
            
        # Machine Leads (Map to SearchQuery)
        for m_hypo in machine_leads:
            # Get common focus areas from Job configuration
            job_obj = session.query(Job).get(job_id)
            focus_areas = []
            if job_obj and job_obj.job_config:
                from app.config.job_config import JobConfig
                if isinstance(job_obj.job_config, dict):
                    cfg = JobConfig(**job_obj.job_config)
                    focus_areas = cfg.query_config.focus_areas
            
            from app.fetching.query_orchestrator import get_or_create_search_query
            s_query = get_or_create_search_query(
                m_hypo, job_id, session,
                focus_areas=focus_areas,
                config=query_config,
                entities=m_hypo.get("path")
            )
            all_targets.append(("machine", s_query))

        # Execute the rest of discovery fetch
        self._execute_unified_fetch(job_id, all_targets, batch_size, session, seen_ids, query_config)
    
    def _get_next_verification_entities(self, job_id: int, source: str, target: str, session: Session) -> Optional[List[str]]:
        """
        Determine the next entity combination to try in verification hierarchy.
        Returns the entities to use, or None if all have been tried.
        
        Hierarchy:
        1. [source, target] - both entities
        2. [source] - source alone
        3. [target] - target alone
        """
        from app.fetching.query_orchestrator import compute_entities_hash
        
        # Check [A,B] status
        entities_ab = [source, target]
        hash_ab = compute_entities_hash(entities_ab)
        query_ab = session.query(SearchQuery).filter(
            SearchQuery.job_id == job_id,
            SearchQuery.entities_hash == hash_ab
        ).first()
        
        # If [A,B] doesn't exist or is still 'new', try it
        if not query_ab or query_ab.status == "new":
            return entities_ab
        
        # [A,B] is done, try [A]
        entities_a = [source]
        hash_a = compute_entities_hash(entities_a)
        query_a = session.query(SearchQuery).filter(
            SearchQuery.job_id == job_id,
            SearchQuery.entities_hash == hash_a
        ).first()
        
        if not query_a or query_a.status == "new":
            return entities_a
        
        # [A] is done, try [B]
        entities_b = [target]
        hash_b = compute_entities_hash(entities_b)
        query_b = session.query(SearchQuery).filter(
            SearchQuery.job_id == job_id,
            SearchQuery.entities_hash == hash_b
        ).first()
        
        if not query_b or query_b.status == "new":
            return entities_b
        
        # All done
        return None

    def _execute_verification_fetch(self, job_id: int, verification_entities: tuple, session: Session, query_config: QueryOrchestratorConfig):
        """
        Verification mode fetch: based on user-provided entities.
        
        IMPORTANT: Lazy-creates queries one-by-one in hierarchy order:
        1st cycle: Create and run [source, target]
        2nd cycle: If not found, create and run [source] alone
        3rd cycle: If not found, create and run [target] alone
        
        Only ONE query is created and executed per call, ensuring logical progression.
        """
        batch_size = int(admin_policy.query_orchestrator.verification_batch_size)
        source, target = verification_entities
        
        logger.info(f"FetchService: Verification mode for {source} -> {target}")
        
        # Get job config for domain resolution
        from app.storage.models import Job
        
        job = session.query(Job).get(job_id)
        resolved_domain = None
        if job and job.job_config:
            from app.config.job_config import JobConfig
            if isinstance(job.job_config, dict):
                cfg = JobConfig(**job.job_config)
                resolved_domain = cfg.domain
        
        # Step 1: Determine next entity combination to try
        next_entities = self._get_next_verification_entities(job_id, source, target, session)
        
        if not next_entities:
            logger.info(f"FetchService: All verification queries done for job {job_id}")
            return
        
        # Step 2: Get or create query for these entities
        from app.fetching.query_orchestrator import compute_entities_hash
        entities_hash = compute_entities_hash(next_entities)
        
        search_query = session.query(SearchQuery).filter(
            SearchQuery.job_id == job_id,
            SearchQuery.entities_hash == entities_hash
        ).first()
        
        if not search_query:
            # Create new verification query lazily for these entities
            config_snapshot = {
                "signature_length": query_config.signature_length,
                "initial_reputation": query_config.initial_reputation,
                "timestamp": datetime.utcnow().isoformat()
            }
            
            query_text = (
                f"relationship between {' and '.join(next_entities)}"
                if len(next_entities) > 1
                else f"related to {next_entities[0]}"
            )
            
            search_query = SearchQuery(
                job_id=job_id,
                hypothesis_signature=hashlib.sha256(f"verification_{entities_hash}".encode()).hexdigest()[:16],
                query_text=query_text,
                resolved_domain=resolved_domain,
                status="new",
                reputation_score=query_config.initial_reputation,
                config_snapshot=config_snapshot,
                entities_used=next_entities,
                entities_hash=entities_hash,
            )
            session.add(search_query)
            session.flush()
            logger.info(f"FetchService: Created verification query for entities {next_entities}")
        
        # Step 3: Ensure query is 'new' (if it was 'done' we'd have moved to next level in step 1)
        if search_query.status != "new":
            logger.info(f"FetchService: Query for entities {next_entities} already {search_query.status}, nothing to do")
            return
        
        logger.info(f"FetchService: Executing verification query {search_query.id} with entities {next_entities}")
        
        # Load IDs for job-level dedup
        from app.fetching.query_orchestrator import get_all_fetched_ids_for_job
        seen_ids = set(get_all_fetched_ids_for_job(job_id, session))
        
        # Execute ONLY this one query in this cycle
        all_targets = [("verification", search_query)]
        self._execute_unified_fetch(job_id, all_targets, batch_size, session, seen_ids, query_config)
    
    def _execute_unified_fetch(self, job_id: int, all_targets: List[Tuple[str, SearchQuery]], batch_size: int, session: Session, seen_ids: set, query_config: QueryOrchestratorConfig):
        """
        Execute fetch for all targets (unified for both modes).
        
        IMPORTANT deduplication logic:
        ================================
        LEVEL 1 - GLOBAL DUPLICATE (in Paper table):
            If paper already exists in system (via fingerprint matching):
            - DO NOT insert to Paper table (reuse existing)
            - But still process for job-level inclusion
        
        LEVEL 2 - JOB-LEVEL DUPLICATE (in JobPaperEvidence):
            If paper already linked to this job:
            - DO NOT create JobPaperEvidence entry
            - Skip adding to IngestionSource
        """
        from app.fetching.query_orchestrator import should_run_query, update_search_query_status
        
        # 5. Execute Unified Fetch
        for origin, search_query in all_targets:
            try:
                should_run, reason = should_run_query(search_query, session, config=query_config)
                
                if not should_run:
                    logger.info(f"FetchService: Skipping {origin} lead {search_query.id}: {reason}")
                    continue

                logger.info(f"FetchService: Executing {origin} search for query {search_query.id}")

                # 6. Fetch via domain-aware providers
                # Attempt to fetch; only mark the query done below if this block completes without
                # raising an exception. A network error / 429 / provider failure will be caught and the
                # query left in 'new' state for retries. This mirrors the requirement that status must
                # only change on a successful HTTP 200-style response.
                try:
                    candidates, provider_name = self.fetch_for_hypothesis(search_query, batch_size)
                    papers_found = len(candidates)
                    logger.info(f"FetchService: Fetch returned {papers_found} candidate papers for query {search_query.id}")
                except Exception as fetch_error:
                    logger.warning(f"FetchService: Fetch for {origin} lead {search_query.id} failed: {fetch_error}")
                    # no status update here – query stays 'new' for Celery-level retry
                    continue

                # At this point the provider call succeeded.  Even if zero results were returned,
                # it was still an HTTP 200-like success, so we can mark the query done.
                if not candidates:
                    logger.info(f"FetchService: No papers found for {origin} lead {search_query.id}")
                    update_search_query_status(search_query, session)  # mark done only after successful fetch
                    
                    # Log an empty run so the system knows an attempt was made
                    record_search_run(
                        search_query=search_query,
                        job_id=job_id,
                        provider_used=provider_name,
                        reason=reason,
                        session=session,
                        config=query_config
                    )
                    session.commit()
                    continue

                # ===== LEVEL 1 DEDUPLICATION: GLOBAL (Paper table) =====
                # 7. Global Deduplication and Persistence
                all_found_papers = self._deduplicate_and_persist(candidates, session)
                logger.info(f"FetchService: After global dedup, {len(all_found_papers)} papers to process for job {job_id}")
                
                # ===== LEVEL 2 DEDUPLICATION: JOB-LEVEL (JobPaperEvidence + IngestionSource) =====
                # 8. Job-level Deduplication - only add to job if not already there
                job_new_papers = []
                for paper in all_found_papers:
                    if paper.id not in seen_ids:
                        job_new_papers.append(paper)
                        seen_ids.add(paper.id)
                    else:
                        logger.debug(f"FetchService: Paper {paper.id} already in job {job_id}, skipping job-level entry")

                if not job_new_papers:
                    logger.info(f"FetchService: All papers from {origin} query already in job {job_id}")
                else:
                    logger.info(f"FetchService: Adding {len(job_new_papers)} new papers to job {job_id} (from {len(all_found_papers)} candidates)")

                # 9. Record SearchQueryRun (Log behavior)  
                search_run = record_search_run(
                    search_query=search_query,
                    job_id=job_id,
                    provider_used=provider_name,
                    reason=reason,
                    session=session,
                    config=query_config
                )
                
                # 10. Update JobPaperEvidence (Strategic Ledger) - ONLY for job-new papers
                if job_new_papers:
                    from app.storage.models import JobPaperEvidence
                    for paper in job_new_papers:
                        new_evidence = JobPaperEvidence(
                            job_id=job_id,
                            run_id=search_run.id,
                            paper_id=paper.id,
                            evaluated=False,
                            impact_score=0.0,
                            hypo_ref_count=0,
                            cumulative_conf=0.0,
                            entity_density=0
                        )
                        session.add(new_evidence)
                    
                    # 11. Create IngestionSources - ONLY for job-new papers
                    self._create_ingestion_sources(job_id, job_new_papers, session)

                # 12. Update query status to 'done' after successful execution
                update_search_query_status(search_query, session)
                session.commit()
                logger.info(f"FetchService: Query {search_query.id} updated - found {papers_found} papers, new to job: {len(job_new_papers)}")

            except Exception as e:
                logger.error(f"FetchService: Error processing {origin} lead {search_query.id}: {e}", exc_info=True)
                session.rollback()

    def fetch_for_hypothesis(self, search_query: SearchQuery, limit: int) -> Tuple[List[Dict[str, Any]], str]:
        """Perform domain-aware provider routing."""
        domain = search_query.resolved_domain or "default"
        
        provider_order = admin_policy.fetch_apis.domain_provider_order.get(domain)
        if not provider_order:
            logger.warning(f"FetchService: No provider order for domain '{domain}', falling back to default")
            provider_order = admin_policy.fetch_apis.domain_provider_order.get("default", [])

        errors = []
        for name in provider_order:
            provider = self.providers.get(name)
            if not provider:
                continue
                
            try:
                results = provider.fetch(search_query.query_text, limit)
                # Provider succeeded, return immediately
                return results, name
            except Exception as e:
                logger.error(f"FetchService: Provider '{name}' failed: {e}")
                errors.append(f"{name}: {str(e)}")

        if errors:
            if not any(self.providers.get(n) for n in provider_order):
                raise FetchServiceError(f"No active providers available for domain '{domain}'")
            raise FetchServiceError(f"All providers failed for domain '{domain}': {'; '.join(errors)}")

        return [], "none"

    def _deduplicate_and_persist(self, candidates: List[Dict[str, Any]], session: Session) -> List[Paper]:
        """
        Deduplicates against global DB and returns Paper objects for all valid candidates.
        """
        all_papers = []
        
        for candidate in candidates:
            # Check for duplicate via standard framework
            dup_result = check_duplicate(candidate, session, self.fingerprint_config)
            
            if dup_result.is_duplicate:
                if dup_result.matched_paper_id is not None:
                    # Globally known paper - retrieve existing object
                    paper = session.query(Paper).get(dup_result.matched_paper_id)
                    if paper:
                        all_papers.append(paper)
            else:
                try:
                    # Globally new paper - persist it
                    paper = persist_paper(candidate, session, self.fingerprint_config)
                    all_papers.append(paper)
                except Exception as e:
                    logger.error(f"FetchService: Failed to persist paper: {e}")
        
        return all_papers

    def _create_ingestion_sources(self, job_id: int, papers: List[Paper], session: Session):
        """Create IngestionSource entries for new papers."""
        logger.info(f"FetchService: Attempting to create ingestion sources for {len(papers)} papers")
        created = 0
        skipped = 0
        for paper in papers:
            if not paper.abstract:
                logger.warning(f"FetchService: Paper {paper.id} has no abstract, skipping")
                skipped += 1
                continue
                
            # Check if source already exists for this job
            source_ref = f"paper:{paper.id}"
            existing = session.query(IngestionSource).filter(
                IngestionSource.job_id == job_id,
                IngestionSource.source_ref == source_ref
            ).first()
            
            if not existing:
                source = IngestionSource(
                    job_id=job_id,
                    source_type=IngestionSourceType.PAPER_ABSTRACT,
                    source_ref=source_ref,
                    raw_text=paper.abstract,
                    processed=False
                )
                session.add(source)
                created += 1
        session.flush()
        logger.info(f"FetchService: Created {created} ingestion sources, skipped {skipped} (no abstract)")

def get_fetch_service() -> FetchService:
    """Helper to get singleton instance."""
    return FetchService()
