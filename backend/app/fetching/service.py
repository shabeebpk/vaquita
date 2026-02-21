"""
FetchService: Singleton orchestrator for domain-aware paper fetching.
"""
import logging
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

    def execute_fetch_stage(self, job_id: int, hypotheses: List[Dict[str, Any]], session: Session):
        """
        Main entry point for the Fetch Stage (Refactored Pipeline).
        """
        logger.info(f"FetchService: Starting fetch stage for Job {job_id}")
        
        # 1. Load Configs
        top_k = int(admin_policy.query_orchestrator.top_k_hypotheses)
        batch_size = int(admin_policy.query_orchestrator.fetch_batch_size)
        query_config = QueryOrchestratorConfig()
        
        # Load job config for focus_areas
        job = session.query(Job).get(job_id)
        job_config = None
        if job and job.job_config:
            from app.config.job_config import JobConfig
            job_config = JobConfig(**job.job_config) if isinstance(job.job_config, dict) else job.job_config
        
        focus_areas = job_config.query_config.focus_areas if job_config else []
        
        # 2. Select top K passed hypotheses
        # Use refined diversity algorithm (Grouped by Source-Target, Tiered Priority)
        targets = select_top_diverse_leads(session, job_id, top_k, hypotheses)
        
        if not targets:
            logger.warning("FetchService: No hypotheses (Passed or Promising) to fetch for")
            return

        # Load IDs for job-level dedup
        seen_ids = set(get_all_fetched_ids_for_job(job_id, session))

        for hypo in targets:
            try:
                # 3. Get/Create SearchQuery with focus_areas
                search_query = get_or_create_search_query(
                    hypo, job_id, session, 
                    focus_areas=focus_areas, 
                    config=query_config
                )
                should_run, reason = should_run_query(search_query, session, config=query_config)
                
                if not should_run:
                    logger.info(f"FetchService: Skipping query {search_query.id}: {reason}")
                    continue

                # 4. Fetch via domain-aware providers
                candidates, provider_name = self.fetch_for_hypothesis(search_query, batch_size)
                
                if not candidates:
                    logger.info(f"FetchService: No papers found for query {search_query.id}")
                    continue

                # 5. Global Deduplication and Persistence
                all_found_papers = self._deduplicate_and_persist(candidates, session)
                
                # 6. Job-level Deduplication
                # We identify papers that are new TO THIS JOB.
                # These might be newly persisted Papers OR existing ones from global DB.
                job_new_papers = []
                for paper in all_found_papers:
                    if paper.id not in seen_ids:
                        job_new_papers.append(paper)
                        seen_ids.add(paper.id)

                # 7. Record SearchQueryRun (Log behavior)
                search_run = record_search_run(
                    search_query=search_query,
                    job_id=job_id,
                    provider_used=provider_name,
                    reason=reason,
                    session=session,
                    config=query_config
                )
                
                # 8. Update JobPaperEvidence (Strategic Ledger)
                # We only add papers that are NEW to this job
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

                # 9. Create IngestionSources
                # We must ingest papers that are new to this job so their abstracts/PDFs are processed
                self._create_ingestion_sources(job_id, job_new_papers, session)

            except Exception as e:
                logger.error(f"FetchService: Error processing hypothesis {hypo.get('id')}: {e}", exc_info=True)

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
                if results:
                    return results, name
                logger.debug(f"FetchService: Provider '{name}' returned no results for query '{search_query.id}'")
            except Exception as e:
                logger.error(f"FetchService: Provider '{name}' failed: {e}")
                errors.append(f"{name}: {str(e)}")

        if errors:
            # If they all failed (raised exception), but returned zero or we caught them:
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
