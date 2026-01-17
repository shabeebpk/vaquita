"""
FetchService: execution-only FETCH_MORE orchestrator.

Orchestrates execution of the fetch pipeline:
1. Select top-K passed hypotheses by reputation
2. For each: fetch via domain-aware provider, deduplicate, persist
3. Create IngestionSources and record SearchQueryRun

Caller (worker) is responsible for:
- Ingestion and graph rebuild
- Path reasoning and decision reruns
- Signal evaluation AFTER decision re-runs

Batch size enforcement:
- All providers respect FETCH_BATCH_SIZE limit at API call level
- FetchService assumes providers already enforce batch size
- FetchService never trims or limits provider results
"""
import logging
import os
from typing import Dict, Any, List, Optional, Tuple
from sqlalchemy.orm import Session

from app.storage.models import (
    Job, SearchQuery, SearchQueryRun, Paper, IngestionSource, 
    IngestionSourceType, Hypothesis
)
from app.fetching.query_orchestrator import (
    compute_hypothesis_signature, get_or_create_search_query, 
    should_run_query, record_search_run, QueryOrchestratorConfig
)
from app.fetching.providers import (
    select_provider_for_domain, ProviderConfig, get_provider
)
from app.deduplication import (
    check_duplicate, persist_paper
)
from app.deduplication.fingerprinting import FingerprintConfig

logger = logging.getLogger(__name__)


class FetchServiceConfig:
    """Configuration for fetch service."""
    
    def __init__(self):
        # Max hypotheses to fetch for in one FETCH_MORE cycle
        self.top_k_hypotheses = int(os.getenv("FETCH_TOP_K_HYPOTHESES", "5"))
        
        # Min reputation to consider hypothesis for fetch
        self.min_reputation_for_fetch = int(os.getenv("FETCH_MIN_REPUTATION", "-10"))
        
        logger.info(
            f"FetchServiceConfig: top_k={self.top_k_hypotheses}, "
            f"min_reputation={self.min_reputation_for_fetch}"
        )


class FetchService:
    """Main FETCH_MORE orchestrator service."""
    
    def __init__(self, llm_client: Optional[Any] = None):
        self.llm_client = llm_client
        self.fetch_config = FetchServiceConfig()
        self.query_config = QueryOrchestratorConfig()
        self.provider_config = ProviderConfig()
        self.fingerprint_config = FingerprintConfig()
        
        logger.info("FetchService initialized")
    
    def select_top_hypotheses(
        self,
        hypotheses: List[Dict[str, Any]],
        session: Session
    ) -> List[Tuple[Dict[str, Any], SearchQuery]]:
        """
        Select top-K passed hypotheses for fetching.
        Orders by reputation of existing SearchQuery or new status.
        
        Args:
            hypotheses: List of hypothesis dicts (passed filter)
            session: SQLAlchemy session
        
        Returns:
            List of (hypothesis, SearchQuery) tuples
        """
        job_id = hypotheses[0].get("job_id") if hypotheses else None
        
        candidates = []
        for hyp in hypotheses:
            if not hyp.get("passed_filter", False):
                continue
            
            sig = compute_hypothesis_signature(hyp)
            search_query = get_or_create_search_query(
                hyp, job_id, session, llm_client=self.llm_client
            )
            
            reputation = search_query.reputation_score
            candidates.append((hyp, search_query, reputation))
        
        # Sort by reputation (descending), then by status (new > reusable)
        candidates.sort(
            key=lambda x: (
                -x[2],  # Reputation descending
                0 if x[1].status == "new" else 1  # New first
            )
        )
        
        # Take top-K
        selected = candidates[:self.fetch_config.top_k_hypotheses]
        logger.info(f"Selected {len(selected)} hypotheses for fetching")
        
        return [(h, q) for h, q, _ in selected]
    
    def fetch_papers_for_hypothesis(
        self,
        hypothesis: Dict[str, Any],
        search_query: SearchQuery,
        job_id: int,
        session: Session
    ) -> Tuple[List[Dict[str, Any]], str]:
        """
        Fetch papers for one hypothesis using appropriate provider.
        
        Args:
            hypothesis: Hypothesis dict
            search_query: SearchQuery instance
            job_id: Job ID
            session: SQLAlchemy session
        
        Returns:
            Tuple of (paper_candidates, provider_used)
        """
        # Determine reason for running query
        should_run, reason = should_run_query(search_query, session, self.query_config)
        if not should_run:
            logger.info(f"Skipping SearchQuery {search_query.id}: {reason}")
            return [], "none"
        
        # Select provider
        provider = select_provider_for_domain(
            search_query.resolved_domain,
            self.provider_config
        )
        
        if not provider:
            logger.warning(f"No provider available for domain {search_query.resolved_domain}")
            return [], "none"
        
        # Fetch papers with batch_size passed to provider
        # Provider will enforce batch_size at API call level
        params = {
            "query": search_query.query_text,
            "domain": search_query.resolved_domain,
            "batch_size": self.provider_config.batch_size
        }
        
        try:
            candidates = provider.fetch(params)
            logger.info(
                f"Fetched {len(candidates)} paper candidates (max {self.provider_config.batch_size}) "
                f"from {provider.name} for SearchQuery {search_query.id}"
            )
            return candidates, provider.name
        except Exception as e:
            logger.error(f"Fetch failed: {e}")
            return [], provider.name
    
    def deduplicate_and_persist(
        self,
        candidates: List[Dict[str, Any]],
        job_id: int,
        session: Session
    ) -> Tuple[List[Paper], int, int]:
        """
        Deduplicate candidates and persist accepted papers.
        
        Args:
            candidates: Paper candidate dicts from provider
            job_id: Job ID
            session: SQLAlchemy session
        
        Returns:
            Tuple of (persisted_papers, accepted_count, rejected_count)
        """
        persisted = []
        accepted_count = 0
        rejected_count = 0
        
        for candidate in candidates:
            # Check for duplicate
            dup_result = check_duplicate(candidate, session, self.fingerprint_config)
            
            if dup_result.is_duplicate:
                logger.debug(
                    f"Rejecting duplicate: {candidate.get('title')} "
                    f"({dup_result.match_type})"
                )
                rejected_count += 1
            else:
                # Persist paper
                try:
                    paper = persist_paper(candidate, session, self.fingerprint_config)
                    persisted.append(paper)
                    accepted_count += 1
                except Exception as e:
                    logger.error(f"Failed to persist paper: {e}")
                    rejected_count += 1
        
        logger.info(
            f"Deduplication result: {accepted_count} accepted, {rejected_count} rejected"
        )
        
        return persisted, accepted_count, rejected_count
    
    def ingest_abstracts(
        self,
        papers: List[Paper],
        job_id: int,
        search_query_run: SearchQueryRun,
        session: Session
    ) -> List[IngestionSource]:
        """
        Create IngestionSources for paper abstracts.
        
        Args:
            papers: List of Paper instances
            job_id: Job ID
            search_query_run: SearchQueryRun instance (for source_ref)
            session: SQLAlchemy session
        
        Returns:
            List of IngestionSource instances
        """
        ingestion_sources = []
        
        for paper in papers:
            if not paper.abstract:
                logger.debug(f"Skipping paper {paper.id} (no abstract)")
                continue
            
            # Create IngestionSource for abstract
            source = IngestionSource(
                job_id=job_id,
                source_type=IngestionSourceType.PAPER_ABSTRACT,
                source_ref=f"paper:{paper.id}",
                raw_text=paper.abstract,
                processed=False
            )
            
            session.add(source)
            ingestion_sources.append(source)
        
        session.flush()
        logger.info(f"Created {len(ingestion_sources)} IngestionSources from papers")
        
        return ingestion_sources
    
    def execute_fetch_more(
        self,
        job_id: int,
        hypotheses: List[Dict[str, Any]],
        session: Session
    ) -> Dict[str, Any]:
        """
        Execute FETCH_MORE pipeline: PHASE 1 SOURCE CREATION ONLY.
        
        STRICT TWO-PHASE MODEL:
        This method ONLY creates IngestionSource rows. It NEVER calls IngestionService.
        Phase 1 responsibility: Source discovery and creation
        Phase 2 responsibility: Ingestion processing (handled by process_job_stage + IngestionService)
        
        Steps (Phase 1 only):
        1. Select top-K hypotheses
        2. For each: fetch papers, deduplicate
        3. Create IngestionSource rows with source_type=PAPER_ABSTRACT, processed=false
        4. Return list of IngestionSource IDs created
        
        Orchestrator (process_job_stage) is responsible for:
        - Verifying IngestionSource rows exist (verify_fetch_sources_ready)
        - Transitioning job status to READY_TO_INGEST
        - Next cycle: calling IngestionService.ingest_job()
        - Rebuilding semantic graph
        - Rerunning path reasoning
        - Rerunning decision layer
        
        Args:
            job_id: Job ID
            hypotheses: List of hypothesis dicts
            session: SQLAlchemy session
        
        Returns:
            Dict with execution summary:
            {
              "queries_executed": int,
              "papers_fetched": int,
              "papers_accepted": int,
              "papers_rejected": int,
              "search_query_runs": [SearchQueryRun ids],
              "ingestion_sources": [IngestionSource ids] <- only source creation, NOT ingestion
            }
        """
        logger.info(f"Starting FETCH_MORE for job {job_id}")
        
        # Select top hypotheses
        selected = self.select_top_hypotheses(hypotheses, session)
        if not selected:
            logger.info("No hypotheses selected for fetching")
            return {
                "queries_executed": 0,
                "papers_fetched": 0,
                "papers_accepted": 0,
                "papers_rejected": 0,
                "search_query_runs": [],
                "ingestion_sources": []
            }
        
        total_fetched = 0
        total_accepted = 0
        total_rejected = 0
        all_runs = []
        all_sources = []
        
        for hypothesis, search_query in selected:
            logger.debug(f"Processing SearchQuery {search_query.id}")
            
            # Fetch papers
            candidates, provider = self.fetch_papers_for_hypothesis(
                hypothesis, search_query, job_id, session
            )
            
            if not candidates:
                logger.debug(f"No candidates fetched for SearchQuery {search_query.id}")
                continue
            
            # Deduplicate and persist
            papers, accepted, rejected = self.deduplicate_and_persist(
                candidates, job_id, session
            )
            
            total_fetched += len(candidates)
            total_accepted += accepted
            total_rejected += rejected
            
            # Ingest abstracts
            if papers:
                sources = self.ingest_abstracts(papers, job_id, None, session)
                all_sources.extend(sources)
            
            # Record SearchQueryRun (without signal_delta; set by worker post-decision)
            reason = "initial_attempt" if search_query.status == "new" else "reuse"
            run = record_search_run(
                search_query, job_id, provider, reason,
                len(candidates), accepted, rejected, session
            )
            all_runs.append(run.id)
        
        # Commit all changes
        session.commit()
        logger.info(
            f"FETCH_MORE completed: fetched={total_fetched}, "
            f"accepted={total_accepted}, rejected={total_rejected}"
        )
        
        return {
            "queries_executed": len(selected),
            "papers_fetched": total_fetched,
            "papers_accepted": total_accepted,
            "papers_rejected": total_rejected,
            "search_query_runs": all_runs,
            "ingestion_sources": [s.id for s in all_sources]
        }
