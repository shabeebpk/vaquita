"""
Semantic Scholar paper provider with authentication and rate-limiting.
"""
import logging
import time
from typing import Dict, Any, List, Optional
import requests

from app.fetching.providers.base import BaseFetchProvider

logger = logging.getLogger(__name__)

class SemanticScholarProvider(BaseFetchProvider):
    """
    Semantic Scholar provider with API Key auth and strict rate limiting (1 req/sec).
    """
    
    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        super().__init__(credentials)
        self.api_key = self.credentials.get("api_key")
        self.base_url = self.credentials.get("base_url", "https://api.semanticscholar.org/graph/v1/paper/search")
        self._last_call_time = 0.0
        
        # Load retry/timeout config from admin_policy
        from app.config.admin_policy import admin_policy
        self.max_retries = admin_policy.query_orchestrator.fetch_params.retry_attempts
        self.timeout = admin_policy.query_orchestrator.fetch_params.timeout_seconds

    
    def _wait_for_rate_limit(self):
        """Rate limiting: apply configured wait time between requests."""
        from app.config.admin_policy import admin_policy
        
        wait_time = 2.0  # default
        provider_policy = admin_policy.fetch_apis.providers.get("semantic_scholar")
        if provider_policy:
            wait_time = provider_policy.rate_limit_wait_seconds
        
        elapsed = time.time() - self._last_call_time
        if elapsed < wait_time:
            logger.debug(f"SemanticScholarProvider: rate limiting, sleeping {wait_time - elapsed:.2f}s")
            time.sleep(wait_time - elapsed)
        self._last_call_time = time.time()

    def fetch(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """
        Fetch papers from Semantic Scholar.
        
        Args:
            query: Search string.
            limit: Max results (fetch_batch_size).
        """
        if not query:
            return []
        
        self._wait_for_rate_limit()
        
        headers = {
            "User-Agent": "MainProject-FETCH-More-Pipeline",
            "Accept": "application/json"
        }
        if self.api_key:
            self.api_key = self.api_key.strip()
            # headers["x-api-key"] = self.api_key
            
        api_params = {
            "query": query,
            "limit": limit,
            "fields": "title,abstract,authors,year,venue,externalIds,openAccessPdf",
            "openAccessPdf": ""  # Filter: only return papers with public PDFs
        }
        
        logger.info(f"SemanticScholarProvider: fetching '{query}' (limit={limit})")
        
        try:
            results = self._do_fetch(query, limit, headers, api_params)
            logger.info(f"\n\nSemanticScholarProvider-fetched: {results}")
            return results
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 403 and self.api_key:
                logger.warning("SemanticScholarProvider: API Key rejected (403). Retrying WITHOUT API key (Graceful Degradation).")
                # Remove key and retry
                headers.pop("x-api-key", None)
                try:
                    return self._do_fetch(query, limit, headers, api_params)
                except Exception as retry_e:
                    logger.error(f"SemanticScholarProvider: Fallback fetch (no key) also failed: {retry_e}")
                    raise
            raise
        except Exception as e:
            logger.error(f"SemanticScholarProvider: API call failed: {e}")
            raise

    def _do_fetch(self, query: str, limit: int, headers: dict, params: dict) -> List[Dict[str, Any]]:
        """Internal fetch execution with exponential backoff for rate limits."""
        base_delay = 2.0
        
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    self.base_url,
                    params=params,
                    headers=headers,
                    timeout=self.timeout
                )
                
                if response.status_code == 429:
                    if attempt < self.max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        logger.warning(f"SemanticScholarProvider: Rate limited (429). Retrying in {delay}s (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(delay)
                        continue
                    else:
                        logger.error(f"SemanticScholarProvider: Rate limit exceeded after {self.max_retries} attempts")
                
                if response.status_code != 200:
                    logger.error(f"SemanticScholarProvider failed: {response.status_code} - {response.text}")
                
                response.raise_for_status()
                
                data = response.json()
                raw_papers = data.get("data", [])
                return [self._normalize(p) for p in raw_papers]
                
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429 and attempt < self.max_retries - 1:
                    continue
                raise
        
        return []


    def _normalize(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Semantic Scholar output to standard contract."""
        external_data = item.get("externalIds", {}) or {}
        
        # Flatten external IDs
        authors = []
        for author in item.get("authors", []):
            authors.append({
                "name": author.get("name", "Unknown")
            })
        
        # Extract PDF URL from openAccessPdf field
        pdf_url = None
        open_access_pdf = item.get("openAccessPdf")
        if open_access_pdf and isinstance(open_access_pdf, dict):
            pdf_url = open_access_pdf.get("url")
            # Normalize empty strings to None
            if pdf_url and pdf_url.strip():
                logger.info(f"SemanticScholarProvider: Found PDF URL for '{item.get('title', 'Unknown')[:50]}...'")
            else:
                pdf_url = None
                logger.info(f"SemanticScholarProvider: Empty PDF URL in openAccessPdf for '{item.get('title', 'Unknown')[:50]}...'")
        else:
            logger.info(f"SemanticScholarProvider: No openAccessPdf field for '{item.get('title', 'Unknown')[:50]}...'")
        
        return {
            "title": item.get("title", "Untitled"),
            "abstract": item.get("abstract"),
            "authors": authors,
            "year": item.get("year"),
            "venue": item.get("venue"),
            "doi": external_data.get("DOI"),
            "external_ids": external_data,
            "source": "semantic_scholar",
            "pdf_url": pdf_url  # Direct PDF URL from openAccessPdf
        }
