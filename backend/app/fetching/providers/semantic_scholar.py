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
    
    def _wait_for_rate_limit(self):
        """Simple rate limiting: 1 request per second."""
        elapsed = time.time() - self._last_call_time
        if elapsed < 1.0:
            logger.debug(f"SemanticScholarProvider: rate limiting, sleeping {1.0 - elapsed:.2f}s")
            time.sleep(1.0 - elapsed)
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
            headers["x-api-key"] = self.api_key
            
        api_params = {
            "query": query,
            "limit": limit,
            "fields": "title,abstract,authors,year,venue,externalIds,url"
        }
        
        logger.info(f"SemanticScholarProvider: fetching '{query}' (limit={limit})")
        
        try:
            response = requests.get(
                self.base_url,
                params=api_params,
                headers=headers,
                timeout=30
            )
            response.raise_for_status()
            
            data = response.json()
            raw_papers = data.get("data", [])
            
            return [self._normalize(p) for p in raw_papers]
        except Exception as e:
            logger.error(f"SemanticScholarProvider: API call failed: {e}")
            raise

    def _normalize(self, item: Dict[str, Any]) -> Dict[str, Any]:
        """Normalize Semantic Scholar output to standard contract."""
        external_data = item.get("externalIds", {}) or {}
        
        # Flatten external IDs
        external_ids = {}
        if "DOI" in external_data: external_ids["doi"] = external_data["DOI"]
        if "ArXiv" in external_data: external_ids["arxiv_id"] = external_data["ArXiv"]
        if "PubMed" in external_data: external_ids["pubmed_id"] = external_data["PubMed"]
        if "CorpusId" in external_data: external_ids["corpus_id"] = external_data["CorpusId"]

        # Ensure authors list of dicts
        authors = []
        for a in item.get("authors", []) or []:
            name = a.get("name")
            if name:
                authors.append({"name": name})

        return {
            "title": item.get("title", "Untitled").strip(),
            "abstract": item.get("abstract"),
            "authors": authors,
            "year": item.get("year"),
            "venue": item.get("venue"),
            "doi": external_ids.get("doi"),
            "external_ids": external_ids,
            "source": self.name,
            "pdf_url": item.get("url")
        }
