"""
Semantic Scholar paper provider with batch size enforcement.

Simple reference implementation demonstrating batch-limited fetching.
Fetches papers from Semantic Scholar API, respecting FETCH_BATCH_SIZE limit.

No pagination, no retries, no extra enrichment—just basic batch-limited fetching.
"""
import logging
from typing import Dict, Any, List, Optional
import requests

from app.fetching.providers.base import PaperProvider, ProviderConfig, PaperProviderError

logger = logging.getLogger(__name__)


class SemanticScholarProvider(PaperProvider):
    """
    Simple reference implementation: Semantic Scholar provider.
    
    Demonstrates correct batch-limited fetching:
    - Request exactly batch_size results from API
    - Return only basic metadata fields
    - No pagination, no retries, no extra logic
    """
    
    def __init__(self, config: Optional[ProviderConfig] = None):
        super().__init__(config)
        self.name = "semantic_scholar"
        self.base_url = "https://api.semanticscholar.org/graph/v1/paper/search"
    
    def fetch(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch papers from Semantic Scholar with strict batch size enforcement.
        
        Simple reference implementation:
        - Single API call with batch_size limit
        - Return only basic metadata (title, abstract, authors, year, venue)
        - No pagination, no retries, no enrichment
        
        Args:
            params: Dict with 'query', optionally 'domain', 'batch_size'
        
        Returns:
            List of papers (len <= batch_size)
        """
        query = params.get("query", "")
        batch_size = self._get_batch_size(params)
        
        if not query:
            logger.warning("SemanticScholarProvider: no query provided")
            return []
        
        try:
            # CRITICAL: Request exactly batch_size results from API
            api_params = {
                "query": query,
                "limit": batch_size,  # Enforce batch_size at API level
                "fields": "title,abstract,authors,year,venue,externalIds,url"
            }
            
            logger.debug(f"SemanticScholarProvider requesting with batch_size={batch_size}")
            
            response = requests.get(
                self.base_url,
                params=api_params,
                timeout=self.config.timeout,
                headers={
                    "User-Agent": "MainProject-FETCH-More-Pipeline",
                    "Accept": "application/json"
                }
            )
            response.raise_for_status()
            
            data = response.json()
            papers = []
            
            for item in data.get("data", []):
                if len(papers) >= batch_size:
                    break  # Safety: never exceed batch_size
                
                # Extract title
                title = item.get("title", "").strip()
                if not title:
                    continue  # Skip papers without title
                
                # Extract abstract
                abstract = item.get("abstract", "")
                
                # Extract authors
                authors = []
                for author in item.get("authors", []):
                    author_name = author.get("name")
                    if author_name:
                        authors.append({"name": author_name})
                
                # Extract year
                year = item.get("year")
                
                # Extract venue
                venue = item.get("venue", "")
                
                # Extract external IDs
                external_ids = {}
                external_data = item.get("externalIds", {})
                if external_data:
                    if "DOI" in external_data:
                        external_ids["doi"] = external_data["DOI"]
                    if "ArXiv" in external_data:
                        external_ids["arxiv_id"] = external_data["ArXiv"]
                    if "PubMed" in external_data:
                        external_ids["pubmed_id"] = external_data["PubMed"]
                
                # Get Semantic Scholar ID
                doi = external_ids.get("doi")
                
                paper = {
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "year": year,
                    "venue": venue,
                    "doi": doi,
                    "external_ids": external_ids,
                    "source": "semantic_scholar",
                    "pdf_url": item.get("url")
                }
                
                papers.append(paper)
            
            logger.info(f"SemanticScholarProvider fetched {len(papers)}/{batch_size} papers for query: {query}")
            return papers
            
        except requests.RequestException as e:
            logger.error(f"SemanticScholarProvider fetch failed: {e}")
            return []
        except Exception as e:
            logger.error(f"SemanticScholarProvider unexpected error: {e}")
            return []
