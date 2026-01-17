"""
CrossRef paper provider with batch size enforcement.

Fetches papers from CrossRef REST API, respecting FETCH_BATCH_SIZE limit.
"""
import logging
from typing import Dict, Any, List, Optional
import requests

from app.fetching.providers.base import PaperProvider, ProviderConfig, PaperProviderError

logger = logging.getLogger(__name__)


class CrossRefProvider(PaperProvider):
    """Paper provider for CrossRef."""
    
    def __init__(self, config: Optional[ProviderConfig] = None):
        super().__init__(config)
        self.name = "crossref"
        self.base_url = "https://api.crossref.org/works"
    
    def fetch(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch papers from CrossRef with strict batch size enforcement.
        
        Internally limits API request to batch_size papers only.
        Never returns more papers than requested batch_size.
        
        Args:
            params: Dict with 'query', optionally 'domain', 'batch_size'
        
        Returns:
            List of papers (len <= batch_size)
        """
        query = params.get("query", "")
        batch_size = self._get_batch_size(params)
        
        if not query:
            logger.warning("CrossRefProvider: no query provided")
            return []
        
        try:
            # CRITICAL: Limit to batch_size in API request itself
            api_params = {
                "query": query,
                "rows": batch_size,  # Enforce batch_size at API level
                "sort": "published",
                "order": "desc"
            }
            
            logger.debug(f"CrossRefProvider requesting with params: {api_params}")
            
            response = requests.get(
                self.base_url,
                params=api_params,
                timeout=self.config.timeout,
                headers={"User-Agent": "MainProject-FETCH-More-Pipeline"}
            )
            response.raise_for_status()
            
            data = response.json()
            papers = []
            
            for item in data.get("message", {}).get("items", []):
                if len(papers) >= batch_size:
                    break  # Safety: never exceed batch_size
                
                # Extract title
                title = None
                if "title" in item:
                    titles = item["title"]
                    title = titles[0] if isinstance(titles, list) else titles
                
                if not title:
                    continue  # Skip papers without title
                
                abstract = item.get("abstract")
                
                # Extract authors
                authors = []
                if "author" in item:
                    for author in item["author"]:
                        author_name = author.get("name") or author.get("literal")
                        if author_name:
                            authors.append({"name": author_name})
                
                # Extract year
                year = None
                if "published-online" in item:
                    date_parts = item["published-online"].get("date-parts", [[None]])[0]
                    year = date_parts[0] if date_parts else None
                
                # Extract venue
                venue = item.get("container-title", "")
                if isinstance(venue, list):
                    venue = venue[0] if venue else ""
                
                # Extract DOI
                doi = item.get("DOI")
                
                paper = {
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "year": year,
                    "venue": venue,
                    "doi": doi,
                    "external_ids": {},
                    "source": "crossref",
                    "pdf_url": None
                }
                
                papers.append(paper)
            
            logger.info(f"CrossRefProvider fetched {len(papers)}/{batch_size} papers for query: {query}")
            return papers
            
        except requests.RequestException as e:
            logger.error(f"CrossRefProvider fetch failed: {e}")
            return []
        except Exception as e:
            logger.error(f"CrossRefProvider unexpected error: {e}")
            return []
