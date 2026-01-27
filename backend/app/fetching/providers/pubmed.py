"""
PubMed paper provider with batch size enforcement.

Fetches papers from PubMed E-utilities API, respecting FETCH_BATCH_SIZE limit.
"""
import logging
from typing import Dict, Any, List, Optional
import requests

from app.fetching.providers.base import PaperProvider, ProviderConfig, PaperProviderError

logger = logging.getLogger(__name__)


class PubMedProvider(PaperProvider):
    """Paper provider for PubMed."""
    
    def __init__(self, config: Optional[ProviderConfig] = None):
        super().__init__(config)
        self.name = "pubmed"
        self.base_url = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils"
    
    def fetch(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch papers from PubMed with strict batch size enforcement.
        
        Uses two-step fetch:
        1. Search for PMIDs with batch_size limit
        2. Fetch metadata for those PMIDs
        
        Never returns more papers than requested batch_size.
        
        Args:
            params: Dict with 'query', optionally 'domain', 'batch_size'
        
        Returns:
            List of papers (len <= batch_size)
        """
        query = params.get("query", "")
        batch_size = self._get_batch_size(params)
        
        if not query:
            logger.warning("PubMedProvider: no query provided")
            return []
        
        try:
            # Step 1: Search for PMIDs with batch_size limit enforced at API level
            search_url = f"{self.base_url}/esearch.json"
            search_params = {
                "db": "pubmed",
                "term": query,
                "retmax": batch_size,  # CRITICAL: Enforce batch_size at API level
                "sort": "date"
            }
            
            logger.debug(f"PubMedProvider searching with params: {search_params}")
            
            response = requests.get(search_url, params=search_params, timeout=self.config.timeout)
            response.raise_for_status()
            
            search_data = response.json()
            pmids = search_data.get("esearchresult", {}).get("idlist", [])
            
            if not pmids:
                logger.info(f"PubMedProvider found 0 PMIDs for query: {query}")
                return []
            
            # Ensure we don't exceed batch_size
            pmids = pmids[:batch_size]
            
            # Step 2: Fetch metadata for these PMIDs
            fetch_url = f"{self.base_url}/efetch.json"
            fetch_params = {
                "db": "pubmed",
                "id": ",".join(pmids),
                "rettype": "json"
            }
            
            response = requests.get(fetch_url, params=fetch_params, timeout=self.config.timeout)
            response.raise_for_status()
            
            fetch_data = response.json()
            papers = []
            
            for article_uid in fetch_data.get("result", {}).get("uids", []):
                if len(papers) >= batch_size:
                    break  # Safety: never exceed batch_size
                
                if article_uid == "uids":  # Skip the uids array itself
                    continue
                
                article_data = fetch_data["result"][article_uid]
                
                # Extract title
                title = article_data.get("title", "")
                if not title:
                    continue  # Skip papers without title
                
                abstract = article_data.get("abstract", "")
                
                # Extract authors
                authors = []
                for author in article_data.get("authors", []):
                    author_name = author.get("name")
                    if author_name:
                        authors.append({"name": author_name})
                
                # Extract year
                year = None
                pubdate = article_data.get("pubdate", "")
                if pubdate:
                    try:
                        year = int(pubdate.split()[0])
                    except (IndexError, ValueError):
                        pass
                
                # Extract venue
                venue = article_data.get("source", "")
                
                paper = {
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "year": year,
                    "venue": venue,
                    "doi": article_data.get("uid"),
                    "external_ids": {"pubmed_id": article_uid},
                    "source": "pubmed",
                    "pdf_url": None
                }
                
                papers.append(paper)
            
            logger.info(f"PubMedProvider fetched {len(papers)}/{batch_size} papers for query: {query}")
            return papers
            
        except requests.RequestException as e:
            logger.error(f"PubMedProvider fetch failed: {e}")
            return []
        except Exception as e:
            logger.error(f"PubMedProvider unexpected error: {e}")
            return []
