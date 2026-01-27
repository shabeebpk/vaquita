"""
ArXiv paper provider with batch size enforcement.

Fetches papers from arXiv REST API, respecting FETCH_BATCH_SIZE limit.
"""
import logging
from typing import Dict, Any, List, Optional
import requests
import xml.etree.ElementTree as ET

from app.fetching.providers.base import PaperProvider, ProviderConfig, PaperProviderError

logger = logging.getLogger(__name__)


class ArxivProvider(PaperProvider):
    """Paper provider for arXiv."""
    
    def __init__(self, config: Optional[ProviderConfig] = None):
        super().__init__(config)
        self.name = "arxiv"
        self.base_url = "http://export.arxiv.org/api/query"
    
    def fetch(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch papers from arXiv with strict batch size enforcement.
        
        Internally limits API request to batch_size papers only.
        Never returns more papers than requested batch_size.
        
        Args:
            params: Dict with 'query', optionally 'domain', 'batch_size'
        
        Returns:
            List of papers (len <= batch_size)
        """
        query = params.get("query", "")
        domain = params.get("domain")
        batch_size = self._get_batch_size(params)
        
        if not query:
            logger.warning("ArxivProvider: no query provided")
            return []
        
        # Build arXiv query with batch size enforced at API level
        search_query = f"search_query=all:{query}"
        if domain:
            search_query += f" AND cat:{domain}"
        
        # CRITICAL: Limit to batch_size in API request itself
        search_query += f"&start=0&max_results={batch_size}&sortBy=submittedDate"
        
        try:
            url = f"{self.base_url}?{search_query}"
            logger.debug(f"ArxivProvider requesting URL: {url}")
            
            response = requests.get(url, timeout=self.config.timeout)
            response.raise_for_status()
            
            papers = []
            root = ET.fromstring(response.content)
            
            # arXiv uses Atom namespace
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            
            # Try to find entries with namespace first
            entries = root.findall("atom:entry", ns)
            if not entries:
                # Fallback: try without namespace
                entries = root.findall("entry")
            
            for entry in entries:
                if len(papers) >= batch_size:
                    break  # Safety: never exceed batch_size
                
                # Handle both namespaced and non-namespaced elements
                title_elem = entry.find("atom:title", ns)
                if title_elem is None:
                    title_elem = entry.find("title")
                title = (title_elem.text or "").strip() if title_elem is not None else ""
                
                abstract_elem = entry.find("atom:summary", ns)
                if abstract_elem is None:
                    abstract_elem = entry.find("summary")
                abstract = (abstract_elem.text or "").strip() if abstract_elem is not None else ""
                
                authors = []
                author_elems = entry.findall("atom:author", ns)
                if not author_elems:
                    author_elems = entry.findall("author")
                for author in author_elems:
                    name_elem = author.find("atom:name", ns)
                    if name_elem is None:
                        name_elem = author.find("name")
                    author_name = name_elem.text if name_elem is not None else ""
                    if author_name:
                        authors.append({"name": author_name})
                
                # arXiv ID from URL
                id_elem = entry.find("atom:id", ns)
                if id_elem is None:
                    id_elem = entry.find("id")
                arxiv_id = (id_elem.text or "").split("/abs/")[-1] if id_elem is not None else ""
                
                # Published date
                pub_elem = entry.find("atom:published", ns)
                if pub_elem is None:
                    pub_elem = entry.find("published")
                published = pub_elem.text if pub_elem is not None else ""
                year = int(published[:4]) if published else None
                
                paper = {
                    "title": title,
                    "abstract": abstract,
                    "authors": authors,
                    "year": year,
                    "venue": "arXiv",
                    "doi": None,
                    "external_ids": {"arxiv_id": arxiv_id},
                    "source": "arxiv",
                    "pdf_url": f"https://arxiv.org/pdf/{arxiv_id}.pdf"
                }
                papers.append(paper)
            
            logger.info(f"ArxivProvider fetched {len(papers)}/{batch_size} papers for query: {query}")
            return papers
            
        except requests.RequestException as e:
            logger.error(f"ArxivProvider fetch failed: {e}")
            return []
        except ET.ParseError as e:
            logger.error(f"ArxivProvider XML parse error: {e}")
            return []
        except Exception as e:
            logger.error(f"ArxivProvider unexpected error: {e}")
            return []
