"""
Base provider contract for paper fetching.
"""
import logging
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)

class BaseFetchProvider(ABC):
    """
    Abstract base class for paper providers.
    
    All providers MUST:
    1. Accept credentials dict in constructor
    2. Implement fetch(query, limit) method
    3. Return list of standardized paper dicts
    4. Raise exceptions on failure
    """
    
    def __init__(self, credentials: Optional[Dict[str, Any]] = None):
        """
        Initialize provider with credentials (API keys, etc.) 
        from FetchService.
        """
        self.credentials = credentials or {}
        self.name = self.__class__.__name__.replace("Provider", "").lower()

    @abstractmethod
    def fetch(self, query: str, limit: int) -> List[Dict[str, Any]]:
        """
        Fetch papers from provider.
        
        Args:
            query: Search query string.
            limit: Max papers to fetch (batch size).
            
        Returns:
            List of standardized paper dicts:
            {
              "title": str,
              "abstract": str or None,
              "authors": [{"name": str}, ...],
              "year": int or None,
              "venue": str or None,
              "doi": str or None,
              "external_ids": {id_type: id_value, ...},
              "source": str,
              "pdf_url": str or None
            }
            
        Raises:
            Exception: On API or network failure.
        """
        pass
