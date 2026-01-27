"""
Base provider contract for paper fetching.

All providers must implement this contract:
- Accept parameters dict with 'query', optionally 'domain' and 'batch_size'
- Respect batch_size: fetch at most FETCH_BATCH_SIZE papers
- Return list of normalized paper dicts with standard fields

Providers must enforce batch size internally (at API call level),
never returning more papers than requested batch_size.
"""
import logging
import os
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod

logger = logging.getLogger(__name__)


class ProviderConfig:
    """Global configuration for all providers."""
    
    def __init__(self):
        # Batch size: max papers per provider call for one SearchQuery
        # From FETCH_BATCH_SIZE env var
        self.batch_size = int(os.getenv("FETCH_BATCH_SIZE", "3"))
        
        # List of enabled providers (comma-separated)
        providers_str = os.getenv("FETCH_PROVIDERS", "arxiv,crossref")
        self.enabled_providers = [p.strip() for p in providers_str.split(",")]
        
        # Provider timeout in seconds
        self.timeout = float(os.getenv("FETCH_TIMEOUT_SECONDS", "30"))
        
        # Retry configuration
        self.retry_attempts = int(os.getenv("FETCH_RETRY_ATTEMPTS", "3"))
        
        logger.info(
            f"ProviderConfig: batch_size={self.batch_size}, "
            f"enabled={self.enabled_providers}, "
            f"timeout={self.timeout}s, retries={self.retry_attempts}"
        )


class PaperProvider(ABC):
    """
    Abstract base class for paper providers.
    
    All providers must:
    1. Accept params dict with 'query', optionally 'domain', 'batch_size'
    2. Internally respect batch_size limit in API calls
    3. Return list of normalized paper dicts
    4. Never return more than batch_size papers
    5. Handle errors gracefully (return partial results or empty list)
    """
    
    def __init__(self, config: Optional[ProviderConfig] = None):
        self.config = config or ProviderConfig()
        self.name = self.__class__.__name__.replace("Provider", "").lower()
    
    @abstractmethod
    def fetch(self, params: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Fetch papers from this provider.
        
        CRITICAL: Must respect batch_size limit by limiting API requests.
        Providers must NOT return more papers than batch_size.
        
        Args:
            params: Dict containing:
                - 'query' (required): Search query string
                - 'domain' (optional): Domain hint for filtering
                - 'batch_size' (optional): Max papers to fetch (uses config.batch_size if not provided)
        
        Returns:
            List of paper dicts, each with fields:
            {
              "title": str (required),
              "abstract": str (may be None),
              "authors": [{"name": str}, ...],
              "year": int (may be None),
              "venue": str (may be None),
              "doi": str (may be None),
              "external_ids": {id_type: id_value, ...},
              "source": str (provider name),
              "pdf_url": str (may be None)
            }
        
        Guarantees:
            - len(returned_papers) <= batch_size
            - All papers have 'title' field
            - All papers have 'source' set to provider name
        """
        pass
    
    def _get_batch_size(self, params: Dict[str, Any]) -> int:
        """Extract batch_size from params or use config default."""
        return params.get("batch_size", self.config.batch_size)
    
    def _normalize_paper(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize provider-specific format to standard contract.
        
        Subclasses may override for provider-specific transformations.
        """
        return data


class PaperProviderError(Exception):
    """Exception raised by provider during fetch."""
    pass
