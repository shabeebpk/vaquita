"""
Providers package: paper source adapters with batch size enforcement.

All providers respect FETCH_BATCH_SIZE limit at the API call level.
"""
import logging
from typing import Optional

from app.fetching.providers.base import PaperProvider, ProviderConfig
from app.fetching.providers.arxiv import ArxivProvider
from app.fetching.providers.crossref import CrossRefProvider
from app.fetching.providers.pubmed import PubMedProvider
from app.fetching.providers.semantic_scholar import SemanticScholarProvider

logger = logging.getLogger(__name__)


def get_provider(provider_name: str, config: Optional[ProviderConfig] = None) -> Optional[PaperProvider]:
    """
    Factory to get a provider instance by name.
    
    Args:
        provider_name: 'arxiv', 'crossref', 'pubmed', 'semantic_scholar', etc.
        config: ProviderConfig (created if None)
    
    Returns:
        PaperProvider instance or None if not recognized
    """
    if config is None:
        config = ProviderConfig()
    
    providers = {
        "arxiv": ArxivProvider,
        "crossref": CrossRefProvider,
        "pubmed": PubMedProvider,
        "semantic_scholar": SemanticScholarProvider,
    }
    
    provider_class = providers.get(provider_name.lower())
    if not provider_class:
        logger.error(f"Unknown provider: {provider_name}")
        return None
    
    logger.debug(f"Creating {provider_name} provider with batch_size={config.batch_size}")
    return provider_class(config)


def select_provider_for_domain(
    domain: Optional[str],
    config: Optional[ProviderConfig] = None
) -> Optional[PaperProvider]:
    """
    Select an appropriate provider based on domain.
    Falls back to first enabled provider if domain is None.
    
    Args:
        domain: Resolved domain (e.g., 'biomedical', 'computer_science')
        config: ProviderConfig (created if None)
    
    Returns:
        PaperProvider instance or None
    """
    if config is None:
        config = ProviderConfig()
    
    # Domain-to-provider mapping
    domain_mapping = {
        "biomedical": "pubmed",
        "biology": "pubmed",
        "medicine": "pubmed",
        "computer_science": "arxiv",
        "physics": "arxiv",
        "mathematics": "arxiv",
        "chemistry": "crossref",
        "engineering": "crossref",
    }
    
    preferred_provider = domain_mapping.get(domain, None) if domain else None
    
    # Check if preferred provider is enabled
    if preferred_provider and preferred_provider in config.enabled_providers:
        logger.info(f"Selected {preferred_provider} for domain {domain}")
        return get_provider(preferred_provider, config)
    
    # Fall back to first enabled provider
    if config.enabled_providers:
        fallback = config.enabled_providers[0]
        logger.info(f"Falling back to {fallback} provider (domain={domain})")
        return get_provider(fallback, config)
    
    logger.warning("No providers enabled")
    return None


__all__ = [
    "PaperProvider",
    "ProviderConfig",
    "ArxivProvider",
    "CrossRefProvider",
    "PubMedProvider",
    "SemanticScholarProvider",
    "get_provider",
    "select_provider_for_domain",
]
