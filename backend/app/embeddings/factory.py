"""Factory for embedding providers.

Configuration-driven provider selection.
"""
import logging
from app.embeddings.interface import EmbeddingProvider
from app.embeddings.providers import SentenceTransformerProvider

logger = logging.getLogger(__name__)

PROVIDERS = {
    "sentence-transformers": SentenceTransformerProvider,
}


def get_embedding_provider(name: str = "sentence-transformers", **kwargs) -> EmbeddingProvider:
    """Get an embedding provider by name.

    Args:
        name: provider name (default: sentence-transformers)
        **kwargs: additional arguments passed to provider __init__

    Returns:
        EmbeddingProvider instance
    """
    if name not in PROVIDERS:
        raise ValueError(f"Unknown embedding provider: {name}. Available: {list(PROVIDERS.keys())}")

    provider_class = PROVIDERS[name]
    logger.info(f"Creating embedding provider: {name}")
    return provider_class(**kwargs)
