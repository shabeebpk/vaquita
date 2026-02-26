"""Factory for embedding providers.

Configuration-driven provider selection via admin_policy.
"""
import logging
from app.embeddings.interface import EmbeddingProvider
from app.embeddings.providers import SentenceTransformerProvider
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)

PROVIDERS = {
    "sentence_transformers": SentenceTransformerProvider,
}


def get_embedding_provider(name: str = None, **kwargs) -> EmbeddingProvider:
    """Get an embedding provider by name, with config from admin_policy.

    Args:
        name: provider name (default: from admin_policy.embeddings.default_provider)
        **kwargs: additional arguments override admin_policy config

    Returns:
        EmbeddingProvider instance
    """
    if name is None:
        name = admin_policy.embeddings.default_provider

    if name not in PROVIDERS:
        raise ValueError(f"Unknown embedding provider: {name}. Available: {list(PROVIDERS.keys())}")

    provider_class = PROVIDERS[name]
    
    # Load config from admin_policy based on provider name
    config_kwargs = {}
    if name == "sentence_transformers":
        st_config = admin_policy.embeddings.sentence_transformers
        config_kwargs = {
            "model_name": st_config.model_name,
            "batch_size": st_config.batch_size,
            "device": st_config.device,
        }
    
    # Override with any provided kwargs
    config_kwargs.update(kwargs)
    
    logger.info(f"Creating embedding provider: {name} with config {config_kwargs}")
    return provider_class(**config_kwargs)
