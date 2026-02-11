from typing import Dict, Type, Optional
from app.llm.providers.base import BaseLLMProvider

class ProviderRegistry:
    """Registry to keep track of available LLM providers."""
    
    _providers: Dict[str, Type[BaseLLMProvider]] = {}

    @classmethod
    def register(cls, provider_id: str):
        """Decorator to register a provider class."""
        def wrapper(provider_cls: Type[BaseLLMProvider]):
            cls._providers[provider_id.lower()] = provider_cls
            return provider_cls
        return wrapper

    @classmethod
    def get_provider_class(cls, provider_id: str) -> Optional[Type[BaseLLMProvider]]:
        """Retrieve a provider class by ID."""
        return cls._providers.get(provider_id.lower())

    @classmethod
    def list_providers(cls) -> list:
        """List all registered provider IDs."""
        return list(cls._providers.keys())

# Global registry instance
registry = ProviderRegistry()
