"""Global LLM Service: Unified Orchestrator with Dynamic Registry.

This service implements the Unified LLM Provider Architecture:
- One global gateway for all text generation.
- Dynamic provider registration (no hardcoded if/else).
- Managed fallback loop across active providers.
- Configuration-driven priority and availability.
"""

import logging
from typing import Dict, Optional
from app.llm.providers.base import BaseLLMProvider
from app.llm.providers.registry import registry
import app.llm.providers  # Ensure all providers are registered

logger = logging.getLogger(__name__)

class LLMServiceError(Exception):
    """Raised when all LLM providers in the fallback chain fail."""
    pass

class LLMService:
    """Global orchestrator for LLM providers."""
    
    def __init__(self):
        """Initialize service by instantiating active providers from registry."""
        from app.config.system_settings import system_settings
        from app.config.admin_policy import admin_policy
        
        self.settings = system_settings
        self.policy = admin_policy.llm
        self.providers: Dict[str, BaseLLMProvider] = {}
        
        self._init_providers()
        
        logger.info(
            f"LLMService initialized with {len(self.providers)} providers. "
            f"Fallback order: {self.policy.fallback_order}"
        )

    def _init_providers(self):
        """Instantiate active providers identified by AdminPolicy."""
        for provider_id, config in self.policy.providers.items():
            if not config.active:
                logger.debug(f"Provider {provider_id} is inactive; skipping.")
                continue
                
            try:
                # Look up class in dynamic registry instead of hardcoded if/else
                provider_cls = registry.get_provider_class(provider_id)
                if not provider_cls:
                    logger.warning(f"No implementation found for provider: {provider_id}")
                    continue
                
                provider = self._create_provider_instance(provider_id, provider_cls)
                if provider:
                    self.providers[provider_id] = provider
                    logger.debug(f"Instantiated provider: {provider_id}")
            except Exception as e:
                logger.error(f"Failed to initialize provider {provider_id}: {e}")

    def _create_provider_instance(self, provider_id: str, provider_cls: type) -> Optional[BaseLLMProvider]:
        """Factory for provider instances mapping system credentials to the class."""
        # Policy-driven default parameters
        common_kwargs = {
            "model": self.policy.defaults.model,
            "temperature": self.policy.defaults.temperature,
            "max_tokens": self.policy.defaults.max_tokens
        }

        # Dynamically collect credentials based on provider's declared needs
        # e.g., NVIDIA_API_KEY -> credentials['api_key']
        credentials = {}
        for key in getattr(provider_cls, "CREDENTIAL_KEYS", []):
            val = getattr(self.settings, key, None)
            if val is not None:
                # Store as lowercased key without provider prefix
                clean_key = key.lower().replace(f"{provider_id}_", "")
                credentials[clean_key] = val
        
        # Instantiate from the registry class
        return provider_cls(credentials=credentials, **common_kwargs)

    def generate(self, prompt: str) -> str:
        """Generate text using the global fallback loop."""
        if not prompt or not isinstance(prompt, str):
            logger.warning("generate() called with invalid prompt.")
            return ""

        errors = []
        for provider_id in self.policy.fallback_order:
            provider = self.providers.get(provider_id)
            if not provider:
                continue
                
            try:
                logger.info(f"LLM Attempt: {provider_id}")
                result = provider.generate(prompt)
                
                if result and result.strip():
                    return result
            except Exception as e:
                logger.error(f"Provider {provider_id} failed: {e}")
                errors.append(f"{provider_id}: {str(e)}")
                continue
        
        final_error = f"All LLM providers failed: {'; '.join(errors)}"
        logger.error(final_error)
        raise LLMServiceError(final_error)

# Singleton setup
_instance: Optional[LLMService] = None

def get_llm_service() -> LLMService:
    global _instance
    if _instance is None:
        _instance = LLMService()
    return _instance

def reset_llm_service():
    global _instance
    _instance = None
