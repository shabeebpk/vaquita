"""Global LLM Service: Single Gateway for All LLM Invocations.

This module is the ONLY place in the codebase that directly uses adapters.
All business logic (triple extraction, decision control, handlers, agents)
must call this service, never instantiate adapters directly.

Configuration comes entirely from environment variables.
"""

import os
import logging
from typing import Optional

from app.llm.adapters import get_adapter, BaseAdapter

logger = logging.getLogger(__name__)


class LLMService:
    """Global gateway for all LLM calls.
    
    Responsibilities:
    - Load config from .env (provider, model, temperature, etc.)
    - Instantiate exactly one adapter
    - Execute prompts and handle errors
    - Fallback to DummyAdapter on failure
    - Guarantee that generate() never raises provider-specific exceptions
    
    This service is stateless and safe to call concurrently.
    """
    
    def __init__(self):
        """Initialize the service with .env configuration."""
        self.provider = os.getenv("LLM_PROVIDER", "dummy").lower().strip()
        self.model = os.getenv("LLM_MODEL", "gpt-4o-mini")
        self.temperature = float(os.getenv("LLM_TEMPERATURE", "0.0"))
        self.max_tokens = int(os.getenv("LLM_MAX_TOKENS", "800"))
        
        # Provider-specific config
        self.openai_api_key = os.getenv("OPENAI_API_KEY")
        self.nvidia_api_key = os.getenv("NVIDIA_API_KEY")
        self.nvidia_base_url = os.getenv("NVIDIA_BASE_URL")
        
        self.adapter = self._init_adapter()
        
        logger.info(
            f"LLMService initialized: provider={self.provider}, "
            f"model={self.model}, temperature={self.temperature}, "
            f"adapter={self.adapter.__class__.__name__}"
        )
    
    def _init_adapter(self) -> BaseAdapter:
        """Initialize the adapter based on environment configuration.
        
        Always returns a valid adapter; never raises exceptions.
        Falls back to DummyAdapter if the configured provider fails.
        """
        try:
            kwargs = {
                "model": self.model,
                "temperature": self.temperature,
            }
            
            if self.openai_api_key:
                kwargs["api_key"] = self.openai_api_key
            
            if self.nvidia_api_key:
                kwargs["api_key"] = self.nvidia_api_key
            
            if self.nvidia_base_url:
                kwargs["base_url"] = self.nvidia_base_url
            
            adapter = get_adapter(self.provider, **kwargs)
            logger.info(f"Adapter initialized: {adapter.__class__.__name__}")
            return adapter
        
        except Exception as e:
            logger.error(f"Failed to initialize adapter for provider {self.provider}: {e}")
            logger.info("Falling back to DummyAdapter")
            from app.llm.adapters import DummyAdapter
            return DummyAdapter()
    
    def generate(self, prompt: str) -> str:
        """Generate text from a prompt.
        
        This is the ONLY public method of LLMService.
        
        Args:
            prompt: The input prompt string.
        
        Returns:
            Raw text output from the adapter.
            Never raises provider-specific exceptions.
            Always returns a string (may be empty).
        
        Guarantees:
            - All LLM calls go through this method
            - No provider-specific exceptions leak upward
            - Automatic fallback to safe defaults on any error
            - Comprehensive logging
        """
        if not prompt or not isinstance(prompt, str):
            logger.warning(f"generate() called with invalid prompt: {type(prompt)}")
            return ""
        
        try:
            logger.debug(f"LLM call with adapter={self.adapter.__class__.__name__}")
            result = self.adapter.call(prompt)
            
            if not result or not isinstance(result, str):
                logger.debug("LLM returned empty or non-string result")
                return ""
            
            logger.debug(f"LLM returned {len(result)} characters")
            return result
        
        except Exception as e:
            logger.error(f"LLM generation failed: {e}")
            logger.info("Returning empty string as safe fallback")
            return ""


# Global singleton instance
_global_llm_service: Optional[LLMService] = None


def get_llm_service() -> LLMService:
    """Get the global LLM service instance (lazy singleton).
    
    Returns:
        The initialized LLMService.
    """
    global _global_llm_service
    if _global_llm_service is None:
        _global_llm_service = LLMService()
    return _global_llm_service


def reset_llm_service() -> None:
    """Reset the global service (for testing only)."""
    global _global_llm_service
    _global_llm_service = None
