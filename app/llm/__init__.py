"""LLM Module: Centralized Language Model Abstraction.

Exports:
- LLMService: The global gateway for all LLM calls
- get_llm_service(): Factory to access the singleton service
- Adapters (for testing only): Not used by business logic

All business logic must use get_llm_service() only.
No phase should import adapters or provider SDKs directly.
"""

from app.llm.service import LLMService, get_llm_service, reset_llm_service

__all__ = [
    "LLMService",
    "get_llm_service",
    "reset_llm_service",
]
