
import os
from typing import Optional, List, Union
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

class SystemSettings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=True,
        extra="ignore"
    )
    """
    Centralized system-level configuration.
    Reads from .env at startup. Immutable at runtime.
    """
    
    # Infrastructure
    DATABASE_URL: str = Field(...)
    REDIS_URL: str = Field("redis://localhost:6379/0")
    CELERY_BROKER_URL: str = Field("redis://localhost:6379/0")
    CELERY_RESULT_BACKEND: str = Field("redis://localhost:6379/1")
    
    # LLM Infrastructure
    LLM_PROVIDER: str = Field("openai")
    LLM_MODEL: str = Field("gpt-4o")
    LLM_TEMPERATURE: float = Field(0.0)
    LLM_MAX_TOKENS: int = Field(1000)
    NVIDIA_BASE_URL: Optional[str] = Field(None)
    NVIDIA_API_KEY: Optional[str] = Field(None)
    OPENAI_API_KEY: Optional[str] = Field(None)
    
    
    # New Safety Caps
    SYSTEM_MAX_PAPERS_PER_JOB: int = Field(100)
    SYSTEM_MAX_FETCH_CYCLES: int = Field(5)
    SYSTEM_MAX_GRAPH_SIZE: int = Field(5000)
    
    
    # Prompt Files (System Assets)
    DOMAIN_RESOLVER_PROMPT_FILE: str = Field("domain_resolver.txt")
    TRIPLE_EXTRACTION_PROMPT_FILE: str = Field("triple_extraction.txt")
    DECISION_LLM_PROMPT_FILE: str = Field("decision_llm.txt")
    CLARIFICATION_PROMPT_FILE: str = Field("clarification_question.txt")
    USER_CLASSIFIER_PROMPT_FILE: str = Field("user_text_classifier.txt")
    CLARIFICATION_HIGH_PROMPT_FILE: str = Field("clarification_high_ambiguity.txt")
    CLARIFICATION_MEDIUM_PROMPT_FILE: str = Field("clarification_medium_ambiguity.txt")
    CLARIFICATION_LOW_PROMPT_FILE: str = Field("clarification_low_ambiguity.txt")


# Singleton instance
system_settings = SystemSettings()
