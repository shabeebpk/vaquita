
import os
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
    REDIS_URL: str = Field(...)
    CELERY_BROKER_URL: str = Field(...)
    CELERY_RESULT_BACKEND: str = Field(...)
    
    # LLM Infrastructure (from .env only)
    NVIDIA_BASE_URL: str = Field(...)
    NVIDIA_API_KEY: str = Field(...)
    LLM_MODEL: str = Field(...)
    SEMANTIC_SCHOLAR_API_KEY: str = Field(...)
    SEMANTIC_SCHOLAR_URL: str = Field(...)
    
    # System constraints (from .env only)
    SYSTEM_MAX_PAPERS_PER_JOB: int = Field(...)


# Singleton instance
system_settings = SystemSettings()
