
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
    
    # LLM Infrastructure (Secrets only)
    NVIDIA_BASE_URL: Optional[str] = Field(None)
    NVIDIA_API_KEY: Optional[str] = Field(None)
    OPENAI_API_KEY: Optional[str] = Field(None)


# Singleton instance
system_settings = SystemSettings()
