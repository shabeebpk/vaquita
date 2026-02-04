
import os
from typing import Optional, List, Union
from pydantic import BaseSettings, Field

class SystemSettings(BaseSettings):
    """
    Centralized system-level configuration.
    Reads from .env at startup. Immutable at runtime.
    """
    
    # Infrastructure
    DATABASE_URL: str = Field(..., env="DATABASE_URL")
    REDIS_URL: str = Field("redis://localhost:6379/0", env="REDIS_URL")
    CELERY_BROKER_URL: str = Field("redis://localhost:6379/0", env="CELERY_BROKER_URL")
    CELERY_RESULT_BACKEND: str = Field("redis://localhost:6379/1", env="CELERY_RESULT_BACKEND")
    
    # LLM Infrastructure
    LLM_PROVIDER: str = Field("openai", env="LLM_PROVIDER")
    LLM_MODEL: str = Field("gpt-4o", env="LLM_MODEL")
    LLM_TEMPERATURE: float = Field(0.0, env="LLM_TEMPERATURE")
    LLM_MAX_TOKENS: int = Field(1000, env="LLM_MAX_TOKENS")
    NVIDIA_BASE_URL: Optional[str] = Field(None, env="NVIDIA_BASE_URL")
    NVIDIA_API_KEY: Optional[str] = Field(None, env="NVIDIA_API_KEY")
    OPENAI_API_KEY: Optional[str] = Field(None, env="OPENAI_API_KEY")
    
    # Hard System Safety Caps
    # Hard System Safety Caps
    FETCH_RESULTS_LIMIT: int = Field(10, env="FETCH_RESULTS_LIMIT")
    FETCH_TIMEOUT_SECONDS: int = Field(30, env="FETCH_TIMEOUT_SECONDS")
    FETCH_RETRY_ATTEMPTS: int = Field(3, env="FETCH_RETRY_ATTEMPTS")
    
    # New Safety Caps
    SYSTEM_MAX_PAPERS_PER_JOB: int = Field(100, env="SYSTEM_MAX_PAPERS_PER_JOB")
    SYSTEM_MAX_FETCH_CYCLES: int = Field(5, env="SYSTEM_MAX_FETCH_CYCLES")
    SYSTEM_MAX_GRAPH_SIZE: int = Field(5000, env="SYSTEM_MAX_GRAPH_SIZE")
    
    # Prompt Files (System Assets)
    DOMAIN_RESOLVER_PROMPT_FILE: str = Field("domain_resolver.txt", env="DOMAIN_RESOLVER_PROMPT_FILE")
    
    # Decision
    DECISION_PROVIDER: str = Field("rule_based", env="DECISION_PROVIDER")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = True

# Singleton instance
system_settings = SystemSettings()
