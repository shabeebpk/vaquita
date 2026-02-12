
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class QueryConfig(BaseModel):
    """Query expansion and search tuning."""
    query_expansion_terms: int = 3

class PathReasoningConfig(BaseModel):
    """Tuning for graph reasoning paths."""
    seeds: List[str] = []
    stoplist: List[str] = []

class ExpertSettings(BaseModel):
    """Feedback from domain experts."""
    assumptions: List[str] = []
    preferred_predicates: List[str] = []
    excluded_entities: List[str] = []

class JobConfig(BaseModel):
    """
    Per-job dynamic configuration.
    Strictly separated from SystemSettings and AdminPolicy.
    """
    domain: str = "biomedical"
    focus_areas: List[str] = []
    
    # Specialized Sections
    query_config: QueryConfig = Field(default_factory=QueryConfig)
    path_reasoning_config: PathReasoningConfig = Field(default_factory=PathReasoningConfig)
    expert_settings: ExpertSettings = Field(default_factory=ExpertSettings)

    class Config:
        extra = "ignore"  # Allow backward compatibility if old keys exist
