
import logging
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field

class QueryConfig(BaseModel):
    """Query building: keyword injection and search scope tuning."""
    focus_areas: List[str] = Field(default_factory=list)
    """Keywords to inject into search query via AND/OR."""


class PathReasoningConfig(BaseModel):
    """Path reasoning: seed nodes and entity filtering."""
    seeds: List[str] = Field(default_factory=list)
    """Entities that must appear in hypothesis paths (strict match on canonical names)."""
    
    stoplist: List[str] = Field(default_factory=list)
    """Entities that must NOT appear in hypothesis paths (strict match on canonical names)."""


class HypothesisConfig(BaseModel):
    """Hypothesis scoring: predicate preferences."""
    preferred_predicates: List[str] = Field(default_factory=list)
    """Predicates to boost in hypothesis scoring (strict match on canonical labels)."""


class GraphConfig(BaseModel):
    """Graph construction: entity filtering."""
    excluded_entities: List[str] = Field(default_factory=list)
    """Entities to remove from graph before semantic merge (strict match on canonical names)."""


class JobConfig(BaseModel):
    """
    Per-job dynamic configuration (copied to job table).
    Strictly separated from SystemSettings (system-wide) and AdminPolicy (admin-wide).
    Each job can override these settings independently.
    
    Sections:
    - domain: LLM domain classification
    - query_config: Search query tuning via focus_areas
    - path_reasoning_config: Path filtering via seeds/stoplist
    - hypothesis_config: Scoring preferences via preferred_predicates
    - graph_config: Entity exclusion via excluded_entities
    """
    domain: str = "biomedical"
    query_config: QueryConfig = Field(default_factory=QueryConfig)
    path_reasoning_config: PathReasoningConfig = Field(default_factory=PathReasoningConfig)
    hypothesis_config: HypothesisConfig = Field(default_factory=HypothesisConfig)
    graph_config: GraphConfig = Field(default_factory=GraphConfig)

    class Config:
        extra = "ignore"  # Allow backward compatibility if old keys exist
