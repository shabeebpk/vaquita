"""
AdminPolicy: Global admin-controlled configuration layer.

This module defines the structure and loader for admin_policy.json.
AdminPolicy is loaded once at system startup, validated via Pydantic,
and accessed via a singleton instance.

AdminPolicy contains:
- Domain definitions and keywords
- Algorithm thresholds and parameters
- Query orchestrator settings
- Decision provider strategy

AdminPolicy is NEVER stored in the database or copied to jobs.
"""

import os
import json
import logging
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field, field_validator

logger = logging.getLogger(__name__)


# ===== Pydantic Models =====

class LLMProviderPolicy(BaseModel):
    """Policy for a single LLM provider."""
    active: bool = True


class LLMDefaults(BaseModel):
    """System-level default parameters for all LLM calls."""
    model: str = "gpt-4o"
    temperature: float = 0.0
    max_tokens: int = 1000


class LLMPolicy(BaseModel):
    """Global LLM policy and fallback configuration."""
    providers: Dict[str, LLMProviderPolicy] = Field(default_factory=dict)
    fallback_order: List[str] = Field(default_factory=list)
    defaults: LLMDefaults = Field(default_factory=LLMDefaults)


class DecisionThresholds(BaseModel):
    """Decision logic thresholds."""
    confidence_norm_factor: int = 10
    high_confidence_threshold: float = 0.7
    dominant_gap_ratio: float = 0.3
    low_diversity_pairs_threshold: int = 2
    diversity_ratio_threshold: float = 0.3
    sparse_graph_density_threshold: float = 0.05
    path_support_threshold: int = 2
    stability_cycle_threshold: int = 3
    min_absolute_growth_threshold: float = 0.5
    min_relative_growth_threshold: float = 0.1
    passed_to_total_ratio_threshold: float = 0.2
    minimum_hypotheses_threshold: int = 1
    semantic_similarity_threshold: float = 0.7


class SignalWeights(BaseModel):
    """Signal measurement weights."""
    passed_hypothesis_count: float = 1.0
    mean_confidence: float = 0.8
    graph_density: float = 0.5
    filtered_to_total_ratio: float = 0.3


class SignalMaxDeltas(BaseModel):
    """Signal measurement max deltas for normalization."""
    passed_hypothesis_count: float = 100.0
    mean_confidence: float = 20.0
    graph_density: float = 0.2
    filtered_to_total_ratio: float = 0.5


class SignalParams(BaseModel):
    """Signal computation parameters."""
    positive_threshold: float = 1.0
    negative_threshold: float = -1.0
    reputation_positive_delta: int = 10
    reputation_negative_delta: int = -20
    weights: SignalWeights = Field(default_factory=SignalWeights)
    max_deltas: SignalMaxDeltas = Field(default_factory=SignalMaxDeltas)


class ExtractionConfig(BaseModel):
    """Configuration for physical layout analysis (DLA)."""
    whitelisted_regions: List[str] = ["abstract", "introduction", "body", "conclusion", "results", "methods"]
    excluded_regions: List[str] = ["references", "bibliography", "acknowledgments", "appendix", "author_contributions"]
    enable_zonal_slicing: bool = True
    fallback_to_full_text: bool = True
    column_width_threshold: int = 200

class RefineryConfig(BaseModel):
    """Configuration for LLM-based text cleaning."""
    model: str = "nvidia/llama3-chatqa-1.5-8b"
    prompt_asset: str = "text_refinery.txt"
    temperature: float = 0.0
    max_tokens_per_span: int = 1000
    enable_recovery: bool = True
    needs_refinement_types: List[str] = ["pdf_text", "user_text"]

class SlicingConfig(BaseModel):
    """Configuration for rule-based sentence slicing."""
    strategy: str = "block"
    sentences_per_block: int = 4
    max_tokens_per_block: int = 250
    overlap_sentences: int = 0


class DeduplicationDefaults(BaseModel):
    """Deduplication and fingerprinting parameters."""
    algorithm: str = "sha256"
    similarity_threshold: float = 0.95
    components: List[str] = ["title", "abstract"]


class PathReasoningDefaults(BaseModel):
    """Path reasoning and hypothesis generation parameters."""
    max_hops: int = 4
    allow_len3: bool = True
    preferred_predicate_boost_factor: float = 1.2
    """Multiplier for confidence when preferred predicates found (min 1.0, max 2.0)."""
    hub_degree_threshold: int = 50
    """Max node degree for intermediate nodes in a path. High-degree hub nodes are suppressed."""
    min_confidence: int = 2
    """Minimum confidence score a hypothesis must have to pass the evidence threshold filter."""



class DomainResolution(BaseModel):
    """Domain resolution parameters for LLM-based classification."""
    allowed_domains: List[str] = Field(default_factory=list)
    llm_order: List[str] = Field(default_factory=list)


class IndirectPath(BaseModel):
    """Indirect path measurement parameters."""
    enabled: bool = True
    temporal_placeholders: bool = True
    dominance_gap_threshold: float = 0.2
    min_length: int = 3
    max_length: int = 4


class TripleExtractionConfig(BaseModel):
    """Configuration for Triple Extraction stage."""
    format: str = "line"  # "json" or "line"
    delimiter: str = "|"
    model: Optional[str] = None
    max_tokens: int = 1000

class DownloaderConfig(BaseModel):
    """Configuration for strategic paper downloading."""
    batch_size: int = 1
    """Number of papers to download and ingest per cycle for 'Evenly Cooked' growth."""

class Algorithm(BaseModel):
    """Algorithm-related configurations."""
    decision_thresholds: DecisionThresholds = Field(default_factory=DecisionThresholds)
    signal_params: SignalParams = Field(default_factory=SignalParams)
    extraction: ExtractionConfig = Field(default_factory=ExtractionConfig)
    refinery: RefineryConfig = Field(default_factory=RefineryConfig)
    slicing: SlicingConfig = Field(default_factory=SlicingConfig)
    triple_extraction: TripleExtractionConfig = Field(default_factory=TripleExtractionConfig)
    deduplication_defaults: DeduplicationDefaults = Field(default_factory=DeduplicationDefaults)
    path_reasoning_defaults: PathReasoningDefaults = Field(default_factory=PathReasoningDefaults)
    domain_resolution: DomainResolution = Field(default_factory=DomainResolution)
    indirect_path: IndirectPath = Field(default_factory=IndirectPath)


class FetchParams(BaseModel):
    """Fetch provider parameters."""
    timeout_seconds: int = 30
    retry_attempts: int = 3


class FetchProviderPolicy(BaseModel):
    """Policy for a single Fetch provider."""
    active: bool = True

class FetchAPIPolicy(BaseModel):
    """Configuration for fetch providers and their domain-specific priority."""
    providers: Dict[str, FetchProviderPolicy] = Field(default_factory=dict)
    domain_provider_order: Dict[str, List[str]] = Field(default_factory=dict)


class QueryOrchestrator(BaseModel):
    """Query orchestrator configuration."""
    signature_length: int = 64
    initial_reputation: int = 0
    exhaustion_decay: int = -5
    max_reuse_attempts: int = 3
    fetch_batch_size: int = 1
    results_limit: int = 10
    top_k_hypotheses: int = 1
    min_reputation: int = -10
    fetch_params: FetchParams = Field(default_factory=FetchParams)


class PromptAssets(BaseModel):
    """System-level prompt template filenames."""
    domain_resolver: str = "domain_resolver.txt"
    triple_extraction: str = "triple_extraction.txt"
    decision_llm: str = "decision_llm.txt"
    clarification_question: str = "clarification_question.txt"
    user_text_classifier: str = "user_text_classifier.txt"
    clarification_high: str = "clarification_high_ambiguity.txt"
    clarification_medium: str = "clarification_medium_ambiguity.txt"
    clarification_low: str = "clarification_low_ambiguity.txt"




class InputProcessing(BaseModel):
    """Input processing parameters for classification."""
    classification_threshold: int = 1000
    preview_snippet_length: int = 300


class GraphRules(BaseModel):
    """Config-driven rules for graph node sanitization."""
    node_removal_patterns: List[str] = Field(default_factory=list)
    """Regex patterns — a node matching any of these is removed."""
    node_removal_exact: List[str] = Field(default_factory=list)
    """Exact lowercase strings that are always removed (stop words, etc)."""
    generic_predicates: List[str] = Field(default_factory=list)
    """Predicates considered too generic to contribute meaningful paths."""


class AdminPolicy(BaseModel):
    """Root AdminPolicy model."""
    llm: LLMPolicy = Field(default_factory=LLMPolicy)
    algorithm: Algorithm = Field(default_factory=Algorithm)
    query_orchestrator: QueryOrchestrator = Field(default_factory=QueryOrchestrator)
    fetch_apis: FetchAPIPolicy = Field(default_factory=FetchAPIPolicy)
    prompt_assets: PromptAssets = Field(default_factory=PromptAssets)
    input_processing: InputProcessing = Field(default_factory=InputProcessing)
    decision_provider: str = "rule_based"
    graph_rules: GraphRules = Field(default_factory=GraphRules)
    downloader: DownloaderConfig = Field(default_factory=DownloaderConfig)

    @field_validator('algorithm')
    @classmethod
    def validate_algorithm(cls, v):
        if not v.domain_resolution.allowed_domains:
            raise ValueError("At least one domain must be defined in domain_resolution.allowed_domains")
        return v


# ===== Loader =====

def load_admin_policy() -> AdminPolicy:
    """
    Load and validate admin_policy.json.
    
    Returns:
        AdminPolicy: Validated admin policy instance.
        
    Raises:
        RuntimeError: If the file cannot be loaded or validation fails.
    """
    try:
        config_path = os.path.join(os.path.dirname(__file__), "admin_policy.json")
        
        with open(config_path, "r") as f:
            data = json.load(f)
        
        policy = AdminPolicy(**data)
        logger.info(f"Loaded AdminPolicy with decision_provider={policy.decision_provider}")
        return policy
        
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load admin policy: {e}")
        raise RuntimeError(f"Could not load admin policy: {e}") from e


# ===== Singleton Instance =====

# Load once at module import
admin_policy = load_admin_policy()
