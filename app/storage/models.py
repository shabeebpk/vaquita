from sqlalchemy import Column, Integer, Text, String, Boolean, DateTime, ForeignKey, JSON, Enum
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from .db import Base
import enum


# ============================================================================
# Enumeration Types for Type Safety and Clarity
# ============================================================================

class FileOriginType(str, enum.Enum):
    """Origin of a physical file in the system."""
    USER_UPLOAD = "user_upload"
    PAPER_DOWNLOAD = "paper_download"


class MessageRole(str, enum.Enum):
    """Actor in a conversation message."""
    USER = "user"
    SYSTEM = "system"


class MessageType(str, enum.Enum):
    """Semantic classification of a conversation message."""
    TEXT = "text"
    STATUS = "status"
    QUESTION = "question"
    ANSWER = "answer"
    UPLOAD_NOTICE = "upload_notice"


class IngestionSourceType(str, enum.Enum):
    """Source origin for a unit of text entering the ingestion pipeline."""
    USER_TEXT = "user_text"
    PAPER_ABSTRACT = "paper_abstract"
    PDF_TEXT = "pdf_text"
    API_TEXT = "api_text"


# ============================================================================
# Core Models: Chat and Job Management
# ============================================================================

class Job(Base):
    """
    Represents a single literature review session or task.
    
    Responsibility: Track job-level metadata and status. No longer stores user text;
    all user input is now exclusively in ConversationMessage.
    
    Future extensibility:
    - Add fields for session metadata (model config, parameters, user context)
    - Support branching/forking by adding parent_job_id
    - Add job_state_snapshot for fast filtering by stage
    """
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    status = Column(String, nullable=True)  # e.g., "in_progress", "completed", "error"


class ConversationMessage(Base):
    """
    Single source of truth for all chat UI rendering and replay.
    
    Responsibility: Store every user or system message with rich type and reference
    information. This is the authoritative record for UI reconstruction without SSE history.
    
    Each row represents one discrete message (one user turn or one system response).
    The role + message_type combination determines interpretation and rendering.
    
    Future extensibility:
    - Add reply_to_message_id for nested conversation threads
    - Add tokens_used for LLM-based messages to track cost
    - Add human_feedback_score for training/fine-tuning signals
    - Add message_metadata JSONB for extensible attributes per message type
    """
    __tablename__ = "conversation_messages"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    role = Column(String, nullable=False)  # 'user' or 'system' (Enum: MessageRole)
    message_type = Column(String, nullable=False)  # 'text', 'status', 'question', 'answer', 'upload_notice' (Enum: MessageType)
    content = Column(Text, nullable=False)  # The actual message text
    
    # Optional FK to decision_results for linking user decisions to system outputs
    related_decision_id = Column(Integer, ForeignKey("decision_results.id"), nullable=True, index=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)


# ============================================================================
# File Management
# ============================================================================

class Paper(Base):
    """
    Reusable scholarly metadata without full-text content.
    
    Responsibility: Store bibliographic and provenance metadata for papers
    independent of whether they are downloaded or already extracted.
    A Paper may exist without a corresponding File (e.g., abstract-only entries).
    
    Future extensibility:
    - Add embeddings_vector for semantic search
    - Add citation_count, h_index for relevance metrics
    - Add keywords JSONB array for filtering and topic discovery
    - Add full_text_available Boolean to track source readiness
    """
    __tablename__ = "papers"

    id = Column(Integer, primary_key=True)
    title = Column(String, nullable=False)
    abstract = Column(Text, nullable=True)
    authors = Column(JSON, nullable=True)  # Array of author dicts: [{name, affiliation}, ...]
    year = Column(Integer, nullable=True)
    venue = Column(String, nullable=True)  # Conference, journal, etc.
    doi = Column(String, nullable=True, unique=True, index=True)
    source = Column(String, nullable=False)  # 'arxiv', 'crossref', 'pubmed', etc.
    pdf_url = Column(String, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class File(Base):
    """
    Represents a single physical file stored by the system.
    
    Responsibility: Track only file artifacts (path, name, type, provenance).
    No processing state, no aggregation logic. A File always belongs to a Job
    and may optionally reference the Paper from which it was downloaded.
    
    Future extensibility:
    - Add file_size, checksum for integrity checking
    - Add storage_location to distinguish between local, S3, etc.
    - Add deleted_at soft-delete timestamp for retention/compliance
    - Add extracted_text_available to optimize lazy-loading decisions
    """
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    paper_id = Column(Integer, ForeignKey("papers.id"), nullable=True, index=True)  # FK if downloaded from a paper
    origin_type = Column(String, nullable=False)  # 'user_upload' or 'paper_download' (Enum: FileOriginType)
    stored_path = Column(String, nullable=False)  # Full system path to the file
    original_filename = Column(String, nullable=False)
    file_type = Column(String, nullable=False)  # 'pdf', 'docx', 'txt', 'json', etc.
    
    uploaded_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# ============================================================================
# Ingestion Pipeline: Single Entry Point
# ============================================================================

class IngestionSource(Base):
    """
    Single entry point into the text processing pipeline.
    
    Responsibility: Represent one independent unit of raw text to be processed.
    Strict separation from aggregation; each row is atomic and may be processed
    independently. source_ref is a string identifier (e.g., "file:123" or "paper:456")
    allowing flexible reference to upstream artifacts without hard-coded ForeignKeys.
    
    processed Boolean tracks whether this source has been through normalization
    and text block generation. One-to-one normalization per source (no aggregation).
    
    Future extensibility:
    - Add source_properties JSONB for variable metadata (language, encoding, format hints)
    - Add processing_log JSONB to track which operations touched this source
    - Add retry_count for failed processing attempts
    - Add confidence_score for text quality/reliability
    """
    __tablename__ = "ingestion_sources"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    source_type = Column(String, nullable=False)  # 'user_text', 'paper_abstract', 'pdf_text', 'api_text' (Enum: IngestionSourceType)
    source_ref = Column(String, nullable=False)  # e.g., "file:42", "paper:7", "message:128" — flexible identifier as string
    raw_text = Column(Text, nullable=False)
    processed = Column(Boolean, default=False, nullable=False, index=True)  # True once normalization + text blocks are done
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# ============================================================================
# Text Processing Outputs
# ============================================================================

class TextBlock(Base):
    """
    Segmented unit of text derived from normalized IngestionSource.
    
    Responsibility: Represent a meaningful span of text (paragraph, sentence group, etc.)
    produced by segmentation strategy applied to a single normalized IngestionSource.
    Each TextBlock is traceable to its parent IngestionSource for full provenance.
    
    Future extensibility:
    - Add embedding_vector for semantic search and clustering
    - Add linguistic_features JSONB (POS tags, entity types, etc.)
    - Add quality_score to rank confidence in segmentation
    - Add human_annotation for feedback loops and refinement
    """
    __tablename__ = "text_blocks"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    ingestion_source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=False, index=True)  # Parent source
    block_text = Column(Text, nullable=False)
    block_order = Column(Integer, nullable=False)  # Sequence number within source
    block_type = Column(String, nullable=False)  # 'paragraph', 'section', 'sentence_group', etc.
    segmentation_strategy = Column(String, nullable=False)  # Name of strategy: 'sentence_tokenizer', 'spacy', etc.
    triples_extracted = Column(Boolean, default=False, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class Triple(Base):
    """
    Atomic fact (subject, predicate, object) extracted from a TextBlock.
    
    Responsibility: Store extracted RDF-like triples with full provenance.
    Each triple is traceable to the block and ingestion source it came from.
    Supports both triple-to-graph aggregation and filtered hypothesis generation.
    
    Future extensibility:
    - Add confidence_score from the extraction model
    - Add extractor_version for reproducibility
    - Add extraction_metadata JSONB for raw extractor output
    - Add human_correction tracking for feedback loops
    """
    __tablename__ = "triples"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    block_id = Column(Integer, ForeignKey("text_blocks.id"), nullable=False, index=True)
    ingestion_source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=False, index=True)  # Provenance
    subject = Column(String, nullable=False)
    predicate = Column(String, nullable=False)
    object = Column(String, nullable=False)
    extractor_name = Column(String, nullable=False)  # Name of extraction strategy/model
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# ============================================================================
# Knowledge Graph and Hypothesis
# ============================================================================

class SemanticGraph(Base):
    """
    Final Phase-3 semantic graph output as JSONB.
    
    Responsibility: Store the complete, normalized knowledge graph produced
    after triple aggregation, deduplication, and relationship inference.
    This is a read-only artifact (pipeline output); graph reconstruction
    is handled by services, not here.
    
    Stores summary metadata (node_count, edge_count) for quick filtering
    and downstream decision-making without full deserialization.
    
    Future extensibility:
    - Add graph_version for tracking iterative updates as new sources are added
    - Add quality_metrics JSONB (clustering_coeff, density, centrality scores)
    - Add compressed_graph for efficient storage of large graphs
    - Add diff_from_previous for incremental update tracking
    """
    __tablename__ = "semantic_graphs"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, unique=True, index=True)
    graph = Column(JSONB, nullable=False)  # {nodes: [...], edges: [...], summary: {...}}
    node_count = Column(Integer, nullable=False)
    edge_count = Column(Integer, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, nullable=True)


class Hypothesis(Base):
    """
    Candidate relationship or claim derived from semantic graph.
    
    Responsibility: Represent a potential answer to a query or an interesting
    structural pattern discovered in the graph. Each hypothesis is tagged with
    confidence, mode (explore vs. query-driven), and filtering results.
    Optionally linked to a ReasoningQuery for query-mode hypotheses.
    
    Future extensibility:
    - Add support_count (number of paths or sources supporting this hypothesis)
    - Add counter_evidence_count to balance confidence scoring
    - Add human_evaluation_score for iterative refinement
    - Add reasoning_chain JSONB for transparent decision audit
    """
    __tablename__ = "hypotheses"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    source = Column(String, nullable=False)
    target = Column(String, nullable=False)
    path = Column(JSONB, nullable=False)  # Ordered array of node texts
    predicates = Column(JSONB, nullable=False)  # Array of edge labels along path
    explanation = Column(Text, nullable=False)
    confidence = Column(Integer, nullable=False)  # 0–100 score
    mode = Column(String, nullable=False)  # 'explore' or 'query'
    
    query_id = Column(Integer, ForeignKey("reasoning_queries.id"), nullable=True, index=True)
    passed_filter = Column(Boolean, default=False, nullable=False)
    filter_reason = Column(JSONB, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


class ReasoningQuery(Base):
    """
    User- or system-initiated question to reason over the semantic graph.
    
    Responsibility: Capture formal queries that drive hypothesis generation.
    Each query may produce multiple hypotheses via graph traversal and filtering.
    Supports iterative refinement in chat-driven workflow.
    
    Future extensibility:
    - Add query_type for classification ('path_query', 'similarity_query', etc.)
    - Add query_vector for semantic search and clustering
    - Add result_count tracking how many hypotheses were generated
    - Add feedback_score for ranking query quality
    """
    __tablename__ = "reasoning_queries"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    query_text = Column(Text, nullable=False)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)


# ============================================================================
# Decision and Control
# ============================================================================

class DecisionResult(Base):
    """
    Output of Phase-5 decision-making: what action to take next.
    
    Responsibility: Capture one decision point (halt, ask user, continue processing, etc.)
    with full auditability including measurements snapshot, provider used, and fallback info.
    May be referenced by ConversationMessage for linking user decisions to system state.
    
    Future extensibility:
    - Add decision_path JSONB for transparent rule/LLM reasoning
    - Add user_override Boolean if human rejected the decision
    - Add alternative_decisions JSONB for decision tree exploration
    - Add impact_metrics JSONB to track downstream effects of this decision
    """
    __tablename__ = "decision_results"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    decision_label = Column(String, nullable=False)  # e.g., 'halt_confident', 'ask_clarification', 'continue'
    provider_used = Column(String, nullable=False)  # 'rule_based', 'llm', 'hybrid'
    measurements_snapshot = Column(JSONB, nullable=False)  # All measurements at decision time
    fallback_used = Column(Boolean, default=False, nullable=False)
    fallback_reason = Column(Text, nullable=True)
    
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, nullable=True)
