from sqlalchemy import Column, Integer, Text, String, Boolean, DateTime, ForeignKey, JSON
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime
from .db import Base

class Job(Base):
    __tablename__ = "jobs"

    id = Column(Integer, primary_key=True)
    user_text = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String)

class File(Base):
    __tablename__ = "files"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"))
    original_filename = Column(String)
    stored_path = Column(String)
    file_type = Column(String)
    uploaded_at = Column(DateTime, default=datetime.utcnow)
    processed = Column(Boolean, default=False)

class IngestionSource(Base):
    __tablename__ = "ingestion_sources"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    source_type = Column(String, nullable=False)  # 'user_text', 'pdf', 'docx', 'txt', 'api_abstract'
    source_ref = Column(String)  # filename, URL, or identifier
    raw_text = Column(Text, nullable=False)
    extracted_urls = Column(JSON, nullable=True)
    extracted_at = Column(DateTime, default=datetime.utcnow)

class NormalizedText(Base):
    __tablename__ = "normalized_text"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, unique=True)
    canonical_text = Column(Text, nullable=False)  # unified, normalized output
    source_count = Column(Integer)  # number of sources aggregated
    normalization_config = Column(JSON)  # encoding, tokenizer version, etc.
    created_at = Column(DateTime, default=datetime.utcnow)

class TextBlock(Base):
    __tablename__ = "text_blocks"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    block_text = Column(Text, nullable=False)
    block_order = Column(Integer)  # sequence number
    source_id = Column(Integer, ForeignKey("ingestion_sources.id"))  # which source
    block_type = Column(String)  # 'paragraph', 'section', 'sentence_group'
    segmentation_strategy = Column(String)  # which strategy produced this block
    created_at = Column(DateTime, default=datetime.utcnow)
    triples_extracted = Column(Boolean, default=False)

class Triple(Base):
    __tablename__ = "triples"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False)
    block_id = Column(Integer, ForeignKey("text_blocks.id"), nullable=False)
    source_id = Column(Integer, ForeignKey("ingestion_sources.id"), nullable=True)
    subject = Column(String, nullable=False)
    predicate = Column(String, nullable=False)
    object = Column(String, nullable=False)
    extractor_name = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)


class SemanticGraph(Base):
    """Stores the final Phase-3 semantic graph as a JSONB document.
    
    This is a read-only artifact of the pipeline: stores only the final semantic graph
    (nodes, edges, summary) without any intermediate graphs, embeddings, vectors, or raw triples.
    """
    __tablename__ = "semantic_graphs"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, unique=True)
    graph = Column(JSONB, nullable=False)  # entire Phase-3 output: {nodes, edges, summary}
    node_count = Column(Integer, nullable=False)  # len(graph["nodes"]) at write time
    edge_count = Column(Integer, nullable=False)  # len(graph["edges"]) at write time
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, nullable=True)


class Hypothesis(Base):
    __tablename__ = "hypotheses"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    source = Column(String, nullable=False)
    target = Column(String, nullable=False)
    path = Column(JSONB, nullable=False)  # ordered array of node texts
    predicates = Column(JSONB, nullable=False)  # array of predicates along the path (evidence labels)
    explanation = Column(Text, nullable=False)
    confidence = Column(Integer, nullable=False)
    mode = Column(String, nullable=False)  # 'explore' or 'query'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    # Nullable FK to reasoning_queries.id — NULL for explore-mode hypotheses
    query_id = Column(Integer, ForeignKey("reasoning_queries.id"), nullable=True, index=False)


class ReasoningQuery(Base):
    __tablename__ = "reasoning_queries"

    id = Column(Integer, primary_key=True)
    job_id = Column(Integer, ForeignKey("jobs.id"), nullable=False, index=True)
    query_text = Column(Text, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
