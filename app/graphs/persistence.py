"""
Persistence layer for Phase-3 semantic graphs.

Handles reading and writing SemanticGraph records to the database
with proper validation and artifact isolation.
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError

from app.storage.models import SemanticGraph
from app.storage.db import engine

logger = logging.getLogger(__name__)


def persist_semantic_graph(job_id: int, semantic_graph: dict) -> SemanticGraph:
    """
    Persist the Phase-3 semantic graph output to the database.
    
    Args:
        job_id: The job ID this graph belongs to.
        semantic_graph: The complete Phase-3 output dict with nodes, edges, summary.
    
    Returns:
        SemanticGraph ORM model instance.
    
    Raises:
        ValueError: If semantic_graph is missing required keys.
        IntegrityError: If a semantic graph already exists for this job.
    """
    # Validate structure
    if not isinstance(semantic_graph, dict):
        raise ValueError(f"semantic_graph must be dict, got {type(semantic_graph)}")
    
    if "nodes" not in semantic_graph or "edges" not in semantic_graph:
        raise ValueError("semantic_graph must contain 'nodes' and 'edges' keys")
    
    node_count = len(semantic_graph.get("nodes", []))
    edge_count = len(semantic_graph.get("edges", []))
    
    with Session(engine) as session:
        try:
            record = SemanticGraph(
                job_id=job_id,
                graph=semantic_graph,  # stored as JSONB unchanged
                node_count=node_count,
                edge_count=edge_count,
                created_at=datetime.utcnow(),
            )
            session.add(record)
            session.commit()
            
            logger.info(
                f"Persisted semantic graph for job {job_id}: "
                f"node_count={node_count}, edge_count={edge_count}"
            )
            return record
        except IntegrityError as e:
            session.rollback()
            msg = f"Semantic graph already exists for job {job_id}"
            logger.error(msg)
            raise IntegrityError(msg, "", "", "") from e


def get_semantic_graph(job_id: int) -> dict | None:
    """
    Retrieve a persisted semantic graph by job_id.
    
    Args:
        job_id: The job ID.
    
    Returns:
        The semantic_graph dict (nodes, edges, summary), or None if not found.
    """
    with Session(engine) as session:
        record = session.query(SemanticGraph).filter(
            SemanticGraph.job_id == job_id
        ).first()
        
        if record:
            logger.debug(f"Retrieved semantic graph for job {job_id}")
            return record.graph
        
        logger.debug(f"No semantic graph found for job {job_id}")
        return None


def delete_semantic_graph(job_id: int) -> bool:
    """
    Delete a persisted semantic graph by job_id (cleanup/rebuild scenarios).
    
    Args:
        job_id: The job ID.
    
    Returns:
        True if a record was deleted, False if not found.
    """
    with Session(engine) as session:
        record = session.query(SemanticGraph).filter(
            SemanticGraph.job_id == job_id
        ).first()
        
        if record:
            session.delete(record)
            session.commit()
            logger.info(f"Deleted semantic graph for job {job_id}")
            return True
        
        logger.debug(f"No semantic graph found to delete for job {job_id}")
        return False
