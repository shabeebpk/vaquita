"""
Persistence layer for Phase-3 semantic graphs with versioning support.

Handles reading and writing SemanticGraph records with versioning:
- Only one is_active=TRUE per job (single-active-state)
- All versions kept for audit trail
- Old versions marked is_active=FALSE
"""
import logging
from datetime import datetime
from sqlalchemy.orm import Session

from app.storage.models import SemanticGraph
from app.storage.db import engine

logger = logging.getLogger(__name__)


def persist_semantic_graph(job_id: int, semantic_graph: dict) -> SemanticGraph:
    """
    Persist Phase-3 semantic graph with versioning.
    
    Deactivates old graph, inserts new as is_active=TRUE.
    
    Args:
        job_id: The job ID.
        semantic_graph: Complete Phase-3 output dict.
    
    Returns:
        SemanticGraph ORM instance.
    
    Raises:
        ValueError: If semantic_graph invalid.
    """
    if not isinstance(semantic_graph, dict):
        raise ValueError(f"semantic_graph must be dict, got {type(semantic_graph)}")
    
    if "nodes" not in semantic_graph or "edges" not in semantic_graph:
        raise ValueError("semantic_graph must contain 'nodes' and 'edges' keys")
    
    node_count = len(semantic_graph.get("nodes", []))
    edge_count = len(semantic_graph.get("edges", []))
    
    with Session(engine) as session:
        try:
            existing = session.query(SemanticGraph).filter(
                SemanticGraph.job_id == job_id,
                SemanticGraph.is_active == True
            ).first()
            
            next_version = 1
            if existing:
                existing.is_active = False
                session.flush()
                next_version = existing.version + 1
                logger.info(f"Deactivated semantic graph v{existing.version} for job {job_id}")
            
            record = SemanticGraph(
                job_id=job_id,
                graph=semantic_graph,
                node_count=node_count,
                edge_count=edge_count,
                version=next_version,
                is_active=True,
                created_at=datetime.utcnow(),
            )
            session.add(record)
            session.commit()
            
            logger.info(
                f"Persisted semantic graph v{next_version} for job {job_id}: "
                f"node_count={node_count}, edge_count={edge_count}"
            )
            return record
        except Exception as e:
            session.rollback()
            msg = f"Failed to persist semantic graph for job {job_id}: {e}"
            logger.error(msg)
            raise ValueError(msg) from e


def get_semantic_graph(job_id: int, version: int = None) -> dict | None:
    """
    Retrieve semantic graph for a job.
    
    By default, retrieves active version. If version specified, retrieves that version.
    
    Args:
        job_id: The job ID.
        version: Optional specific version (for audit/rollback).
    
    Returns:
        The semantic_graph dict, or None if not found.
    """
    with Session(engine) as session:
        query = session.query(SemanticGraph).filter(SemanticGraph.job_id == job_id)
        
        if version is None:
            query = query.filter(SemanticGraph.is_active == True)
        else:
            query = query.filter(SemanticGraph.version == version)
        
        record = query.first()
        
        if record:
            logger.debug(f"Retrieved semantic graph v{record.version} for job {job_id}")
            return record.graph
        
        logger.debug(f"No semantic graph found for job {job_id}")
        return None


def get_active_semantic_version(job_id: int) -> int | None:
    """Return active semantic graph version number for job, or None if not found."""
    with Session(engine) as session:
        record = session.query(SemanticGraph.version).filter(
            SemanticGraph.job_id == job_id,
            SemanticGraph.is_active == True
        ).first()
        if record:
            return int(record[0])
        return None


def get_semantic_graph_record(job_id: int, version: int | None = None):
    """Return the ORM record for semantic graph (active by default)."""
    with Session(engine) as session:
        query = session.query(SemanticGraph).filter(SemanticGraph.job_id == job_id)
        if version is None:
            query = query.filter(SemanticGraph.is_active == True)
        else:
            query = query.filter(SemanticGraph.version == version)
        return query.first()


def delete_semantic_graph(job_id: int) -> bool:
    """
    Deactivate all semantic graphs (soft delete for versioning).
    
    Args:
        job_id: The job ID.
    
    Returns:
        True if deactivated, False if not found.
    """
    with Session(engine) as session:
        records = session.query(SemanticGraph).filter(
            SemanticGraph.job_id == job_id,
            SemanticGraph.is_active == True
        ).all()
        
        if records:
            for record in records:
                record.is_active = False
            session.commit()
            logger.info(f"Deactivated semantic graph(s) for job {job_id}")
            return True
        
        logger.debug(f"No active semantic graph found for job {job_id}")
        return False
