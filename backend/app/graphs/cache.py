"""Redis-backed cache for interim graph artifacts.

This cache stores Phase-2 structural graph results in Redis, allowing
different Celery worker processes to share the graph data.
"""
import json
import logging
from typing import Any, Optional
import redis

from app.config.system_settings import system_settings
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)

# Initialize synchronous Redis client
try:
    _redis_client = redis.from_url(system_settings.REDIS_URL)
    _redis_client.ping()
    logger.info("Structural graph cache connected to Redis.")
except Exception as e:
    logger.error(f"Failed to connect to Redis for structural graph cache: {e}")
    # Fallback to a dictionary for local testing if Redis is missing, 
    # though this will fail in multi-process worker environments.
    _redis_client = None
    _local_fallback = {}

# Load config from admin_policy
_CACHE_PREFIX = admin_policy.caching.redis.prefix
_DEFAULT_TTL = admin_policy.caching.redis.ttl_seconds

def set_structural_graph(job_id: int, value: Any) -> None:
    """Store the structural graph in Redis with a TTL."""
    key = f"{_CACHE_PREFIX}{job_id}"
    try:
        if _redis_client:
            # Serialize to JSON for Redis storage
            serialized = json.dumps(value)
            _redis_client.set(key, serialized, ex=_DEFAULT_TTL)
            logger.debug(f"Stored structural graph in Redis for job {job_id}")
        else:
            _local_fallback[int(job_id)] = value
    except Exception as e:
        logger.error(f"Error setting structural graph in cache for job {job_id}: {e}")

def get_structural_graph(job_id: int) -> Optional[Any]:
    """Retrieve the structural graph from Redis."""
    key = f"{_CACHE_PREFIX}{job_id}"
    try:
        if _redis_client:
            data = _redis_client.get(key)
            if data:
                return json.loads(data)
            return None
        else:
            return _local_fallback.get(int(job_id))
    except Exception as e:
        logger.error(f"Error getting structural graph from cache for job {job_id}: {e}")
        return None

def delete_structural_graph(job_id: int) -> None:
    """Delete the structural graph from Redis."""
    key = f"{_CACHE_PREFIX}{job_id}"
    try:
        if _redis_client:
            _redis_client.delete(key)
            logger.debug(f"Deleted structural graph from Redis for job {job_id}")
        else:
            _local_fallback.pop(int(job_id), None)
    except Exception as e:
        logger.error(f"Error deleting structural graph from cache for job {job_id}: {e}")
