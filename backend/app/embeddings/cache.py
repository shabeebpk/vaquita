"""Embedding cache for incremental semantic merging.

Caches node text -> embedding vector to avoid re-embedding unchanged nodes.
Uses Redis for distributed cache, falls back to in-memory dict.
"""
import logging
from typing import Dict, Optional, List
import numpy as np

from app.config.system_settings import system_settings

logger = logging.getLogger(__name__)

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


class EmbeddingCache:
    """Cache for node embeddings to avoid re-embedding."""
    
    def __init__(self, job_id: int):
        self.job_id = job_id
        self.cache_key = f"job:{job_id}:embeddings"
        self.redis_client = None
        self.memory_cache: Dict[str, np.ndarray] = {}
        
        if REDIS_AVAILABLE:
            try:
                self.redis_client = redis.from_url(system_settings.REDIS_URL)
            except Exception as e:
                logger.warning(f"Redis unavailable for embedding cache: {e}, using memory cache")
    
    async def get(self, node_text: str) -> Optional[np.ndarray]:
        """Get cached embedding for a node."""
        if node_text in self.memory_cache:
            return self.memory_cache[node_text]
        
        if self.redis_client:
            try:
                cached = await self.redis_client.hget(self.cache_key, node_text)
                if cached:
                    vector = np.frombuffer(cached, dtype=np.float32)
                    self.memory_cache[node_text] = vector
                    return vector
            except Exception as e:
                logger.debug(f"Redis cache get failed: {e}")
        
        return None
    
    async def set(self, node_text: str, embedding: np.ndarray) -> None:
        """Cache an embedding vector."""
        self.memory_cache[node_text] = embedding
        
        if self.redis_client:
            try:
                await self.redis_client.hset(
                    self.cache_key,
                    node_text,
                    embedding.astype(np.float32).tobytes()
                )
                await self.redis_client.expire(self.cache_key, 30 * 24 * 3600)
            except Exception as e:
                logger.debug(f"Redis cache set failed: {e}")
    
    async def get_multiple(self, node_texts: List[str]) -> Dict[str, np.ndarray]:
        """Get multiple cached embeddings."""
        cached = {}
        for text in node_texts:
            embedding = await self.get(text)
            if embedding is not None:
                cached[text] = embedding
        return cached


def get_embedding_cache(job_id: int) -> EmbeddingCache:
    """Get or create embedding cache for job."""
    return EmbeddingCache(job_id)
