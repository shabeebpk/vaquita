"""Sentence-Transformers embedding provider (default for Phase-3)."""
import logging
from typing import List
import numpy as np

from app.embeddings.interface import EmbeddingProvider

logger = logging.getLogger(__name__)


class SentenceTransformerProvider(EmbeddingProvider):
    """Embedding provider using sentence-transformers library."""

    def __init__(self, model_name: str = "all-MiniLM-L6-v2", batch_size: int = 32, device: str = "cpu"):
        """Initialize the provider.

        Args:
            model_name: HuggingFace model identifier
            batch_size: batch size for encoding
            device: "cpu" or "cuda"
        """
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as e:
            raise RuntimeError(
                "sentence-transformers is required for semantic merging. "
                "Install it with: pip install sentence-transformers"
            ) from e

        self.model_name = model_name
        self.batch_size = batch_size
        self.device = device

        try:
            self.model = SentenceTransformer(model_name, device=device)
            logger.info(f"Loaded SentenceTransformer model: {model_name} on device {device}")
        except Exception as e:
            logger.error(f"Failed to load SentenceTransformer model {model_name}: {e}")
            raise

    def embed(self, texts: List[str]) -> np.ndarray:
        """Embed texts using sentence-transformers.

        Returns normalized vectors (unit L2 norm).
        """
        if not texts:
            return np.array([]).reshape(0, self.get_dimension())

        embeddings = self.model.encode(
            texts,
            batch_size=self.batch_size,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )

        return embeddings

    def get_dimension(self) -> int:
        """Return embedding dimension."""
        return self.model.get_sentence_embedding_dimension()

    def get_name(self) -> str:
        """Return provider name."""
        return "sentence-transformers"
