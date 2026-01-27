"""Embedding provider interface for Phase-3.

All embedding backends must implement this interface.
"""
from abc import ABC, abstractmethod
from typing import List
import numpy as np


class EmbeddingProvider(ABC):
    """Abstract interface for embedding providers."""

    @abstractmethod
    def embed(self, texts: List[str]) -> np.ndarray:
        """Embed a list of text strings.

        Args:
            texts: list of strings to embed

        Returns:
            numpy array of shape (len(texts), embedding_dim)
            Normalized to unit length (L2 norm = 1.0)
        """
        pass

    @abstractmethod
    def get_dimension(self) -> int:
        """Return the embedding dimension."""
        pass

    @abstractmethod
    def get_name(self) -> str:
        """Return the provider name (e.g., 'sentence-transformers')."""
        pass
