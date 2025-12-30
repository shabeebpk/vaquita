"""Embedding adapter layer for Phase-3 semantic merging.

This package abstracts embedding models behind a clean provider interface.
Providers can be swapped without changing Phase-3 logic.
"""

from .interface import EmbeddingProvider

__all__ = ["EmbeddingProvider"]
