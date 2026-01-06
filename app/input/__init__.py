"""
Input module: Handles user input classification and routing.

This module provides the text classifier that determines whether user input
represents content to be ingested, intent/commands, greetings, or a combination.
"""

from app.input.classifier import (
    TextClassifier,
    ClassificationLabel,
    ClassificationResult,
    get_classifier,
)

__all__ = [
    "TextClassifier",
    "ClassificationLabel",
    "ClassificationResult",
    "get_classifier",
]
