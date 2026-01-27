"""Registry for input classifier handlers."""
import logging
from typing import Dict, Type, Optional
from app.input.handlers.base import ClassifierHandler

logger = logging.getLogger(__name__)

class HandlerRegistry:
    """Manages the lifecycle and lookup of classifier handlers."""
    
    _handlers: Dict[str, Type[ClassifierHandler]] = {}

    @classmethod
    def register(cls, label: str, handler_class: Type[ClassifierHandler]):
        """Register a handler for a specific classification label."""
        cls._handlers[label.upper()] = handler_class
        logger.debug(f"Registered classifier handler for label: {label.upper()}")

    @classmethod
    def get_handler(cls, label: str) -> Optional[Type[ClassifierHandler]]:
        """Retrieve the handler class for a label."""
        return cls._handlers.get(label.upper())

    @classmethod
    def all_labels(cls) -> set:
        """Get all registered labels."""
        return set(cls._handlers.keys())


def register_classifier_handler(label: str):
    """Decorator for easy handler registration."""
    def decorator(cls: Type[ClassifierHandler]):
        HandlerRegistry.register(label, cls)
        return cls
    return decorator


def get_handler_for_label(label: str) -> Optional[Type[ClassifierHandler]]:
    """Helper to fetch handler class by label."""
    return HandlerRegistry.get_handler(label)
