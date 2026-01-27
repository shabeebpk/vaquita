"""Handler Registry: Declarative Mapping of Decisions to Handlers.

The registry maps decision labels (strings) to handler classes.
Adding a new decision requires only registering a new handler class.
No orchestration code changes needed.
"""

from typing import Dict, Type
from app.decision.handlers.base import Handler


class HandlerRegistry:
    """Registry of decision → handler mappings."""
    
    def __init__(self):
        """Initialize an empty registry. Register handlers via register()."""
        self._handlers: Dict[str, Type[Handler]] = {}
    
    def register(self, decision_label: str, handler_class: Type[Handler]) -> None:
        """Register a handler for a decision label.
        
        Args:
            decision_label: The decision label string (e.g., "halt_confident").
            handler_class: The handler class (subclass of Handler).
        
        Raises:
            ValueError: If the handler class does not subclass Handler.
        """
        if not issubclass(handler_class, Handler):
            raise ValueError(f"{handler_class} must subclass Handler")
        
        self._handlers[decision_label] = handler_class
    
    def get(self, decision_label: str) -> Type[Handler] | None:
        """Retrieve the handler class for a decision label.
        
        Args:
            decision_label: The decision label string.
        
        Returns:
            The handler class, or None if not registered.
        """
        return self._handlers.get(decision_label)
    
    def all_labels(self) -> set:
        """Return the set of all registered decision labels."""
        return set(self._handlers.keys())
    
    def __repr__(self) -> str:
        labels = ", ".join(sorted(self._handlers.keys()))
        return f"HandlerRegistry({labels})"


# Global registry instance
_global_registry = HandlerRegistry()


def register_handler(decision_label: str, handler_class: Type[Handler]) -> None:
    """Register a handler in the global registry.
    
    Intended for use at module load time.
    """
    _global_registry.register(decision_label, handler_class)


def get_handler_for_decision(decision_label: str) -> Type[Handler] | None:
    """Retrieve handler class from global registry."""
    return _global_registry.get(decision_label)


def get_global_registry() -> HandlerRegistry:
    """Get the global registry instance."""
    return _global_registry
