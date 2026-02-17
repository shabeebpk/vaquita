"""Decision Handlers: Control Layer for Phase-5 Decisions.

Handlers translate decision outputs into actions without adding reasoning, graph,
or filtering logic. Each handler is small, single-purpose, and idempotent per job.

This layer is the control-plane; the pipeline (Phase-1 to Phase-5) remains unchanged.
"""

from app.decision.handlers.registry import get_handler_for_decision, get_global_registry
from app.decision.handlers.controller import HandlerController, get_handler_controller

# Import all handler modules to trigger registration
from app.decision.handlers import (
    halt_confident,
    fetch_more_literature,
    ask_domain_expert,
    ask_user_input,
    insufficient_signal,
    undecided,
    strategic_download,
)

__all__ = [
    "get_handler_for_decision",
    "get_global_registry",
    "HandlerController",
    "get_handler_controller",
]
