"""Decision Handlers: Control Layer for Phase-5 Decisions.

Handlers translate decision outputs into actions without adding reasoning, graph,
or filtering logic. Each handler is small, single-purpose, and idempotent per job.

This layer is the control-plane; the pipeline (Phase-1 to Phase-5) remains unchanged.
"""

from app.decision.handlers.registry import get_handler_for_decision, get_global_registry
from app.decision.handlers.controller import HandlerController, get_handler_controller

# Import all handler modules to trigger registration
import app.decision.handlers.halt_confident
import app.decision.handlers.halt_no_hypothesis
import app.decision.handlers.fetch_more_literature
import app.decision.handlers.insufficient_signal
import app.decision.handlers.strategic_download
import app.decision.handlers.verification_found
import app.decision.handlers.verification_not_found

__all__ = [
    "get_handler_for_decision",
    "get_global_registry",
    "HandlerController",
    "get_handler_controller",
]
