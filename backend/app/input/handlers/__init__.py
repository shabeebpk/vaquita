"""Initialization for classifier handlers.

Importing all handlers here ensures they register themselves with the registry.
"""
from app.input.handlers.base import ClassifierHandler, ClassifierHandlerResult
from app.input.handlers.registry import get_handler_for_label
from app.input.handlers.controller import get_classifier_handler_controller

# Force registration by importing all handler modules
from app.input.handlers import (
    research_seed,
    evidence_input,
    clarification_constraint,
    expert_guidance,
    graph_query,
    conversational
)
