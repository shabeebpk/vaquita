"""
Text Classifier 2.0: LLM-Only Intent Classification and Handling.

This module provides a pure LLM-driven classification of user messages.
It routes inputs to specialized handlers to prepare job state.
"""

import logging
import json
import os
from typing import Dict, Any, Optional, List
from enum import Enum

from app.llm import get_llm_service
from app.prompts.loader import load_prompt
from app.input.handlers.controller import get_classifier_handler_controller, ClassifierHandlerResult

logger = logging.getLogger(__name__)


class ClassificationLabel(str, Enum):
    """Refactored classification labels per Design 2.0."""
    RESEARCH_SEED = "RESEARCH_SEED"
    EVIDENCE_INPUT = "EVIDENCE_INPUT"
    CLARIFICATION_CONSTRAINT = "CLARIFICATION_CONSTRAINT"
    EXPERT_GUIDANCE = "EXPERT_GUIDANCE"
    GRAPH_QUERY = "GRAPH_QUERY"
    CONVERSATIONAL = "CONVERSATIONAL"


class ClassificationResult:
    """Refactored classification result containing structured payload and handler outcome."""
    
    def __init__(
        self,
        label: ClassificationLabel,
        payload: Dict[str, Any],
        handler_result: Optional[ClassifierHandlerResult] = None,
        raw_llm_response: Optional[str] = None
    ):
        self.label = label
        self.payload = payload
        self.handler_result = handler_result
        self.raw_llm_response = raw_llm_response

    @property
    def confidence(self) -> float:
        """Compatibility property for legacy callers."""
        return 1.0

    def is_content_available(self) -> bool:
        """Legacy compatibility for chat.py Routing."""
        return self.label == ClassificationLabel.EVIDENCE_INPUT

    def get_content_text(self) -> str:
        """Legacy compatibility for chat.py Ingestion."""
        if self.label == ClassificationLabel.EVIDENCE_INPUT:
            return self.payload.get("raw_text", "")
        return ""


class TextClassifier:
    """
    Pure LLM-based text classifier.
    
    Responsibilities:
    1. Send user text to LLM with strict classification prompt.
    2. Parse structured JSON payload.
    3. Invoke the matching ClassifierHandler to prepare job state.
    4. Provide a safe fallback to CONVERSATIONAL on any failure.
    """
    
    def __init__(self):
        self.llm = get_llm_service()
        self.handler_controller = get_classifier_handler_controller()
        logger.info("TextClassifier 2.0 initialized (LLM-Only mode)")

    def _get_fallback_result(self, text: str, reason: str) -> ClassificationResult:
        """Create a safe CONVERSATIONAL fallback result."""
        logger.warning(f"Classification fallback invoked: {reason}")
        return ClassificationResult(
            label=ClassificationLabel.CONVERSATIONAL,
            payload={"raw_text": text}
        )

    def classify(self, text: str, job_id: Optional[int] = None, session: Any = None) -> ClassificationResult:
        """
        Classify text and trigger state handlers.
        
        Args:
            text: Raw user input.
            job_id: Job ID for handler execution.
            session: DB session for handler execution.
        """
        if not text or not text.strip():
            return self._get_fallback_result(text, "Empty input")

        # 1. Classification via LLM
        try:
            from app.config.admin_policy import admin_policy
            prompt_template = load_prompt(admin_policy.prompt_assets.user_text_classifier)
            prompt = prompt_template.format(text=text)
            
            response_text = self.llm.generate(prompt).strip()
            
            # Remove potential markdown fences if the LLM ignored instructions
            if response_text.startswith("```"):
                response_text = response_text.split("\n", 1)[1].rsplit("\n", 1)[0].strip()
            if response_text.startswith("json"): # Handle ```json variant
                response_text = response_text[4:].strip()

            parsed = json.loads(response_text)
            
            label_name = parsed.get("label", "CONVERSATIONAL").upper()
            payload = parsed.get("payload", {"raw_text": text})
            
            # Validate label
            try:
                label = ClassificationLabel(label_name)
            except ValueError:
                logger.error(f"LLM returned invalid label: {label_name}")
                return self._get_fallback_result(text, "Invalid label")

        except Exception as e:
            logger.error(f"LLM classification or parsing failed: {e}")
            return self._get_fallback_result(text, str(e))

        # 2. Handler Execution (if job_id and session provided)
        handler_outcome = None
        if job_id is not None and session is not None:
            handler_outcome = self.handler_controller.execute_handler(
                label.value, job_id, payload, session
            )

        return ClassificationResult(
            label=label,
            payload=payload,
            handler_result=handler_outcome,
            raw_llm_response=response_text
        )


def get_classifier() -> TextClassifier:
    """Get the global TextClassifier 2.0 instance."""
    return TextClassifier()
