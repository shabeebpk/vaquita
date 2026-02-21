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
    """Core classification labels for Design 2.0 Ignition."""
    RESEARCH_SEED = "RESEARCH_SEED"
    EVIDENCE_INPUT = "EVIDENCE_INPUT"
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


class TextClassifier:
    """
    Pure LLM-based text classifier.
    
    Responsibilities:
    1. Send user text (or preview) to LLM with strict classification prompt.
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

    def _wrap_large_text(self, text: str) -> str:
        """
        Create a metadata wrapper for large inputs to save tokens.
        
        If text exceeds threshold, send a snippet + count to the LLM.
        """
        from app.config.admin_policy import admin_policy
        threshold = int(admin_policy.input_processing.classification_threshold)
        snippet_len = int(admin_policy.input_processing.preview_snippet_length)
        
        if len(text) <= threshold:
            return text
            
        preview = text[:snippet_len]
        tail = text[-100:] if len(text) > snippet_len + 100 else ""
        
        wrapper = (
            f"[LARGE INPUT PREVIEW]\n"
            f"Total characters: {len(text)}\n"
            f"Snippet: {preview}...\n"
            f"End of input: ...{tail}\n"
            f"[END PREVIEW]"
        )
        logger.debug(f"Created metadata wrapper for input of length {len(text)}")
        return wrapper

    def _extract_json(self, text: str) -> str:
        """Robustly extract JSON from a potentially messy LLM response."""
        text = text.strip()
        
        # 1. Look for JSON code blocks
        if "```json" in text:
            try:
                content = text.split("```json")[1].split("```")[0].strip()
                if content:
                    return content
            except IndexError:
                pass
        elif "```" in text:
            try:
                content = text.split("```")[1].split("```")[0].strip()
                if content:
                    return content
            except IndexError:
                pass

        # 2. Fallback: Find the first '{' and last '}'
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1:
            return text[start:end+1]
            
        return text

    def _repair_json(self, text: str) -> str:
        """Attempt to repair common LLM JSON mistakes (like unquoted keys)."""
        import re
        if not text:
            return text
            
        # 0. Ensure outer braces exist if we see a label
        if "label" in text and not text.strip().startswith("{"):
            text = "{" + text.strip() + "}"
            
        # 1. Wrap unquoted keys in double quotes. 
        # Pattern matches: marker({ or , or whitespace or start), then a word(\w+), then a colon(:)
        # We use a lookbehind/lookahead style approach for robustness
        repaired = re.sub(r'([{,\s])(\w+)(\s*:)', r'\1"\2"\3', text)
        
        # 2. Fix trailing commas in objects/arrays (common LLM mistake)
        repaired = re.sub(r',\s*([}\]])', r'\1', repaired)
        
        return repaired

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

        # 1. Classification via LLM (using preview for large inputs)
        response_text = None
        cleaned_json = None
        try:
            from app.config.admin_policy import admin_policy
            prompt_template = load_prompt(admin_policy.prompt_assets.user_text_classifier)
            
            # Dynamic Domain Injection
            allowed_domains = admin_policy.algorithm.domain_resolution.allowed_domains
            domain_hints = " | ".join([f'"{d}"' for d in allowed_domains]) + ' | null'
            
            wrapped_text = self._wrap_large_text(text)
            
            # Use .replace() instead of .format() to avoid KeyError on JSON braces in prompt
            prompt = prompt_template.replace("{text}", wrapped_text).replace("{allowed_domains}", domain_hints)
            
            response_text = self.llm.generate(prompt).strip()
            logger.info(f"LLM Raw Response: {response_text}")
            
            # Robust JSON extraction
            cleaned_json = self._extract_json(response_text)
            
            try:
                parsed = json.loads(cleaned_json)
            except json.JSONDecodeError:
                # Attempt Repair
                logger.info("JSON parsing failed. Attempting repair...")
                repaired = self._repair_json(cleaned_json)
                logger.debug(f"Repaired JSON: {repaired}")
                parsed = json.loads(repaired)
            
            label_name = parsed.get("label", "CONVERSATIONAL").upper()
            payload = parsed.get("payload", {})
            
            # Critical: Restore full original text to payload for data integrity
            # EVIDENCE_INPUT now returns raw_text: null to force this.
            if "raw_text" not in payload or payload["raw_text"] is None or len(str(payload["raw_text"])) < len(text):
                payload["raw_text"] = text
            
            # Validate label
            try:
                label = ClassificationLabel(label_name)
            except ValueError:
                logger.error(f"LLM returned invalid label: {label_name}. Falling back.")
                return self._get_fallback_result(text, "Invalid label")

        except Exception as e:
            logger.error(f"LLM classification or parsing failed: {e}")
            logger.debug(f"Raw LLM response was: {response_text or '[EMPTY]'}")
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
