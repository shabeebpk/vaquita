"""
Text Classifier: Categorize user input as content, intent, greeting, or mixed.

This module provides flexible classification of user messages to determine
whether they represent document content to be ingested, user intents/commands,
greetings, or a combination of content and intent.

The classifier can run in two modes:
1. Deterministic: Uses regex and heuristic rules (fast, no LLM calls)
2. LLM mode: Uses a language model for semantic understanding (slower, more accurate)

This separation ensures the system can work without LLM access and gracefully
degrade to deterministic classification if LLM is unavailable.
"""

import re
import logging
import os
from typing import Literal, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class ClassificationLabel(str, Enum):
    """Classification labels for user input."""
    CONTENT = "content"
    INTENT = "intent"
    GREETING = "greeting"
    MIXED = "mixed"  # content + intent or content + greeting


class ClassificationResult:
    """Result of classifying user input."""
    
    def __init__(
        self,
        label: ClassificationLabel,
        confidence: float,
        content_portion: Optional[str] = None,
        intent_portion: Optional[str] = None,
        reasoning: Optional[str] = None,
    ):
        """
        Args:
            label: Primary classification label
            confidence: Confidence score 0.0-1.0
            content_portion: If mixed, the content portion of the input
            intent_portion: If mixed, the intent/command portion
            reasoning: Explanation of the classification
        """
        self.label = label
        self.confidence = confidence
        self.content_portion = content_portion
        self.intent_portion = intent_portion
        self.reasoning = reasoning or ""
    
    def is_content_available(self) -> bool:
        """True if this input contains content to ingest."""
        return self.label in (ClassificationLabel.CONTENT, ClassificationLabel.MIXED)
    
    def get_content_text(self) -> str:
        """
        Get the text to ingest.
        
        For pure content, returns the entire input.
        For mixed, returns only the content portion.
        For intent/greeting, returns empty string.
        """
        if self.label == ClassificationLabel.CONTENT:
            return ""  # Caller already has the full text
        elif self.label == ClassificationLabel.MIXED:
            return self.content_portion or ""
        else:
            return ""


class TextClassifier:
    """
    Classify user input to determine next action in the pipeline.
    
    Supports two modes:
    1. Deterministic (heuristic-based) — fast, no LLM calls
    2. LLM-based — more accurate semantic understanding
    
    Mode is controlled by TEXT_CLASSIFIER_MODE environment variable.
    """
    
    def __init__(self, mode: Optional[Literal["deterministic", "llm"]] = None):
        """
        Initialize the classifier.
        
        Args:
            mode: "deterministic" for heuristic rules, "llm" for LLM-based classification.
                  If None, reads from TEXT_CLASSIFIER_MODE environment variable (default: deterministic).
        """
        if mode is None:
            mode = os.getenv("TEXT_CLASSIFIER_MODE", "deterministic").lower()
        
        self.mode = mode
        
        if mode == "llm":
            from app.llm.service import LLMService
            self.llm = LLMService()
        else:
            self.llm = None
        
        logger.info(f"TextClassifier initialized: mode={self.mode}")
    
    def classify(self, text: str) -> ClassificationResult:
        """
        Classify user input text.
        
        Args:
            text: Raw user input text
        
        Returns:
            ClassificationResult with label, confidence, and optional portions
        """
        if self.mode == "llm":
            return self._classify_llm(text)
        else:
            return self._classify_deterministic(text)
    
    # ========================================================================
    # Deterministic Classification (Heuristics & Regex)
    # ========================================================================
    
    def _classify_deterministic(self, text: str) -> ClassificationResult:
        """
        Classify using deterministic rules and heuristics.
        
        Strategy:
        1. Check for greeting patterns (hello, hi, thanks, etc.)
        2. Check for intent patterns (find papers, show me, fetch, etc.)
        3. Check for content indicators (paragraph length, academic terms, etc.)
        4. Return appropriate label and confidence
        """
        text_lower = text.lower().strip()
        
        # Check for greetings
        if self._is_greeting(text_lower):
            return ClassificationResult(
                label=ClassificationLabel.GREETING,
                confidence=0.95,
                reasoning="Detected greeting pattern (hello, hi, thanks, etc.)"
            )
        
        # Check for intent/command patterns
        intent_label, intent_confidence, intent_text = self._extract_intent(text)
        content_text, content_confidence = self._extract_content(text)
        
        # Decide on label based on detected portions
        if intent_text and content_text:
            # Mixed: has both intent and content
            return ClassificationResult(
                label=ClassificationLabel.MIXED,
                confidence=min(intent_confidence, content_confidence),
                content_portion=content_text,
                intent_portion=intent_text,
                reasoning=f"Detected both intent (confidence {intent_confidence:.2f}) and content (confidence {content_confidence:.2f})"
            )
        elif intent_text:
            # Pure intent/command
            return ClassificationResult(
                label=intent_label,
                confidence=intent_confidence,
                reasoning=f"Detected intent pattern: '{intent_text}'"
            )
        elif content_text:
            # Pure content
            return ClassificationResult(
                label=ClassificationLabel.CONTENT,
                confidence=content_confidence,
                reasoning="Classified as content (length, structure, academic terms)"
            )
        else:
            # Fallback: assume content if no clear patterns match
            return ClassificationResult(
                label=ClassificationLabel.CONTENT,
                confidence=0.5,
                reasoning="No clear patterns matched; defaulting to content"
            )
    
    def _is_greeting(self, text_lower: str) -> bool:
        """Check if text is a greeting."""
        greeting_patterns = [
            r"^(hello|hi|hey|greetings|good morning|good afternoon|good evening)",
            r"^(thanks|thank you|appreciate it)",
            r"^(welcome|nice to meet|glad to)",
            r"^(how are you|how do you do)",
        ]
        for pattern in greeting_patterns:
            if re.search(pattern, text_lower):
                return True
        return False
    
    def _extract_intent(self, text: str) -> tuple[str, float, Optional[str]]:
        """
        Extract intent/command from text.
        
        Returns:
            (label, confidence, intent_text) where intent_text is the matched intent portion
        """
        text_lower = text.lower()
        
        intent_patterns = [
            # Find/fetch papers
            (r"(find|search for|show me|fetch|get|retrieve|look for)\s+(?:papers|articles|studies)?(?:\s+(?:about|on|regarding|related to)\s+)?([\w\s]+?)(?:\.|$)", ClassificationLabel.INTENT),
            # Analyze/summarize
            (r"(analyze|summarize|summarise|explain|interpret|discuss)\s+(this|these|that|the)?\s*([\w\s]+?)(?:\.|$)", ClassificationLabel.INTENT),
            # Ask questions
            (r"^(what|how|why|when|where|who|which)\s+", ClassificationLabel.INTENT),
            # Help/clarification
            (r"(help|assist|clarify|explain)\s+(me|us)?(?:\s+with)?\s+([\w\s]*)?", ClassificationLabel.INTENT),
        ]
        
        for pattern, label in intent_patterns:
            match = re.search(pattern, text_lower)
            if match:
                matched_text = match.group(0)
                return (label.value, 0.85, matched_text)
        
        return (ClassificationLabel.INTENT.value, 0.0, None)
    
    def _extract_content(self, text: str) -> tuple[Optional[str], float]:
        """
        Detect if text looks like document content to be ingested.
        
        Returns:
            (content_text, confidence)
        
        Heuristics:
        - Long paragraphs (> 100 chars) suggest content
        - Multiple sentences suggest content
        - Academic terms, citations increase confidence
        - Short, command-like text suggests intent
        """
        # If text is very short, probably not content
        if len(text.strip()) < 50:
            return (None, 0.0)
        
        # Multiple sentences suggest content
        sentences = re.split(r'[.!?]+', text)
        if len(sentences) < 2:
            return (None, 0.3)
        
        # Check for academic/content indicators
        academic_terms = [
            r'\b(abstract|introduction|methodology|conclusion|reference|citation|'\
            r'hypothesis|research|experiment|analysis|data|finding|result)\b',
            r'\b(et al|©|©|\d{4})\b',  # Author lists, copyright, years
            r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\s',  # Proper names (authors)
        ]
        
        academic_score = 0
        for pattern in academic_terms:
            if re.search(pattern, text, re.IGNORECASE):
                academic_score += 1
        
        # Confidence: base on length, sentence count, and academic signals
        # - Length > 200 chars: strong signal (0.7)
        # - Multiple sentences: strong signal (0.6+)
        # - Each academic term: +0.15
        base_confidence = 0.6 if len(text.strip()) > 200 else 0.5
        
        # Add confidence for number of sentences (more sentences = more likely content)
        sentence_bonus = min(0.3, (len(sentences) - 2) * 0.05)
        academic_bonus = academic_score * 0.15
        
        final_confidence = base_confidence + sentence_bonus + academic_bonus
        
        return (text, min(1.0, final_confidence))
    
    # ========================================================================
    # LLM-Based Classification
    # ========================================================================
    
    def _classify_llm(self, text: str) -> ClassificationResult:
        """
        Classify using LLM semantic understanding.
        
        This mode provides more accurate classification but requires LLM access.
        Falls back to deterministic mode if LLM fails.
        """
        if not self.llm:
            logger.warning("LLM mode selected but LLMService not available; falling back to deterministic")
            return self._classify_deterministic(text)
        
        prompt = self._build_classification_prompt(text)
        
        try:
            response = self.llm.generate(prompt)
            return self._parse_llm_response(text, response)
        except Exception as e:
            logger.error(f"LLM classification failed: {e}; falling back to deterministic")
            return self._classify_deterministic(text)
    
    def _build_classification_prompt(self, text: str) -> str:
        """Build prompt for LLM classification using loaded template."""
        from app.prompts.loader import load_prompt
        
        template = load_prompt("text_classifier.txt")
        return template.format(text=text)
    
    def _parse_llm_response(self, original_text: str, response: str) -> ClassificationResult:
        """Parse LLM response into ClassificationResult."""
        try:
            lines = response.strip().split('\n')
            
            label_str = ""
            confidence = 50
            content_portion = None
            intent_portion = None
            
            for line in lines:
                if line.startswith("LABEL:"):
                    label_str = line.replace("LABEL:", "").strip().upper()
                elif line.startswith("CONFIDENCE:"):
                    try:
                        confidence = int(line.replace("CONFIDENCE:", "").strip())
                    except ValueError:
                        confidence = 50
                elif line.startswith("CONTENT_PORTION:"):
                    content_portion = line.replace("CONTENT_PORTION:", "").strip() or None
                elif line.startswith("INTENT_PORTION:"):
                    intent_portion = line.replace("INTENT_PORTION:", "").strip() or None
            
            # Map string label to enum
            if label_str == "CONTENT":
                label = ClassificationLabel.CONTENT
            elif label_str == "INTENT":
                label = ClassificationLabel.INTENT
            elif label_str == "GREETING":
                label = ClassificationLabel.GREETING
            elif label_str == "MIXED":
                label = ClassificationLabel.MIXED
            else:
                label = ClassificationLabel.CONTENT  # Fallback
            
            return ClassificationResult(
                label=label,
                confidence=confidence / 100.0,
                content_portion=content_portion,
                intent_portion=intent_portion,
                reasoning=f"LLM classification: {response}"
            )
        except Exception as e:
            logger.error(f"Failed to parse LLM response: {e}")
            return self._classify_deterministic(original_text)


# Global singleton instance
_classifier_instance: Optional[TextClassifier] = None


def get_classifier(mode: Optional[Literal["deterministic", "llm"]] = None) -> TextClassifier:
    """
    Get or create the global TextClassifier instance.
    
    Args:
        mode: "deterministic" (default, fast) or "llm" (more accurate).
              If None, reads from TEXT_CLASSIFIER_MODE environment variable.
    
    Returns:
        TextClassifier instance
    """
    global _classifier_instance
    if _classifier_instance is None:
        _classifier_instance = TextClassifier(mode=mode)
    return _classifier_instance
