import logging
import time
from typing import List, Optional, Callable
from app.llm import get_llm_service
from app.prompts.loader import load_prompt
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)

class TextRefineryService:
    """
    Cleans raw extraction text via LLM.
    
    Features:
    - Streaming response handling.
    - Line-by-line validation and persistence.
    - Automatic recovery and retry for incomplete sentences.
    - Admin-driven prompt and model configuration.
    """

    def __init__(self):
        self.llm_service = get_llm_service()
        self.config = admin_policy.algorithm.refinery
        self.prompt_template = load_prompt(self.config.prompt_asset)

    def refine_text(self, raw_text: str, on_line_confirmed: Optional[Callable[[str], None]] = None) -> str:
        """
        Refine text, using recursive span chunking for large inputs.
        
        Args:
            raw_text: Messy text from extraction.
            on_line_confirmed: Callback to persist each confirmed clean line.
            
        Returns:
            Fully refined text.
        """
        if not raw_text.strip():
            return ""

        # Recursive Span Strategy: 
        # Break large text into logical 'bites' that fit model response limits.
        # Approx 3 chars per token for scientific text + safety margin.
        max_chars = self.config.max_tokens_per_span * 3 
        
        spans = self._split_into_spans(raw_text, max_chars)
        refined_results = []

        for i, span in enumerate(spans):
            logger.info(f"TextRefinery: Processing span {i+1}/{len(spans)} ({len(span)} chars).")
            clean_span = self._refine_span(span, on_line_confirmed)
            if clean_span:
                refined_results.append(clean_span)
        
        return "\n".join(refined_results)

    def _split_into_spans(self, text: str, max_chars: int) -> List[str]:
        """Split text into manageable spans, trying to respect paragraph boundaries."""
        if len(text) <= max_chars:
            return [text]

        spans = []
        remaining = text
        while len(remaining) > max_chars:
            # Look for last double newline or newline within the window
            split_idx = remaining.rfind("\n\n", 0, max_chars)
            if split_idx == -1:
                split_idx = remaining.rfind("\n", 0, max_chars)
            
            # Emergency split at space if no newline found
            if split_idx == -1:
                split_idx = remaining.rfind(" ", 0, max_chars)
            
            # Absolute hard split if needed
            if split_idx == -1:
                split_idx = max_chars

            spans.append(remaining[:split_idx].strip())
            remaining = remaining[split_idx:].strip()
        
        if remaining:
            spans.append(remaining)
        
        return spans

    def _refine_span(self, span_text: str, on_line_confirmed: Optional[Callable[[str], None]] = None) -> str:
        """Internal helper to refine a single manageable span."""
        prompt = self.prompt_template.replace("{text}", span_text)
        
        retries = 3
        while retries > 0:
            try:
                full_response = self.llm_service.generate(
                    prompt, 
                    temperature=self.config.temperature,
                    max_tokens=self.config.max_tokens_per_span
                )
                
                if not full_response:
                    return ""
                
                logger.info(f"TextRefinery: Raw LLM Output for span:\n{full_response}\n---")
                
                lines = [l.strip() for l in full_response.split('\n') if l.strip()]
                
                # Loose Incomplete Sentence Check: 
                # Only retry if the span is large and definitely seems truncated mid-sentence.
                # If the span is short, it might be a header or a fragment.
                if len(span_text) > 200 and lines and not any(lines[-1].endswith(p) for p in [".", "?", "!", '"']):
                    logger.warning("TextRefinery: Detected likely truncation in large span. Retrying...")
                    retries -= 1
                    time.sleep(1)
                    continue

                clean_text = "\n".join(lines)
                
                # Meta-filler Scrubbing: Prevent LLM from poisoning triples with "Here is your text"
                garbage_markers = ["here is", "clean text:", "cleaned text:", "the following", "refinement:"]
                for marker in garbage_markers:
                    if clean_text.lower().startswith(marker):
                        clean_text = clean_text[clean_text.find("\n")+1:].strip() if "\n" in clean_text else ""
                
                if on_line_confirmed:
                    for line in lines:
                        on_line_confirmed(line)
                
                return clean_text

            except Exception as e:
                logger.error(f"TextRefinery: Span failed (Retries: {retries}): {e}.")
                retries -= 1
                if retries == 0:
                    raise
                time.sleep(2)
        
        return ""
