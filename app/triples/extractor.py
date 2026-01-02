"""Provider-agnostic TripleExtractor.

Public API:
  extractor = TripleExtractor()
  result = extractor.extract(block_text)  # -> dict or None

Strict contract: returns either a dict matching {"triples": [{subject,predicate,object}]} or None.

Uses the global LLM service (app.llm.service) for all LLM calls.
Loads prompts via the centralized prompt loader (app.prompts.loader).
No provider-specific imports or logic here.
"""
import os
import json
import logging
from typing import Optional

from app.llm import get_llm_service
from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

# Fallback template if prompt file is missing
TRIPLE_EXTRACTION_FALLBACK = """{block_text}"""


class TripleExtractor:
    def __init__(self, provider_name: Optional[str] = None):
        # provider_name is kept for backward compatibility but ignored
        # All LLM calls go through the global service now
        self.llm_service = get_llm_service()
        # Load prompt template using the centralized loader
        self.prompt_template = load_prompt(
            "triple_extraction.txt",
            fallback=TRIPLE_EXTRACTION_FALLBACK
        )

    def _build_prompt(self, block_text: str) -> str:
        return self.prompt_template.replace("{block_text}", block_text)

    def _validate(self, parsed) -> bool:
        if not isinstance(parsed, dict):
            return False
        if "triples" not in parsed:
            return False
        triples = parsed["triples"]
        if not isinstance(triples, list):
            return False
        for item in triples:
            if not isinstance(item, dict):
                return False
            keys = set(item.keys())
            if keys != {"subject", "predicate", "object"}:
                return False
            for k in ("subject", "predicate", "object"):
                v = item.get(k)
                if not isinstance(v, str) or not v.strip():
                    return False
        return True

    def extract(self, block_text: str) -> Optional[dict]:
        if not isinstance(block_text, str):
            logger.debug("extract called with non-string")
            return None

        prompt = self._build_prompt(block_text)
        try:
            raw = self.llm_service.generate(prompt)
            logger.info(f"returned raw : {raw}")
        except Exception as e:
            logger.error("LLM call failed: %s", e)
            return None

        if not raw or not isinstance(raw, str):
            logger.debug("LLM returned empty/non-str response")
            return None

        try:
            parsed = json.loads(raw)
        except Exception as e:
            logger.warning("Failed to parse JSON from LLM response: %s", e)
            logger.debug("Raw response: %s", raw)
            return None

        if not self._validate(parsed):
            logger.warning("Invalid triple schema from LLM")
            logger.debug("Parsed: %s", parsed)
            return None

        return parsed
