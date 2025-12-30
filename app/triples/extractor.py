"""Provider-agnostic TripleExtractor.

Public API:
  extractor = TripleExtractor()
  result = extractor.extract(block_text)  # -> dict or None

Strict contract: returns either a dict matching {"triples": [{subject,predicate,object}]} or None.
"""
import os
import json
import logging
from typing import Optional

from .providers import get_adapter

logger = logging.getLogger(__name__)

BASE_DIR = os.path.dirname(os.path.dirname(__file__))
PROMPT_PATH = os.path.join(BASE_DIR, "prompts", "triple_extraction.txt")


class TripleExtractor:
    def __init__(self, provider_name: Optional[str] = None):
        self.provider_name = provider_name or os.environ.get("TRIPLE_PROVIDER", "dummy")
        self.adapter = get_adapter(self.provider_name)
        try:
            with open(PROMPT_PATH, "r", encoding="utf-8") as f:
                self.prompt_template = f.read()
        except Exception:
            # Minimal fallback
            self.prompt_template = "{block_text}"

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
            raw = self.adapter.call(prompt)
            logger.info(f"returned raw : {raw}")
        except Exception as e:
            logger.error("Adapter call failed: %s", e)
            return None

        if not raw or not isinstance(raw, str):
            logger.debug("Adapter returned empty/non-str response")
            return None

        try:
            parsed = json.loads(raw)
        except Exception as e:
            logger.warning("Failed to parse JSON from adapter response: %s", e)
            logger.debug("Raw response: %s", raw)
            return None

        if not self._validate(parsed):
            logger.warning("Invalid triple schema from adapter")
            logger.debug("Parsed: %s", parsed)
            return None

        return parsed
