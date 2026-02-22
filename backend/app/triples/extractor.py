"""Provider-agnostic TripleExtractor — pipe-delimited format with partial recovery.

Public API:
  extractor = TripleExtractor()
  result = extractor.extract(block_text)  # -> {"triples": [...]} or None

Format contract: LLM returns one triple per line:
  subject | predicate | object

Recovery strategy:
  - LLMs sometimes add commentary at the top/bottom of the response.
    We detect the middle block of valid triple lines and discard surrounding noise.
  - Each line is parsed independently — a bad line is dropped, not the whole block.
  - Returns None only when zero valid triples survive.
"""
import logging
from typing import Optional, List, Dict

from app.llm import get_llm_service
from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)

TRIPLE_EXTRACTION_FALLBACK = "{block_text}"

# Hard limits to catch hallucinated values
MAX_FIELD_LEN = 300


class TripleExtractor:

    def __init__(self, provider_name: Optional[str] = None):
        # provider_name kept for backward compatibility, routing is via LLMService
        self.provider_name = provider_name or "llm"
        self.llm_service = get_llm_service()

        from app.config.admin_policy import admin_policy
        self.prompt_template = load_prompt(
            admin_policy.prompt_assets.triple_extraction,
            fallback=TRIPLE_EXTRACTION_FALLBACK,
        )

    # ------------------------------------------------------------------
    # Prompt
    # ------------------------------------------------------------------

    def _build_prompt(self, block_text: str) -> str:
        return self.prompt_template.replace("{block_text}", block_text)

    # ------------------------------------------------------------------
    # Parsing — one concern per function
    # ------------------------------------------------------------------

    def _is_triple_line(self, line: str) -> bool:
        """A valid candidate triple line has exactly 2 pipe characters."""
        return line.count("|") == 2

    def _trim_comment_noise(self, lines: List[str]) -> List[str]:
        """
        LLMs occasionally wrap triples with top/bottom commentary.
        Keep only the contiguous block from the first to the last triple-like line.
        """
        flags = [self._is_triple_line(ln) for ln in lines]
        if not any(flags):
            return []
        first = next(i for i, v in enumerate(flags) if v)
        last = len(flags) - 1 - next(i for i, v in enumerate(reversed(flags)) if v)
        return lines[first : last + 1]

    def _parse_line(self, line: str) -> Optional[Dict[str, str]]:
        """Parse one pipe-delimited line. Returns None if malformed."""
        parts = line.split("|")
        if len(parts) != 3:
            return None
        subject, predicate, obj = (p.strip() for p in parts)
        if not subject or not predicate or not obj:
            return None
        # Reject obviously hallucinated / broken values
        for val in (subject, predicate, obj):
            if "\n" in val or len(val) > MAX_FIELD_LEN:
                return None
        return {"subject": subject, "predicate": predicate, "object": obj}

    def _parse_response(self, raw: str) -> List[Dict[str, str]]:
        """
        Full recovery pipeline:
        1. Split into lines, drop blanks.
        2. Trim comment noise from top/bottom.
        3. Parse each line independently — drop bad lines, keep good ones.
        """
        lines = [ln.strip() for ln in raw.strip().splitlines() if ln.strip()]
        candidate_lines = self._trim_comment_noise(lines)

        triples: List[Dict[str, str]] = []
        dropped = 0

        for line in candidate_lines:
            triple = self._parse_line(line)
            if triple:
                triples.append(triple)
            else:
                dropped += 1
                logger.debug(f"TripleExtractor: Dropped malformed line: {line!r}")

        if dropped:
            logger.info(
                f"TripleExtractor: Partial recovery — kept {len(triples)}, dropped {dropped} line(s)."
            )
        return triples

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract(self, block_text: str) -> Optional[dict]:
        if not isinstance(block_text, str) or not block_text.strip():
            logger.debug("TripleExtractor: empty or non-string input, skipping.")
            return None

        prompt = self._build_prompt(block_text)

        try:
            raw = self.llm_service.generate(prompt)
            logger.info(f"TripleExtractor raw response: {raw!r}")
        except Exception as e:
            logger.error("TripleExtractor: LLM call failed: %s", e)
            return None

        if not raw or not isinstance(raw, str) or not raw.strip():
            logger.debug("TripleExtractor: LLM returned empty response.")
            return None

        triples = self._parse_response(raw)

        if not triples:
            logger.info("TripleExtractor: No valid triples recovered from block.")
            return None

        return {"triples": triples}
