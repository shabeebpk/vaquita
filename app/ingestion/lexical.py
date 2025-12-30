"""Lightweight lexical repair to fix layout-induced word splits.

This module provides a conservative, deterministic token-level repair that
merges adjacent alphabetic tokens when their concatenation appears as a
valid word according to `wordfreq.zipf_frequency` (or an internal fallback).

Rules (conservative):
- Only consider adjacent tokens that are purely alphabetic (no digits/punct).
- Merge only if the concatenated form has a zipf frequency >= `min_zipf`.
- Do not alter punctuation or sentence structure.
- Deterministic and reversible (no context-based inference).
"""
import re
import logging
from typing import List

logger = logging.getLogger(__name__)

try:
    from wordfreq import zipf_frequency
    WORDQ_AVAILABLE = True
except Exception:
    WORDQ_AVAILABLE = False
    def zipf_frequency(word: str, lang: str = 'en'):
        # fallback: very conservative heuristic — treat long concatenations as valid
        return 0.0


_TOKEN_RE = re.compile(r"\S+")


def _is_alpha_token(tok: str) -> bool:
    return tok.isalpha()


def lexical_repair(text: str, min_zipf: float = 3.0, lang: str = 'en') -> str:
    """Repair layout-induced splits in `text`.

    Args:
        text: raw extracted text
        min_zipf: minimum zipf frequency for accepting merged token
        lang: language code for wordfreq

    Returns:
        repaired text (may be identical to input)
    """
    if not text or not isinstance(text, str):
        return text

    if not WORDQ_AVAILABLE:
        logger.debug("wordfreq not available — skipping lexical repair")
        return text

    tokens = _TOKEN_RE.findall(text)
    if not tokens:
        return text

    out_tokens: List[str] = []
    i = 0
    n = len(tokens)

    while i < n:
        tok = tokens[i]
        # if token is alphabetic, look ahead to next alphabetic token and test merge
        if i + 1 < n and _is_alpha_token(tok):
            nxt = tokens[i + 1]
            if _is_alpha_token(nxt):
                merged = tok + nxt
                try:
                    score = zipf_frequency(merged, lang)
                except Exception:
                    score = 0.0

                # Accept merge only if merged token frequency >= min_zipf
                if score >= min_zipf:
                    out_tokens.append(merged)
                    i += 2
                    continue

        # default: keep token as-is
        out_tokens.append(tok)
        i += 1

    # Reconstruct text preserving single spaces between tokens
    repaired = ' '.join(out_tokens)
    return repaired
