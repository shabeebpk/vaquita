"""Filler/contrast trimming regex patterns used during structural compression.

Keep trimming rules minimal and deterministic. Export raw regex strings
so they can be updated independently from the projection logic.
"""
LEADING_FILLERS = r"^(ensuring that|ensuring|ensures that|that|to )\b"
RATHER_THAN = r"rather than.*$"
OF_PREFIX = r"^of\s+"
TRAILING_PUNCT = r"[.,;:\)]+$"
