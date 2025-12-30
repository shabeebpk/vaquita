"""
Aggregates text from multiple sources (user_text + extracted documents + fetched papers).
Produces a single canonical text input for downstream processing.
"""
from typing import List, Tuple


class TextAggregator:
    """Combines text from multiple sources into single input space."""

    @staticmethod
    def aggregate(
        user_text: str,
        extracted_texts: List[Tuple[str, str, str]],  # (source_type, source_ref, text)
    ) -> str:
        """
        Combine user text and extracted document texts into single canonical input.
        
        Args:
            user_text: user's initial query/instruction
            extracted_texts: list of (source_type, source_ref, text) tuples
                E.g., ('pdf', 'smith2020.pdf', 'extracted pdf text')
        
        Returns:
            single aggregated text string
        """
        parts = []
        
        if user_text and user_text.strip():
            parts.append(f"[USER QUERY]\n{user_text.strip()}")
        
        for source_type, source_ref, text in extracted_texts:
            if text and text.strip():
                header = f"[{source_type.upper()}: {source_ref}]"
                parts.append(f"{header}\n{text.strip()}")
        
        canonical_text = "\n\n---\n\n".join(parts)
        
        return canonical_text

    @staticmethod
    def aggregate_with_metadata(
        user_text: str,
        extracted_texts: List[Tuple[str, str, str]],
    ) -> Tuple[str, dict]:
        """
        Aggregate and return metadata about aggregation.
        
        Returns:
            (canonical_text, metadata_dict)
        """
        canonical_text = TextAggregator.aggregate(user_text, extracted_texts)
        
        metadata = {
            "has_user_text": bool(user_text and user_text.strip()),
            "source_count": len(extracted_texts),
            "sources": [
                {"type": src_type, "ref": ref}
                for src_type, ref, _ in extracted_texts
            ],
            "canonical_text_length": len(canonical_text),
            "aggregation_method": "concatenation_with_headers"
        }
        
        return canonical_text, metadata
