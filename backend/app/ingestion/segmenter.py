"""
Text block segmentation from normalized canonical text.
Splits text into semantic chunks for downstream triple extraction.
"""
import re
from typing import List, Tuple


class TextSegmenter:
    """Segment normalized text into blocks."""

    @staticmethod
    def segment_by_sentences(text: str, sentences_per_block: int = 3) -> List[str]:
        """
        Split text into blocks of N sentences.

        Args:
            text: normalized text (assume sentence boundaries are correct)
            sentences_per_block: number of sentences per block
        
        Returns:
            list of text blocks
        """
        # Split by sentence boundary (period, !, ?)
        sentences = re.split(r'(?<=[.!?])\s+', text)
        sentences = [s.strip() for s in sentences if s.strip()]
        
        blocks = []
        for i in range(0, len(sentences), sentences_per_block):
            block = ' '.join(sentences[i:i + sentences_per_block])
            if block.strip():
                blocks.append(block.strip())
        
        return blocks

    @staticmethod
    def segment_by_paragraphs(text: str, min_para_length: int = 50) -> List[str]:
        """
        Split by paragraph boundaries (double newlines) or fallback to sentences.
        
        Args:
            text: normalized text
            min_para_length: minimum characters per paragraph
        
        Returns:
            list of text blocks
        """
        # Split by multiple newlines (paragraph breaks)
        paragraphs = re.split(r'\n{2,}', text)
        paragraphs = [p.strip() for p in paragraphs if p.strip()]
        
        # Filter by minimum length and further subdivide if needed
        blocks = []
        for para in paragraphs:
            if len(para) > min_para_length:
                blocks.append(para)
            elif len(para) > 0:
                # For short paragraphs, try to combine with previous if possible
                if blocks:
                    blocks[-1] += ' ' + para
                else:
                    blocks.append(para)
        
        return blocks

    @staticmethod
    def segment_by_length(text: str, block_length: int = 300, overlap: int = 50) -> List[str]:
        """
        Split by character count with optional overlap (sliding window).
        
        Args:
            text: normalized text
            block_length: target characters per block
            overlap: characters to overlap between blocks
        
        Returns:
            list of text blocks
        """
        blocks = []
        start = 0
        
        while start < len(text):
            end = start + block_length
            block = text[start:end].strip()
            
            if block:
                blocks.append(block)
            
            # Move start position (with overlap)
            start = end - overlap if overlap > 0 else end
        
        return blocks

    @staticmethod
    def segment_by_sections(text: str, section_markers: List[str] = None) -> List[Tuple[str, str]]:
        """
        Split by section headers (e.g., # Introduction, ## Methods).
        
        Args:
            text: normalized text (may include markdown-style headers)
            section_markers: regex patterns for section starts
        
        Returns:
            list of (section_title, section_text) tuples
        """
        if section_markers is None:
            # Default: markdown headers
            section_markers = [r'^#+\s+', r'^\d+\.\s+', r'^[A-Z][A-Z\s]+$']
        
        sections = []
        pattern = '|'.join(section_markers)
        
        # Split by section pattern
        parts = re.split(f'({pattern}.*?)(?=\n|$)', text, flags=re.MULTILINE)
        
        current_section = None
        for part in parts:
            if re.match(pattern, part):
                current_section = part.strip()
            elif current_section and part.strip():
                sections.append((current_section, part.strip()))
        
        return sections

    @staticmethod
    def segment(
        text: str,
        strategy: str = "sentences",
        **kwargs
    ) -> List[str]:
        """
        Unified segmentation interface.
        
        Args:
            text: normalized text
            strategy: 'sentences', 'paragraphs', 'length', or 'sections'
            **kwargs: strategy-specific parameters
        
        Returns:
            list of text blocks
        """
        if strategy == "sentences":
            sentences_per_block = kwargs.get('sentences_per_block', 3)
            return TextSegmenter.segment_by_sentences(text, sentences_per_block)
        elif strategy == "paragraphs":
            min_length = kwargs.get('min_para_length', 50)
            return TextSegmenter.segment_by_paragraphs(text, min_length)
        elif strategy == "length":
            block_length = kwargs.get('block_length', 300)
            overlap = kwargs.get('overlap', 50)
            return TextSegmenter.segment_by_length(text, block_length, overlap)
        elif strategy == "sections":
            section_markers = kwargs.get('section_markers', None)
            # Returns (title, text) tuples; extract just text
            sections = TextSegmenter.segment_by_sections(text, section_markers)
            return [text for _, text in sections]
        else:
            raise ValueError(f"Unknown segmentation strategy: {strategy}")
