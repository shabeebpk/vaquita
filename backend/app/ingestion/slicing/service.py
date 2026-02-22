import logging
import re
from typing import List
from app.config.admin_policy import admin_policy

logger = logging.getLogger(__name__)

class SentenceSlicingService:
    """
    Slices refined text into blocks based on sentence integrity and token limits.
    
    Rules:
    - Never end with a partial sentence.
    - Respect max_tokens_per_block (approximate via char count if needed, or real tokenizer).
    - Group by sentences_per_block.
    """

    def __init__(self):
        self.config = admin_policy.algorithm.slicing

    def slice_text(self, text: str) -> List[str]:
        """
        Split text into validated blocks.
        """
        if not text.strip():
            return []

        # 1. Split into individual sentences (Basic regex for speed/recovery)
        # We look for terminal punctuation followed by whitespace or end of string.
        sentences = re.split(r'(?<=[.!?])\s+', text.strip())
        sentences = [s.strip() for s in sentences if s.strip()]

        if not sentences:
            return [text.strip()]

        blocks: List[str] = []
        current_block_sentences: List[str] = []
        current_block_token_est = 0
        
        # Approximate tokens as chars / 4 (safe floor)
        CHARS_PER_TOKEN = 3.5 
        MAX_CHARS = self.config.max_tokens_per_block * CHARS_PER_TOKEN

        for sentence in sentences:
            sentence_chars = len(sentence)
            
            # Rule: Group by count AND token limit
            # If adding this sentence would exceed tokens, we push current block
            if current_block_sentences and (
                len(current_block_sentences) >= self.config.sentences_per_block or
                (current_block_token_est + (sentence_chars / CHARS_PER_TOKEN)) > self.config.max_tokens_per_block
            ):
                blocks.append(" ".join(current_block_sentences))
                current_block_sentences = []
                current_block_token_est = 0

            current_block_sentences.append(sentence)
            current_block_token_est += (sentence_chars / CHARS_PER_TOKEN)

        # Catch remaining
        if current_block_sentences:
            blocks.append(" ".join(current_block_sentences))

        logger.info(f"SentenceSlicingService: Created {len(blocks)} blocks from {len(sentences)} sentences.")
        return blocks
