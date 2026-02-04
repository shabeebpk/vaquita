"""
Text normalization using NLTK and deterministic transformations.
Removes layout noise, fixes encoding, normalizes whitespace, enforces sentence boundaries.
Optionally applies lexical repair to fix layout-induced word splits.
"""
import os
import re
import unicodedata
from typing import List
import nltk
from nltk.tokenize import sent_tokenize

# Download required NLTK data (safe to call multiple times)
try:
    nltk.data.find('tokenizers/punkt')
except LookupError:
    nltk.download('punkt', quiet=True)


class TextNormalizer:
    """Normalize raw text to canonical form."""

    @staticmethod
    def normalize_encoding(text: str) -> str:
        """
        Fix common encoding issues.
        - Normalize Unicode (NFC form)
        - Fix smart quotes, dashes, etc.
        """
        # Normalize to NFC (decomposed → composed)
        text = unicodedata.normalize('NFC', text)
        
        # Replace common smart quotes with ASCII equivalents
        text = text.replace('"', '"').replace('"', '"')  # smart double quotes
        text = text.replace(''', "'").replace(''', "'")  # smart single quotes
        
        # Replace em-dashes and en-dashes with standard hyphen
        text = text.replace('—', '-').replace('–', '-')
        
        return text

    @staticmethod
    def normalize_whitespace(text: str) -> str:
        """
        Remove extra whitespace, normalize line breaks.
        - Remove leading/trailing whitespace
        - Collapse multiple spaces to single space
        - Remove excessive line breaks
        """
        # Remove leading/trailing whitespace per line
        lines = [line.strip() for line in text.split('\n')]
        
        # Remove empty lines, rejoin
        lines = [line for line in lines if line]
        text = ' '.join(lines)
        
        # Collapse multiple spaces
        text = re.sub(r' +', ' ', text)
        
        return text

    @staticmethod
    def fix_sentence_boundaries(text: str) -> str:
        """
        Ensure proper sentence boundaries using NLTK.
        - Split into sentences
        - Rejoin with consistent spacing
        - Remove sentence fragments
        """
        try:
            sentences = sent_tokenize(text)
            # Filter out very short sentences (likely noise)
            sentences = [s.strip() for s in sentences if len(s.strip()) > 5]
            # Rejoin with single space after period
            text = ' '.join(sentences)
        except Exception:
            # Fallback: use regex-based splitting
            text = re.sub(r'([.!?])\s+', r'\1 ', text)
        
        return text

    @staticmethod
    def extract_urls(text: str) -> tuple:
        """
        Find URLs in text and replace them with a placeholder.

        Returns:
            (cleaned_text, list_of_urls)
        """
        url_pattern = r'https?://\S+|www\.\S+'
        urls = re.findall(url_pattern, text)
        # Replace all URLs with placeholder [URL]
        cleaned = re.sub(url_pattern, '[URL]', text)
        return cleaned, urls

    @staticmethod
    def remove_emails(text: str) -> str:
        """
        Remove email addresses (optional).
        """
        email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
        text = re.sub(email_pattern, '', text)
        return text

    @staticmethod
    def normalize(
        text: str,
        extract_urls: bool = True,
        remove_emails: bool = True,
        fix_encoding: bool = True,
        fix_whitespace: bool = True,
        fix_sentences: bool = True,
        apply_lexical_repair: bool = None
    ) -> tuple:
        """
        Full normalization pipeline.
        
        Args:
            text: raw input text
            extract_urls: strip URLs
            remove_emails: strip email addresses
            fix_encoding: normalize Unicode and quotes
            fix_whitespace: collapse whitespace and line breaks
            fix_sentences: enforce sentence boundaries
            apply_lexical_repair: if None, reads from ENABLE_LEXICAL_REPAIR env var (default: False)
        
        Returns:
            canonical normalized text and extracted URLs tuple
        """
        # Determine lexical repair setting from parameter or environment
        if apply_lexical_repair is None:
            enable_lexical_repair = False 
        else:
            enable_lexical_repair = apply_lexical_repair

        extracted_urls = []

        if fix_encoding:
            text = TextNormalizer.normalize_encoding(text)

        if extract_urls:
            text, extracted_urls = TextNormalizer.extract_urls(text)

        if remove_emails:
            text = TextNormalizer.remove_emails(text)

        # Apply lexical repair BEFORE whitespace normalization
        # Lexical repair works on token boundaries and should happen early
        if enable_lexical_repair:
            from app.ingestion.lexical import lexical_repair
            text = lexical_repair(text)

        if fix_whitespace:
            text = TextNormalizer.normalize_whitespace(text)

        if fix_sentences:
            text = TextNormalizer.fix_sentence_boundaries(text)

        return text.strip(), extracted_urls

    @staticmethod
    def get_normalization_config(
        remove_urls: bool = True,
        remove_emails: bool = True,
        fix_encoding: bool = True,
        fix_whitespace: bool = True,
        fix_sentences: bool = True
    ) -> dict:
        """
        Return normalization config for DB storage (auditability).
        """
        return {
            "remove_urls": remove_urls,
            "remove_emails": remove_emails,
            "fix_encoding": fix_encoding,
            "fix_whitespace": fix_whitespace,
            "fix_sentences": fix_sentences,
            "nltk_tokenizer": "punkt",
            "unicode_form": "NFC"
        }
