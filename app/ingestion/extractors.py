"""
Text extraction from different document formats.
Handles PDF, DOCX, and plain text files.
"""
import os

try:
    import fitz  # PyMuPDF
except Exception:
    fitz = None

from docx import Document
from typing import List, Tuple


class DocumentExtractor:
    """Extract text from various document formats."""

    @staticmethod
    def extract_pdf(file_path: str) -> List[Tuple[str, str]]:
        """
        Extract text from PDF.
        Returns list of (page_content, page_metadata).
        """
        extracted = []
        try:
            if fitz is None:
                raise RuntimeError("PyMuPDF (fitz) is not installed")

            doc = fitz.open(file_path)
            num_pages = doc.page_count
            for page_num in range(num_pages):
                page = doc.load_page(page_num)
                # Extract as simple text; this reconstructs layout better than pypdf
                text = page.get_text("text") or ""
                metadata = f"PDF page {page_num+1}/{num_pages}"
                extracted.append((text, metadata))
        except Exception as e:
            raise ValueError(f"Failed to extract PDF {file_path}: {str(e)}")
        return extracted

    @staticmethod
    def extract_docx(file_path: str) -> List[Tuple[str, str]]:
        """
        Extract text from DOCX.
        Returns list of (paragraph_content, paragraph_metadata).
        """
        extracted = []
        try:
            doc = Document(file_path)
            for para_num, para in enumerate(doc.paragraphs, 1):
                text = para.text.strip()
                if text:  # only non-empty paragraphs
                    metadata = f"DOCX paragraph {para_num}"
                    extracted.append((text, metadata))
        except Exception as e:
            raise ValueError(f"Failed to extract DOCX {file_path}: {str(e)}")
        return extracted

    @staticmethod
    def extract_plain_text(file_path: str) -> List[Tuple[str, str]]:
        """
        Extract text from plain text file (.txt).
        Returns list of (content, metadata).
        """
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                text = f.read()
            return [(text, "Plain text file")]
        except UnicodeDecodeError:
            # Try alternate encoding
            with open(file_path, 'r', encoding='latin-1') as f:
                text = f.read()
            return [(text, "Plain text file (latin-1)")]
        except Exception as e:
            raise ValueError(f"Failed to extract text from {file_path}: {str(e)}")

    @staticmethod
    def extract_from_file(file_path: str, file_type: str) -> List[Tuple[str, str]]:
        """
        Route to appropriate extractor based on file type.
        file_type: 'pdf', 'docx', 'txt'
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")

        if file_type.lower() == "pdf":
            return DocumentExtractor.extract_pdf(file_path)
        elif file_type.lower() in ("docx", "doc"):
            return DocumentExtractor.extract_docx(file_path)
        elif file_type.lower() == "txt":
            return DocumentExtractor.extract_plain_text(file_path)
        else:
            raise ValueError(f"Unsupported file type: {file_type}")
