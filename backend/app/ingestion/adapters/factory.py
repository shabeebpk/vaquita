import os
from typing import Any
from .base import BaseExtractionAdapter
from .pdf import PDFAdapter
from .text import SimpleTextAdapter

def get_adapter_for_source(source_type: str, source_ref: str) -> BaseExtractionAdapter:
    """
    Factory to route ingestion sources to the correct physical adapter.
    """
    source_type = source_type.lower()
    
    # 1. Routing by Explicit Source Type
    if source_type in ["pdf_text"]:
        return PDFAdapter()
    
    if source_type in ["user_text", "paper_abstract", "api_text"]:
        return SimpleTextAdapter()
    
    # 2. Routing by Extension in Reference
    if "file:" in source_ref:
        file_path = source_ref.replace("file:", "")
        ext = os.path.splitext(file_path)[1].lower().replace('.', '')
        if ext == "pdf":
            return PDFAdapter()
        if ext == "txt":
            return SimpleTextAdapter()
            
    # Fallback
    return SimpleTextAdapter()
