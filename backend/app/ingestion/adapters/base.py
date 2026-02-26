from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional
import os

class ExtractionRegion:
    """Represents a physically isolated region of text with metadata."""
    def __init__(self, text: str, region_type: str, page_num: int, metadata: Optional[Dict[str, Any]] = None):
        self.text = text
        self.region_type = region_type # 'abstract', 'body', 'header', etc.
        self.page_num = page_num
        self.metadata = metadata or {}

class BaseExtractionAdapter(ABC):
    """
    Abstract contract for Layout-Aware Extractor Adapters.
    
    Responsibility: Take a raw file, analyze its physical layout, 
    and return structured regions of text. Adapters NEVER write back to the database;
    they only return region objects. The ingestion service is responsible for:
    - Optionally refining region text via the refinery layer
    - Concatenating all regions
    - Writing the final concatenated text to IngestionSource.raw_text (canonical storage)
    - Slicing that raw_text into blocks
    
    Contract enforcement: All extracted text MUST flow through IngestionSource.raw_text
    before slicing occurs. No adapter or caller may bypass this column.
    """

    @abstractmethod
    def extract_regions(self, file_path: str, config: Any) -> List[ExtractionRegion]:
        """
        Analyze layout and return whitelisted regions.
        
        Args:
            file_path: Absolute path to local file (or text string for text adapters).
            config: AdminPolicy.algorithm.extraction config object.
            
        Returns:
            List of ExtractionRegion objects sorted by logical reading order.
            The ingestion service will concatenate these regions, optionally refine,
            and write the result to IngestionSource.raw_text before slicing.
        """
        pass

    def validate_file(self, file_path: str, supported_extensions: List[str]):
        """Helper to validate file existence and extension."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Extraction file missing: {file_path}")
        
        ext = os.path.splitext(file_path)[1].lower().replace('.', '')
        if ext not in supported_extensions:
            raise ValueError(f"Unsupported extension '{ext}' for adapter. Expected: {supported_extensions}")
