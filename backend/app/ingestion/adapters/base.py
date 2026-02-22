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
    and return structured regions of text.
    """

    @abstractmethod
    def extract_regions(self, file_path: str, config: Any) -> List[ExtractionRegion]:
        """
        Analyze layout and return whitelisted regions.
        
        Args:
            file_path: Absolute path to local file.
            config: AdminPolicy.algorithm.extraction config object.
            
        Returns:
            List of ExtractionRegion objects sorted by logical reading order.
        """
        pass

    def validate_file(self, file_path: str, supported_extensions: List[str]):
        """Helper to validate file existence and extension."""
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Extraction file missing: {file_path}")
        
        ext = os.path.splitext(file_path)[1].lower().replace('.', '')
        if ext not in supported_extensions:
            raise ValueError(f"Unsupported extension '{ext}' for adapter. Expected: {supported_extensions}")
