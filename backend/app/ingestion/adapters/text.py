import os
import logging
from typing import List, Any
from .base import BaseExtractionAdapter, ExtractionRegion

logger = logging.getLogger(__name__)

class SimpleTextAdapter(BaseExtractionAdapter):
    """
    Adapter for raw text and .txt files.
    
    Responsibility: Take raw text or a .txt file, and return it as a single body region.
    No DLA needed as these formats lack physical layout coordinates.
    
    Contract: This adapter returns ExtractionRegion objects only; it does NOT write to
    IngestionSource.raw_text. The ingestion service is responsible for concatenating
    regions, refining, and storing the final text to raw_text before slicing.
    """

    SUPPORTED_EXTENSIONS = ["txt"]

    def extract_regions(self, input_data: str, config: Any) -> List[ExtractionRegion]:
        """
        Handle either a file path or direct raw text.
        """
        # 1. Check if input is a file path
        if os.path.exists(input_data):
            self.validate_file(input_data, self.SUPPORTED_EXTENSIONS)
            try:
                with open(input_data, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                return [ExtractionRegion(content, "body", 1)]
            except Exception as e:
                logger.error(f"SimpleTextAdapter: Failed to read file {input_data}: {e}.")
                raise
        
        # 2. Otherwise treat as raw text (chat/api)
        if not input_data.strip():
            return []
            
        return [ExtractionRegion(input_data, "body", 1)]
