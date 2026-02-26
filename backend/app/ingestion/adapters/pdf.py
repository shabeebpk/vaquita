import fitz
import logging
from typing import List, Any, Optional
from .base import BaseExtractionAdapter, ExtractionRegion

logger = logging.getLogger(__name__)

# Section headings that signal the start of a named region
REGION_MARKERS = {
    "abstract": "abstract",
    "introduction": "introduction",
    "conclusion": "conclusion",
    "conclusions": "conclusion",
    "results": "results",
    "result": "results",
    "method": "methods",
    "methods": "methods",
    "methodology": "methods",
    "discussion": "discussion",
}

class PDFAdapter(BaseExtractionAdapter):
    """
    Layout-aware PDF extractor using PyMuPDF (fitz).

    Strategy:
    1. Sort blocks by column layout (DLA).
    2. Detect named region headings (abstract, intro, etc).
    3. Accumulate all text under that heading into one stream.
    4. When a NEW heading is found, flush the old stream IF it is whitelisted.
    5. Stop completely when an excluded heading (references, bibliography) is found.
    6. Return only whitelisted region streams to the ingestion service.
    
    Contract: This adapter returns ExtractionRegion objects only; it does NOT write to
    IngestionSource.raw_text. The ingestion service is responsible for concatenating
    regions, refining, and storing the final text to raw_text before slicing.
    """

    SUPPORTED_EXTENSIONS = ["pdf"]

    def _detect_region(self, text: str) -> Optional[str]:
        """Check if a short block header maps to a known region name."""
        # Only check short blocks — real headings are short
        if len(text) > 80:
            return None
        lower = text.lower().strip().rstrip(".")
        return REGION_MARKERS.get(lower)

    def extract_regions(self, file_path: str, config: Any) -> List[ExtractionRegion]:
        self.validate_file(file_path, self.SUPPORTED_EXTENSIONS)

        doc = fitz.open(file_path)
        all_regions: List[ExtractionRegion] = []

        current_region: Optional[str] = None  # Only set when inside a NAMED region
        region_buffer: List[str] = []
        is_pruned = False

        whitelisted = set(config.whitelisted_regions)
        excluded = set(config.excluded_regions)

        try:
            for page_num, page in enumerate(doc, 1):
                if is_pruned:
                    break

                blocks = page.get_text("blocks")
                col_threshold = config.column_width_threshold
                # DLA: sort by column (x0) then by vertical position (y0)
                sorted_blocks = sorted(blocks, key=lambda b: (b[0] // col_threshold, b[1]))

                for b in sorted_blocks:
                    text = b[4].strip()
                    if not text:
                        continue

                    lower_text = text.lower()

                    # --- PRUNE CHECK ---
                    # If this block header matches an excluded region, stop everything
                    if any(exc in lower_text[:80] for exc in excluded):
                        logger.info(f"PDFAdapter: Exclusion marker found on page {page_num}. Pruning.")
                        # Flush whatever was being gathered if it was whitelisted
                        self._flush(region_buffer, current_region, whitelisted, all_regions, page_num)
                        is_pruned = True
                        break

                    # --- REGION HEADER DETECTION ---
                    detected = self._detect_region(text)
                    if detected and detected != current_region:
                        # Flush the previous region if it was whitelisted
                        self._flush(region_buffer, current_region, whitelisted, all_regions, page_num)
                        # Start fresh under the new region
                        current_region = detected
                        region_buffer = []
                        logger.info(f"PDFAdapter: Entering region '{current_region}' on page {page_num}.")
                        # Don't add the heading text itself to the buffer
                        continue

                    # --- ACCUMULATE ---
                    # Only accumulate if we are inside a known region
                    if current_region is not None:
                        region_buffer.append(text)

            # Final flush for last region
            if not is_pruned:
                self._flush(region_buffer, current_region, whitelisted, all_regions, page_num if 'page_num' in dir() else 1)

            if not all_regions and config.fallback_max_tokens > 0:
                logger.warning(f"PDFAdapter: No whitelisted regions found. Extracting up to {config.fallback_max_tokens} tokens via page-progressive fallback.")
                fallback_text = self._extract_fallback_progressive(doc, config.fallback_max_tokens)
                if fallback_text.strip():
                    all_regions.append(ExtractionRegion(fallback_text, "fallback_progressive", 1))

        finally:
            doc.close()

        logger.info(f"PDFAdapter: Extraction complete. {len(all_regions)} region(s) gathered.")
        return all_regions

    def _flush(self, buffer: List[str], region: Optional[str], whitelisted: set,
               output: List[ExtractionRegion], page_num: int):
        """Flush the buffer into output if region is whitelisted and buffer is non-empty."""
        if not buffer or region is None:
            return
        if region not in whitelisted:
            logger.info(f"PDFAdapter: Discarding non-whitelisted region '{region}' ({len(buffer)} blocks).")
            return
        full_text = " ".join(buffer).strip()
        if full_text:
            output.append(ExtractionRegion(full_text, region, page_num))
            logger.info(f"PDFAdapter: Flushed whitelisted region '{region}' — {len(full_text)} chars.")

    def _extract_fallback_progressive(self, doc, max_tokens: int) -> str:
        """
        Extract pages progressively until token limit is reached.
        Approximate tokens as chars / 3.5.
        """
        CHARS_PER_TOKEN = 3.5
        max_chars = max_tokens * CHARS_PER_TOKEN
        accumulated_text = []
        accumulated_chars = 0
        
        for page_num, page in enumerate(doc, 1):
            page_text = page.get_text().strip()
            if not page_text:
                continue
            
            page_chars = len(page_text)
            
            # Stop if adding this page would exceed limit
            if accumulated_chars + page_chars > max_chars:
                logger.info(f"PDFAdapter: Token limit reached after page {page_num}. Stopping fallback extraction.")
                break
            
            accumulated_text.append(page_text)
            accumulated_chars += page_chars
            logger.info(f"PDFAdapter: Fallback extracted page {page_num} ({page_chars} chars, cumulative {accumulated_chars}/{max_chars}).")
        
        return "\n\n".join(accumulated_text)
