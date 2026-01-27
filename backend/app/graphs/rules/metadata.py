"""Metadata extraction and attribute rules for Phase-2.5 graph sanitization.

When a metadata node (year, DOI, ISSN, etc.) is demoted, its value is
extracted and assigned as an attribute on the subject node. This module
defines the extraction logic and attribute naming.
"""
import re

# Metadata extractors: (pattern, attribute_name, extractor_fn)
YEAR_EXTRACTOR = (r"^(19|20)\d{2}$", "year", lambda m: int(m.group(1) + m.group(2)))
DOI_EXTRACTOR = (r"^(?:doi:|10\.\d+/.*)$", "doi", lambda m: m.group(0).replace("doi:", "").replace("DOI:", ""))
ISSN_EXTRACTOR = (r"^(?:ISSN|issn)[\s-]?(\d{4})[\s-]?(\d{4})$", "issn", lambda m: f"{m.group(1)}-{m.group(2)}")
ARXIV_EXTRACTOR = (r"^(?:arxiv:)?(\d+\.\d+)$", "arxiv_id", lambda m: m.group(1))
PMID_EXTRACTOR = (r"^(?:PMID|pmid):?(\d+)$", "pmid", lambda m: int(m.group(1)))
URL_EXTRACTOR = (r"^(https?://\S+)$", "url", lambda m: m.group(1))

METADATA_EXTRACTORS = [
    YEAR_EXTRACTOR,
    DOI_EXTRACTOR,
    ISSN_EXTRACTOR,
    ARXIV_EXTRACTOR,
    PMID_EXTRACTOR,
    URL_EXTRACTOR,
]


def extract_metadata(node: str) -> tuple:
    """Extract metadata from a node.
    
    Returns: (attribute_name, attribute_value) or (None, None) if no match
    """
    for pattern, attr_name, extractor in METADATA_EXTRACTORS:
        m = re.match(pattern, node, re.I)
        if m:
            try:
                attr_val = extractor(m)
                return (attr_name, attr_val)
            except Exception:
                pass
    return (None, None)


# Metadata-to-edge-type mapping: when demoting metadata, how to tag the edge?
METADATA_EDGE_TYPE = {
    "year": "has_year",
    "doi": "has_doi",
    "issn": "has_issn",
    "arxiv_id": "has_arxiv",
    "pmid": "has_pmid",
    "url": "has_url",
}
