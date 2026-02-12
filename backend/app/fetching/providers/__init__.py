"""
Fetch Provider Registry.
"""
from app.fetching.providers.semantic_scholar import SemanticScholarProvider

# Registry of available provider classes
# Currently only Semantic Scholar is supported
PROVIDER_REGISTRY = {
    "semantic_scholar": SemanticScholarProvider,
}
