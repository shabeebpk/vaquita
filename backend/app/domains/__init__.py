"""Domains module: domain detection for provider selection."""
from app.domains.resolver import (
    DomainResolverConfig,
    get_hypothesis_keywords,
    score_domain_match,
    deterministic_domain_resolution,
    llm_domain_resolution,
    resolve_domain,
)

__all__ = [
    "DomainResolverConfig",
    "get_hypothesis_keywords",
    "score_domain_match",
    "deterministic_domain_resolution",
    "llm_domain_resolution",
    "resolve_domain",
]
