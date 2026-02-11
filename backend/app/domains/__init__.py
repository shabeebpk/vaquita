"""Domains module: domain detection for provider selection."""
from app.domains.resolver import (
    DomainResolverConfig,
    llm_domain_resolution,
    resolve_domain,
)

__all__ = [
    "DomainResolverConfig",
    "llm_domain_resolution",
    "resolve_domain",
]
