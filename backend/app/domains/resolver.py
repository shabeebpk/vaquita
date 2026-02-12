"""
Domains module: LLM-only domain detection for hypothesis classification.

This module implements the Domain Resolution Contract:
- LLM-only (or job override)
- Closed-set classification from admin_policy.allowed_domains
- Fallback loop through admin_policy.llm_order
- Stable, one-time assignment per hypothesis
"""
import logging
from typing import Dict, Any, Optional, List, Tuple

from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)


class DomainResolverConfig:
    """Config wrapper for domain resolution from AdminPolicy."""
    def __init__(self):
        from app.config.admin_policy import admin_policy
        dr = admin_policy.algorithm.domain_resolution
        
        self.allowed_domains = dr.allowed_domains
        
        logger.debug(f"DomainResolverConfig loaded: allowed={self.allowed_domains}")


def llm_domain_resolution(
    hypothesis: Dict[str, Any],
    llm_client: Any,
    config: DomainResolverConfig
) -> Optional[str]:
    """
    Classify hypothesis into a domain using the LLM fallback loop.
    
    Tries each LLM provider in config.llm_order. Returns the first valid domain.
    """
    source = hypothesis.get("source", "")
    target = hypothesis.get("target", "")
    explanation = hypothesis.get("explanation", "")
    path = hypothesis.get("path", [])
    
    # Format hypothesis text for prompt
    hyp_text = (
        f"Source: {source}\n"
        f"Target: {target}\n"
        f"Path: {' -> '.join(path)}\n"
        f"Explanation: {explanation}"
    )
    
    domains_str = ", ".join(config.allowed_domains)
    
    # Load prompt contract
    from app.config.admin_policy import admin_policy
    prompt_file = admin_policy.prompt_assets.domain_resolver
    template = load_prompt(
        prompt_file, 
        fallback="Classify the following hypothesis domain. Allowed domains: {domains}. "
                 "The hypothesis is: {hypothesis}. "
                 "Return ONLY the domain name from the list, or empty if uncertain. "
                 "No explanation, no formatting."
    )
    
    try:
        prompt = template.format(
            hypothesis=hyp_text, 
            domains=domains_str,
            source=source,
            target=target,
            explanation=explanation
        )
    except KeyError as e:
        logger.warning(f"Prompt template missing variable: {e}. Falling back to basic format.")
        prompt = f"Classify hypothesis: {hyp_text}. Domains: {domains_str}"
    
    # Call global LLM service (fallback is handled inside generate())
    try:
        response = llm_client.generate(prompt)
        
        if not response:
            return None
            
        resolved = response.strip().lower()
        
        # Validation: must be in allowed_domains
        for domain in config.allowed_domains:
            if resolved == domain.lower():
                logger.info(f"Successfully resolved domain '{domain}'")
                return domain
        
        logger.warning(f"LLM returned invalid domain: '{resolved}'")
        
    except Exception as e:
        logger.error(f"Domain resolution failed at service level: {e}")
        
    return None


def resolve_domain(
    hypothesis: Dict[str, Any],
    job_config: Dict[str, Any],
    llm_client: Any
) -> Optional[str]:
    """
    Authoritative domain resolution entry point.
    
    Contract Flow:
    1. Job Override Check
    2. LLM Fallback Loop
    """
    # 1. Job Override Check
    job_override = job_config.get("domain")
    if job_override:
        logger.info(f"Using job override domain: {job_override}")
        return job_override
        
    # 2. LLM-based automatic resolution
    config = DomainResolverConfig()
    return llm_domain_resolution(hypothesis, llm_client, config)
