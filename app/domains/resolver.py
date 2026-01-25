"""
Domains module: domain detection for provider selection.

Separated from fetching and signals. Domain resolution guides provider
selection only; never influences hypothesis confidence or decision outcomes.

Deterministic + LLM fallback strategy with full configuration.
"""
import logging
import os
import json
from typing import Dict, Any, Optional, Tuple, List
from sqlalchemy.orm import Session

from app.prompts.loader import load_prompt

logger = logging.getLogger(__name__)


class DomainResolverConfig:
    """Configuration for domain resolution behavior."""
    
    def __init__(self):
        # Deterministic confidence threshold (0.0-1.0)
        # If deterministic signal < threshold, fall back to LLM
        self.deterministic_threshold = float(
            os.getenv("DOMAIN_DETERMINISTIC_THRESHOLD", "0.7")
        )
        
        # LLM confidence threshold for accepting LLM classification
        self.llm_threshold = float(os.getenv("DOMAIN_LLM_THRESHOLD", "0.6"))
        
        # Available domain labels (comma-separated)
        domain_labels = os.getenv(
            "DOMAIN_LABELS",
            "biomedical,computer_science,physics,chemistry,mathematics,engineering"
        )
        self.domain_labels = [d.strip() for d in domain_labels.split(",")]
        
        # Keyword mappings for deterministic detection (JSON)
        keywords_json = os.getenv("DOMAIN_KEYWORDS", "{}")
        try:
            self.domain_keywords = json.loads(keywords_json)
        except json.JSONDecodeError:
            logger.warning("Failed to parse DOMAIN_KEYWORDS JSON, using empty dict")
            self.domain_keywords = {}
        
        logger.info(
            f"DomainResolverConfig: threshold={self.deterministic_threshold}, "
            f"domains={self.domain_labels}, llm_threshold={self.llm_threshold}"
        )


def get_hypothesis_keywords(hypothesis: Dict[str, Any]) -> List[str]:
    """Extract keywords from hypothesis (source, target, path)."""
    keywords = []
    
    # Source and target nodes
    if "source" in hypothesis:
        keywords.append(str(hypothesis["source"]).lower())
    if "target" in hypothesis:
        keywords.append(str(hypothesis["target"]).lower())
    
    # Path nodes
    if "path" in hypothesis and isinstance(hypothesis["path"], list):
        for node in hypothesis["path"]:
            keywords.append(str(node).lower())
    
    # Explanation
    if "explanation" in hypothesis:
        explanation_words = str(hypothesis["explanation"]).lower().split()
        keywords.extend(explanation_words[:20])  # Limit to first 20 words
    
    return keywords


def score_domain_match(
    keywords: List[str],
    domain_labels: List[str],
    domain_keywords: Dict[str, List[str]]
) -> Dict[str, float]:
    """
    Score each domain based on keyword matches.
    
    Args:
        keywords: List of keyword strings from hypothesis
        domain_labels: List of domain names
        domain_keywords: Dict mapping domain -> list of keywords
    
    Returns:
        Dict mapping domain -> confidence score (0.0-1.0)
    """
    scores = {}
    
    for domain in domain_labels:
        domain_kw = domain_keywords.get(domain, [])
        if not domain_kw:
            scores[domain] = 0.0
            continue
        
        # Count matches (case-insensitive, substring match)
        matches = sum(
            1 for kw in keywords
            for dkw in domain_kw
            if dkw.lower() in kw.lower()
        )
        
        # Normalize by maximum possible matches
        max_matches = len(keywords) * len(domain_kw)
        if max_matches > 0:
            score = min(1.0, matches / max_matches)
        else:
            score = 0.0
        
        scores[domain] = score
    
    return scores


def deterministic_domain_resolution(
    hypothesis: Dict[str, Any],
    config: Optional[DomainResolverConfig] = None
) -> Tuple[Optional[str], float]:
    """
    Attempt deterministic domain resolution from hypothesis keywords.
    
    Args:
        hypothesis: Hypothesis dict with source, target, path, explanation
        config: DomainResolverConfig (created if None)
    
    Returns:
        Tuple of (domain_name or None, confidence score)
    """
    if config is None:
        config = DomainResolverConfig()
    
    keywords = get_hypothesis_keywords(hypothesis)
    if not keywords:
        logger.debug("No keywords extracted from hypothesis for deterministic resolution")
        return None, 0.0
    
    # Score each domain
    scores = score_domain_match(keywords, config.domain_labels, config.domain_keywords)
    
    # Find best match
    if not scores:
        return None, 0.0
    
    best_domain = max(scores, key=scores.get)
    best_score = scores[best_domain]
    
    logger.debug(
        f"Deterministic domain scores: {scores}. Best: {best_domain} ({best_score:.2f})"
    )
    
    if best_score >= config.deterministic_threshold:
        return best_domain, best_score
    
    return None, best_score


def llm_domain_resolution(
    hypothesis: Dict[str, Any],
    llm_client: Optional[Any] = None,
    config: Optional[DomainResolverConfig] = None
) -> Tuple[Optional[str], float]:
    """
    Fall back to LLM for closed-set domain classification.
    
    Args:
        hypothesis: Hypothesis dict
        llm_client: LLM client (e.g., OpenAI)
        config: DomainResolverConfig (created if None)
    
    Returns:
        Tuple of (domain_name or None, confidence score)
    """
    if config is None:
        config = DomainResolverConfig()
    
    if not llm_client:
        logger.warning("No LLM client provided, cannot perform LLM domain resolution")
        return None, 0.0
    
    source = hypothesis.get("source", "")
    target = hypothesis.get("target", "")
    explanation = hypothesis.get("explanation", "")
    
    # Build closed-set classification prompt
    domains_str = ", ".join(config.domain_labels)
    
    # Load prompt template from file
    prompt_file = os.getenv("DOMAIN_RESOLVER_PROMPT_FILE", "domain_resolver.txt")
    template = load_prompt(prompt_file)
    
    if not template:
        # Fallback template if file not found
        template = """Classify the following scientific hypothesis into ONE domain.

Hypothesis:
- Source: {source}
- Target: {target}
- Explanation: {explanation}

Domains: {domains}

Respond with ONLY the domain name, nothing else."""
    
    prompt = template.format(source=source, target=target, explanation=explanation, domains=domains_str)
    
    try:

        logger.info(f"llm client, {llm_client} , { 'yes' if 'call' in dir(llm_client) else dir(llm_client) }")
        # Call LLM
        response = llm_client.invoke(prompt)
        
        if isinstance(response, str):
            domain_text = response.strip().lower()
        else:
            # Handle different LLM response formats
            domain_text = str(response).lower()
        
        # Validate domain is in allowed list
        for domain in config.domain_labels:
            if domain.lower() in domain_text:
                logger.info(f"LLM domain resolution: {domain}")
                return domain, config.llm_threshold
        
        logger.warning(f"LLM returned unrecognized domain: {domain_text}")
        return None, 0.0
        
    except Exception as e:
        logger.error(f"LLM domain resolution failed: {e}")
        return None, 0.0


def resolve_domain(
    hypothesis: Dict[str, Any],
    llm_client: Optional[Any] = None,
    config: Optional[DomainResolverConfig] = None
) -> Tuple[Optional[str], float]:
    """
    Resolve domain with deterministic fallback to LLM.
    
    Order:
    1. Attempt deterministic resolution
    2. If below threshold, fall back to LLM
    3. If LLM succeeds and confidence >= threshold, return domain
    4. Otherwise return None
    
    Args:
        hypothesis: Hypothesis dict
        llm_client: LLM client for fallback
        config: DomainResolverConfig (created if None)
    
    Returns:
        Tuple of (domain_name or None, confidence score)
    """
    if config is None:
        config = DomainResolverConfig()
    
    # Try deterministic first
    domain, confidence = deterministic_domain_resolution(hypothesis, config)
    if domain:
        return domain, confidence
    
    # Fall back to LLM
    logger.debug("Deterministic resolution insufficient, falling back to LLM")
    domain, confidence = llm_domain_resolution(hypothesis, llm_client, config)
    
    return domain, confidence
