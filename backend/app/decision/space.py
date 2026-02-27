"""Decision space definition.

Defines the finite set of decisions the system can make after Phase-4.5 (hypotheses filtering).
All decision labels must be defined here; no hardcoded decisions elsewhere.
Supports both discovery mode (hypothesis generation) and verification mode (entity connection verification).
"""
from enum import Enum
from typing import Set


class Decision(Enum):
    """Enumeration of all possible decisions after Phase-4.5."""
    
    # ===== Discovery Mode Decisions =====
    
    # Insufficient signal: not enough data for confident decision
    INSUFFICIENT_SIGNAL = "insufficient_signal"
    
    # Confident outcome: sufficient signal to proceed
    HALT_CONFIDENT = "halt_confident"
    
    # No hypothesis path: no evidence of indirect path growth or support
    HALT_NO_HYPOTHESIS = "halt_no_hypothesis"
    
    # Need more data
    FETCH_MORE_LITERATURE = "fetch_more_literature"
    
    # Strategic download: targeted extraction of promising leads
    STRATEGIC_DOWNLOAD_TARGETED = "strategic_download_targeted"
    
    # ===== Verification Mode Decisions =====
    
    # Connection found: Source and target entities are connected (direct or indirect)
    VERIFICATION_FOUND = "verification_found"
    
    # Connection not found: No connection found after exhausting all search strategies
    VERIFICATION_NOT_FOUND = "verification_not_found"


def decision_from_string(decision_str: str) -> Decision:
    """Convert a string to a Decision enum value.
    
    Args:
        decision_str: String representation of a decision (e.g., "halt_confident")
    
    Returns:
        Decision enum value
    
    Raises:
        ValueError: If the string doesn't match any decision
    """
    decision_str = decision_str.strip().lower()
    
    for decision in Decision:
        if decision.value == decision_str:
            return decision
    
    raise ValueError(f"Unknown decision: {decision_str}")


def all_decisions() -> Set[str]:
    """Get all valid decision labels as strings.
    
    Returns:
        Set of all decision label strings
    """
    return {decision.value for decision in Decision}


def is_discovery_mode_decision(decision: Decision) -> bool:
    """Check if a decision is for discovery mode."""
    discovery_decisions = {
        Decision.INSUFFICIENT_SIGNAL,
        Decision.HALT_CONFIDENT,
        Decision.HALT_NO_HYPOTHESIS,
        Decision.FETCH_MORE_LITERATURE,
        Decision.STRATEGIC_DOWNLOAD_TARGETED
    }
    return decision in discovery_decisions


def is_verification_mode_decision(decision: Decision) -> bool:
    """Check if a decision is for verification mode."""
    verification_decisions = {
        Decision.VERIFICATION_FOUND,
        Decision.VERIFICATION_NOT_FOUND
    }
    return decision in verification_decisions