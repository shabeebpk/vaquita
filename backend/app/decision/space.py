"""Decision space definition.

Defines the finite set of decisions the system can make after Phase-4.5 (hypotheses filtering).
All decision labels must be defined here; no hardcoded decisions elsewhere.
"""
from enum import Enum
from typing import Set


class Decision(Enum):
    """Enumeration of all possible decisions after Phase-4.5."""
    
    # Undecided: unable to reach conclusion
    UNDECIDED = "undecided"
    
    # Insufficient signal: not enough data for confident decision
    INSUFFICIENT_SIGNAL = "insufficient_signal"
    
    # Confident outcome: sufficient signal to proceed
    HALT_CONFIDENT = "halt_confident"
    
    # No hypothesis path: no evidence of indirect path growth or support
    HALT_NO_HYPOTHESIS = "halt_no_hypothesis"
    
    # Need external input
    ASK_DOMAIN_EXPERT = "ask_domain_expert"
    ASK_USER_INPUT = "ask_user_input"
    
    # Need more data
    FETCH_MORE_LITERATURE = "fetch_more_literature"
    
    # Strategic download: targeted extraction of promising leads
    STRATEGIC_DOWNLOAD_TARGETED = "strategic_download_targeted"

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