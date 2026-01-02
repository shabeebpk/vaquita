"""Decision space definition.

Defines the finite set of decisions the system can make after Phase-4.5 (hypotheses filtering).
All decision labels must be defined here; no hardcoded decisions elsewhere.
"""
from enum import Enum
from typing import Set


class Decision(Enum):
    """Enumeration of all possible decisions after Phase-4.5."""
    
    # Confident outcome: sufficient signal to proceed
    HALT_CONFIDENT = "halt_confident"
    
    # Need external input
    ASK_DOMAIN_EXPERT = "ask_domain_expert"
    ASK_USER_INPUT = "ask_user_input"
    
    # Need more data
    FETCH_MORE_LITERATURE = "fetch_more_literature"
