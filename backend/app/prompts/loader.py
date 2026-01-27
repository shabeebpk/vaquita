"""Prompt Template Loader: Centralized Prompt Management.

This module is responsible for loading prompt templates from the app/prompts/ directory.
All modules in the system that need prompts should use this loader.

Responsibilities:
- Compute base directory internally
- Load prompt files by filename
- Return raw template strings (no formatting)
- Handle failures safely (return fallback template)
- Never raise exceptions
- Never perform any formatting

Contract:
    text = load_prompt("triple_extraction.txt")  # -> str
    
    # Caller applies formatting:
    formatted = text.format(block_text=some_text)

Invariants:
- Always returns a string (never None, never raises)
- Returns fallback if file missing or unreadable
- No side effects
- Stateless (safe to call repeatedly)
"""

import os
import logging

logger = logging.getLogger(__name__)


def load_prompt(filename: str, fallback: str = "") -> str:
    """Load a prompt template file.
    
    Args:
        filename: Name of the prompt file (e.g., "triple_extraction.txt")
        fallback: Fallback template if file missing/unreadable (default empty string)
    
    Returns:
        Raw template string (ready for .format() or other formatting)
        Always returns a string (never raises exceptions)
    
    Examples:
        # Load with default fallback (empty string)
        template = load_prompt("triple_extraction.txt")
        
        # Load with custom fallback
        template = load_prompt(
            "decision_llm.txt",
            fallback="Decide: {decision_labels}"
        )
        
        # Use the loaded template
        prompt = template.format(block_text=text)
    """
    if not filename or not isinstance(filename, str):
        logger.warning(f"load_prompt: invalid filename {filename}")
        return fallback
    
    # Compute path internally
    # __file__ is app/prompts/loader.py
    # Parent is app/prompts/
    current_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(current_dir, filename)
    
    try:
        # Attempt to read file
        with open(filepath, "r", encoding="utf-8") as f:
            content = f.read()
        
        logger.debug(f"Loaded prompt from {filename} ({len(content)} chars)")
        return content
    
    except FileNotFoundError:
        logger.warning(f"Prompt file not found: {filename}. Using fallback.")
        return fallback
    
    except PermissionError:
        logger.warning(f"Permission denied reading prompt: {filename}. Using fallback.")
        return fallback
    
    except Exception as e:
        logger.warning(f"Failed to read prompt {filename}: {e}. Using fallback.")
        return fallback


def load_prompt_or_default(filename: str, default: str) -> str:
    """Load a prompt with a mandatory default.
    
    This is a convenience function that treats the second argument as
    the required default. Useful when you want explicit fallback text.
    
    Args:
        filename: Name of the prompt file
        default: Default template if file cannot be loaded
    
    Returns:
        Loaded template or default
    """
    return load_prompt(filename, fallback=default)
