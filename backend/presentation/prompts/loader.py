import os
import logging

logger = logging.getLogger(__name__)


def load_presentation_prompt(filename: str, fallback: str = "") -> str:
    """Load a presentation prompt from the local presentation/prompts/ directory."""
    if not filename:
        return fallback

    current_dir = os.path.dirname(os.path.abspath(__file__))
    filepath = os.path.join(current_dir, filename)

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return f.read()
    except Exception as e:
        logger.warning(f"Failed to read presentation prompt {filename}: {e}. Using fallback.")
        return fallback
