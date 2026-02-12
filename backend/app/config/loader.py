
import os
import json
import logging
import copy
from typing import Dict, Any
from app.config.job_config import JobConfig

logger = logging.getLogger(__name__)

def load_default_job_config() -> Dict[str, Any]:
    """
    Load, validate, and return a deep copy of the default job configuration.
    
    This function is the single source of truth for reading the JSON template.
    It ensures that every new job starts with a pristine, validated config structure.
    
    Returns:
        Dict[str, Any]: A deep copy of the default configuration dictionary.
    """
    try:
        # Resolve absolute path relative to this file
        config_path = os.path.join(os.path.dirname(__file__), "default_job_config.json")
        
        with open(config_path, "r") as f:
            config = json.load(f)
            
        # Validate and convert to model
        validated = JobConfig(**config)
            
        # Return dict copy back for existing legacy callers if needed, 
        # but ensure it came through the typed model.
        return validated.model_dump()
        
    except Exception as e:
        logger.error(f"CRITICAL: Failed to load default job config: {e}")
        # In a real system, we might want to crush here, but for now return empty to avoid immediate crash
        # though the system will likely fail later.
        raise RuntimeError(f"Could not load default job config: {e}") from e
