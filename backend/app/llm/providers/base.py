from abc import ABC, abstractmethod

class BaseLLMProvider(ABC):
    """Abstract base class for all LLM providers.
    
    Following the Unified LLM Architecture contract:
    - Input: prompt only
    - Output: string only
    - On failure: raise exception
    """
    
    # Subclasses define which SystemSettings keys they need
    CREDENTIAL_KEYS: list = []
    
    @abstractmethod
    def __init__(self, credentials: dict, **kwargs):
        """Initialize with credentials and policy."""
        self.credentials = credentials
        self.config = kwargs

    @abstractmethod
    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text from a prompt. Accepts runtime overrides in **kwargs."""
        pass
