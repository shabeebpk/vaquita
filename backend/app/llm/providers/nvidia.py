import logging
from app.llm.providers.base import BaseLLMProvider
from app.llm.providers.registry import registry

logger = logging.getLogger(__name__)

@registry.register("nvidia")
class NvidiaProvider(BaseLLMProvider):
    """NVIDIA provider implementation."""
    
    CREDENTIAL_KEYS = ["NVIDIA_API_KEY", "NVIDIA_BASE_URL"]
    
    def __init__(self, credentials: dict, **kwargs):
        self.api_key = credentials.get("api_key")
        self.base_url = credentials.get("base_url")
        self.model = kwargs.get("model", "llama2")
        self.max_tokens = kwargs.get("max_tokens", 1024)
        
        if not self.api_key:
            raise ValueError("NVIDIA_API_KEY is required for NvidiaProvider")
            
        try:
            from openai import OpenAI
            self.client = OpenAI(api_key=self.api_key, base_url=self.base_url)
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client for Nvidia: {e}")
            raise

    def generate(self, prompt: str, **kwargs) -> str:
        """Generate text using NVIDIA API. Supports runtime overrides."""
        # Use runtime kwargs, then instance config, then defaults
        max_tokens = kwargs.get("max_tokens") or self.max_tokens
        # NVIDIA API hard limit for llama-chatqa is often 1024
        max_tokens = min(max_tokens, 1024) 
        temperature = kwargs.get("temperature", 0.0)
        
        try:
            resp = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=max_tokens,
                temperature=temperature
            )
            content = resp.choices[0].message.content
            if not content:
                raise ValueError("NVIDIA API returned empty content")
            return content
        except Exception as e:
            logger.error(f"NVIDIA generation failed: {e}")
            raise
