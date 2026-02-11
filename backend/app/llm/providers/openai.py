import logging
from app.llm.providers.base import BaseLLMProvider
from app.llm.providers.registry import registry

logger = logging.getLogger(__name__)

@registry.register("openai")
class OpenAIProvider(BaseLLMProvider):
    """OpenAI provider implementation."""
    
    CREDENTIAL_KEYS = ["OPENAI_API_KEY"]
    
    def __init__(self, credentials: dict, **kwargs):
        self.api_key = credentials.get("api_key")
        self.model = kwargs.get("model", "gpt-4o")
        self.temperature = kwargs.get("temperature", 0.0)
        
        if not self.api_key:
            raise ValueError("OPENAI_API_KEY is required for OpenAIProvider")
            
        try:
            import openai
            self.openai = openai
            self.openai.api_key = self.api_key
        except Exception as e:
            logger.error(f"Failed to initialize OpenAI client: {e}")
            raise

    def generate(self, prompt: str) -> str:
        """Generate text using OpenAI API. Raises on failure."""
        try:
            resp = self.openai.ChatCompletion.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=self.temperature
            )
            content = resp.choices[0].message.content
            if not content:
                raise ValueError("OpenAI API returned empty content")
            return content
        except Exception as e:
            logger.error(f"OpenAI generation failed: {e}")
            raise
