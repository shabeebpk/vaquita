"""LLM Provider Adapters.

Adapters accept a prompt string and return raw text only.
They do not validate, parse, or interpret outputs.
All adapters follow the same minimal contract: call(prompt: str) -> str
"""

import os
import json
import logging

logger = logging.getLogger(__name__)


class BaseAdapter:
    """Abstract base for all LLM adapters."""
    
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}

    def call(self, prompt: str) -> str:
        """Call the LLM with a prompt and return raw text.
        
        Args:
            prompt: The prompt string.
        
        Returns:
            Raw text output from the LLM (or safe default for Dummy).
            Never raises provider-specific exceptions.
        """
        raise NotImplementedError()


class DummyAdapter(BaseAdapter):
    """Safe fallback adapter that returns empty/safe outputs.
    
    Used when:
    - LLM_PROVIDER=dummy
    - Provider initialization fails
    - System wants deterministic, safe behavior
    
    Returns raw text that is safe and parseable by consuming code.
    For JSON-expecting consumers, returns valid JSON with empty arrays.
    """
    
    def __init__(self):
        super().__init__("dummy")

    def call(self, prompt: str) -> str:
        """Return safe empty JSON."""
        return json.dumps({"triples": []})


class OpenAIAdapter(BaseAdapter):
    """OpenAI adapter using the openai package.
    
    Wraps a single prompt -> text call.
    Returns raw text only; consuming code handles parsing.
    """
    
    def __init__(self, api_key: str = None, model: str = None, temperature: float = 0.0):
        model = model or os.environ.get("LLM_MODEL", "gpt-4o-mini")
        super().__init__("openai", {"model": model, "temperature": temperature})
        
        try:
            import openai
        except Exception as e:
            raise RuntimeError("openai package not available") from e

        self.openai = openai
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise RuntimeError("OPENAI_API_KEY not set")
        self.openai.api_key = self.api_key

    def call(self, prompt: str) -> str:
        """Call OpenAI and return raw text."""
        try:
            resp = self.openai.ChatCompletion.create(
                model=self.config.get("model"),
                messages=[{"role": "user", "content": prompt}],
                temperature=self.config.get("temperature", 0.0),
                max_tokens=800,
            )
            choices = resp.get("choices") or []
            if not choices:
                return ""
            return choices[0].get("message", {}).get("content", "")
        except Exception as e:
            logger.error(f"OpenAI call failed: {e}")
            raise


class NvidiaAdapter(BaseAdapter):
    """NVIDIA adapter using the OpenAI client with NVIDIA endpoint.
    
    Wraps a single prompt -> text call.
    Returns raw text only; consuming code handles parsing.
    """
    
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        model = model or os.environ.get("LLM_MODEL", "llama2")
        super().__init__(
            "nvidia",
            {"model": model}
        )

        try:
            from openai import OpenAI
        except Exception as e:
            raise RuntimeError("openai client not available") from e

        self.client = OpenAI(
            api_key=api_key or os.environ.get("NVIDIA_API_KEY"),
            base_url=base_url or os.environ.get("NVIDIA_BASE_URL")
        )

        if not self.client:
            raise RuntimeError("NVIDIA client init failed")

    def call(self, prompt: str) -> str:
        """Call NVIDIA and return raw text."""
        try:
            resp = self.client.chat.completions.create(
                model=self.config.get("model"),
                messages=[{"role": "user", "content": prompt}],
                temperature=0.1,
                max_tokens=1000
            )
            return resp.choices[0].message.content
        except Exception as e:
            logger.error(f"NVIDIA call failed: {e}")
            raise


def get_adapter(name: str, **kwargs) -> BaseAdapter:
    """Factory function to instantiate an adapter by name.
    
    Args:
        name: Provider name (openai, nvidia, dummy, etc.)
        **kwargs: Optional config (api_key, model, temperature, base_url)
    
    Returns:
        An adapter instance. Falls back to DummyAdapter on any error.
    """
    name = (name or "").lower().strip()

    if name in ("openai", "gpt", "gpt-4", "gpt-4o", "gpt-3.5-turbo"):
        try:
            return OpenAIAdapter(**kwargs)
        except Exception as e:
            logger.warning(f"OpenAI adapter failed, falling back to dummy: {e}")
            return DummyAdapter()

    if name in ("nvidia", "nv", "llama", "llama2"):
        try:
            return NvidiaAdapter(**kwargs)
        except Exception as e:
            logger.warning(f"NVIDIA adapter failed, falling back to dummy: {e}")
            return DummyAdapter()
    
    # Default to dummy for unknown providers or explicit "dummy"
    return DummyAdapter()
