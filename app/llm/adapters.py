"""LLM Provider Adapters.

Adapters accept a prompt string and return raw text only.
They do not validate, parse, or interpret outputs.
All adapters follow the same minimal contract: call(prompt: str, **kwargs) -> str

Argument Filtering:
- Each adapter declares which kwargs it supports via SUPPORTED_KWARGS
- BaseAdapter.filter_kwargs() removes unsupported arguments silently
- Callers can pass a superset of kwargs (provider-agnostic)
- Each adapter only receives the parameters it actually supports
- This enables future-proof provider addition without conditionals
"""

import os
import json
import logging

logger = logging.getLogger(__name__)


class BaseAdapter:
    """Abstract base for all LLM adapters with built-in argument filtering."""
    
    # Subclasses override this to declare supported kwargs
    SUPPORTED_KWARGS = set()
    
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}

    @staticmethod
    def filter_init_kwargs(adapter_cls, **kwargs) -> dict:
        """Filter kwargs for adapter initialization.
        
        This is the primary safety mechanism: ensures no adapter constructor
        ever receives unsupported keyword arguments.
        
        Called by get_adapter() BEFORE constructing the adapter.
        
        Args:
            adapter_cls: The adapter class (e.g., NvidiaAdapter)
            **kwargs: Constructor arguments passed by caller
        
        Returns:
            dict: Filtered kwargs containing only what the adapter supports
        """
        filtered = {k: v for k, v in kwargs.items() if k in adapter_cls.SUPPORTED_KWARGS}
        
        if len(filtered) < len(kwargs):
            removed = set(kwargs.keys()) - set(filtered.keys())
            logger.debug(
                f"Filtered init kwargs for {adapter_cls.__name__}: removed {removed}"
            )
        
        return filtered

    def filter_kwargs(self, **kwargs) -> dict:
        """Filter kwargs to only include those supported by this adapter.
        
        This is runtime safety for call() arguments (secondary layer).
        Unsupported kwargs are silently removed, preventing errors.
        
        Args:
            **kwargs: Any keyword arguments passed by the caller
        
        Returns:
            dict: Filtered kwargs containing only supported arguments
        """
        filtered = {k: v for k, v in kwargs.items() if k in self.SUPPORTED_KWARGS}
        
        if len(filtered) < len(kwargs):
            removed = set(kwargs.keys()) - set(filtered.keys())
            logger.debug(
                f"{self.name} adapter ignored unsupported call kwargs: {removed}"
            )
        
        return filtered

    def call(self, prompt: str, **kwargs) -> str:
        """Call the LLM with a prompt and return raw text.
        
        Args:
            prompt: The prompt string.
            **kwargs: Optional parameters (temperature, max_tokens, top_p, etc.)
                     Unsupported kwargs are silently filtered out.
        
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
    
    Supports all common kwargs for compatibility (but doesn't use them).
    """
    
    SUPPORTED_KWARGS = {
        "temperature", "max_tokens", "top_p", "top_k", "presence_penalty",
        "frequency_penalty", "model", "api_key", "base_url"
    }
    
    def __init__(self):
        super().__init__("dummy")

    def call(self, prompt: str, **kwargs) -> str:
        """Return safe empty JSON (ignores all kwargs)."""
        return json.dumps({"triples": []})


class OpenAIAdapter(BaseAdapter):
    """OpenAI adapter using the openai package.
    
    Wraps a single prompt -> text call.
    Returns raw text only; consuming code handles parsing.
    
    Supports: temperature, max_tokens, top_p, presence_penalty, frequency_penalty
    (all parameters supported by OpenAI API)
    
    Note: Constructor only receives filtered kwargs that are in SUPPORTED_KWARGS.
    get_adapter() ensures this via filter_init_kwargs().
    """
    
    SUPPORTED_KWARGS = {
        "temperature", "max_tokens", "top_p", "presence_penalty", "frequency_penalty"
    }
    
    def __init__(self, api_key: str = None, model: str = None, temperature: float = 0.0, **kwargs):
        """Initialize OpenAI adapter.
        
        Args:
            api_key: OpenAI API key (or from env)
            model: Model name (or from env)
            temperature: Default temperature (or from env)
            **kwargs: Other params already filtered by get_adapter()
        """
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

    def call(self, prompt: str, **kwargs) -> str:
        """Call OpenAI and return raw text.
        
        Filters kwargs at runtime (double-check safety).
        """
        try:
            # Runtime safety: filter call-time kwargs
            filtered_kwargs = self.filter_kwargs(**kwargs)
            
            # Build API call parameters with defaults
            api_params = {
                "model": self.config.get("model"),
                "messages": [{"role": "user", "content": prompt}],
                "temperature": filtered_kwargs.get("temperature", self.config.get("temperature", 0.0)),
                "max_tokens": filtered_kwargs.get("max_tokens", 800),
            }
            
            # Add optional parameters if provided
            if "top_p" in filtered_kwargs:
                api_params["top_p"] = filtered_kwargs["top_p"]
            if "presence_penalty" in filtered_kwargs:
                api_params["presence_penalty"] = filtered_kwargs["presence_penalty"]
            if "frequency_penalty" in filtered_kwargs:
                api_params["frequency_penalty"] = filtered_kwargs["frequency_penalty"]
            
            resp = self.openai.ChatCompletion.create(**api_params)
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
    
    Note: NVIDIA API has limited parameter support compared to OpenAI.
    Supports: max_tokens only
    Does NOT support: temperature, top_p, presence_penalty, frequency_penalty
    (These are filtered out by get_adapter() before construction)
    
    Constructor only receives filtered kwargs via get_adapter().
    """
    
    SUPPORTED_KWARGS = {
        "max_tokens"
    }
    
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None, **kwargs):
        """Initialize NVIDIA adapter.
        
        Args:
            api_key: NVIDIA API key (or from env)
            base_url: NVIDIA API base URL (or from env)
            model: Model name (or from env)
            **kwargs: Other params (already filtered by get_adapter())
        """
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

    def call(self, prompt: str, **kwargs) -> str:
        """Call NVIDIA and return raw text.
        
        Filters kwargs at runtime (double-check safety).
        Only max_tokens is supported.
        """
        try:
            # Runtime safety: filter call-time kwargs
            filtered_kwargs = self.filter_kwargs(**kwargs)
            
            # Build API call parameters
            # Note: NVIDIA API does not support temperature parameter
            api_params = {
                "model": self.config.get("model"),
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": filtered_kwargs.get("max_tokens", 1000),
            }
            
            resp = self.client.chat.completions.create(**api_params)
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
    
    IMPORTANT: This function filters kwargs BEFORE construction to ensure
    no adapter constructor ever receives unsupported keyword arguments.
    """
    name = (name or "").lower().strip()

    if name in ("openai", "gpt", "gpt-4", "gpt-4o", "gpt-3.5-turbo"):
        try:
            # Filter kwargs to only those OpenAIAdapter supports
            filtered_kwargs = BaseAdapter.filter_init_kwargs(OpenAIAdapter, **kwargs)
            return OpenAIAdapter(**filtered_kwargs)
        except Exception as e:
            logger.warning(f"OpenAI adapter failed, falling back to dummy: {e}")
            return DummyAdapter()

    if name in ("nvidia", "nv", "llama", "llama2"):
        try:
            # Filter kwargs to only those NvidiaAdapter supports (only max_tokens)
            filtered_kwargs = BaseAdapter.filter_init_kwargs(NvidiaAdapter, **kwargs)
            return NvidiaAdapter(**filtered_kwargs)
        except Exception as e:
            logger.warning(f"NVIDIA adapter failed, falling back to dummy: {e}")
            return DummyAdapter()
    
    # Default to dummy for unknown providers or explicit "dummy"
    return DummyAdapter()
