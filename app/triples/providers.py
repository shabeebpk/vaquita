"""Provider adapters for triple extraction.

Adapters accept a prompt string and return raw text. They do not validate or parse JSON.
"""
import os
import json
import logging

logger = logging.getLogger(__name__)


class BaseAdapter:
    def __init__(self, name: str, config: dict = None):
        self.name = name
        self.config = config or {}

    def call(self, prompt: str) -> str:
        raise NotImplementedError()


class DummyAdapter(BaseAdapter):
    """A safe adapter that returns an empty triples JSON."""
    def __init__(self):
        super().__init__("dummy")

    def call(self, prompt: str) -> str:
        return json.dumps({"triples": []})


class OpenAIAdapter(BaseAdapter):
    """Minimal OpenAI adapter using `openai` package if available.

    This adapter only wraps a single prompt -> text call.
    """
    def __init__(self, api_key: str = None, model: str = "gpt-4o-mini", temperature: float = 0.0):
        super().__init__("openai", {"model": model or os.environ.get("OPENAI_MODEL"), "temperature": temperature})
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


class NvidiaAdapter(BaseAdapter):
    def __init__(self, api_key: str = None, base_url: str = None, model: str = None):
        super().__init__(
            "nvidia",
            {"model": model or os.environ.get("NVIDIA_MODEL")}
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
        resp = self.client.chat.completions.create(
            model=self.config.get("model"),
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
            max_tokens=1000
        )
        return resp.choices[0].message.content



def get_adapter(name: str):
    name = (name or "").lower()

    if name in ("openai", "gpt", "gpt-4", "gpt-4o"):
        try:
            return OpenAIAdapter()
        except Exception as e:
            logger.warning("OpenAI adapter not available, falling back to dummy: %s", e)
            return DummyAdapter()

    if name in ("nvidia", "nv", "llama"):
        try:
            return NvidiaAdapter()
        except Exception as e:
            logger.warning("Nvidia adapter not available, falling back to dummy: %s", e)
            return DummyAdapter()
            
    return DummyAdapter()