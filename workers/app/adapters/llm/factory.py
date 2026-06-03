import logging
from app import config
from app.adapters.llm.base import BaseLLMProvider
from app.adapters.llm.openai import OpenAICompatibleProvider

logger = logging.getLogger("uvicorn.error.llm_factory")


class LLMProviderFactory:
    """
    Reads LLM_PROVIDER from config and returns the matching provider instance.
    Add a new branch here to support a new backend without touching any usecase.
    """

    _PROVIDER_MAP = {
        "openai": lambda: OpenAICompatibleProvider(
            api_url=config.OPENAI_API_URL,
            api_key=config.OPENAI_API_KEY,
            model_name=config.OPENAI_MODEL,
            provider_name="openai",
        ),
        "gemini": lambda: OpenAICompatibleProvider(
            api_url=config.GEMINI_API_URL,
            api_key=config.GEMINI_API_KEY,
            model_name=config.GEMINI_MODEL,
            provider_name="gemini",
        ),
        "ollama": lambda: OpenAICompatibleProvider(
            api_url=config.OLLAMA_API_URL,
            api_key="",
            model_name=config.OLLAMA_MODEL,
            provider_name="ollama",
        ),
        "groq": lambda: OpenAICompatibleProvider(
            api_url=config.GROQ_API_URL,
            api_key=config.GROQ_API_KEY,
            model_name=config.GROQ_MODEL,
            provider_name="groq",
        ),
    }

    @classmethod
    def get_provider(cls) -> BaseLLMProvider:
        """Return a provider instance based on the LLM_PROVIDER env variable."""
        key = config.LLM_PROVIDER.lower()
        factory_fn = cls._PROVIDER_MAP.get(key)
        if factory_fn is None:
            logger.warning(
                f"Unknown LLM_PROVIDER '{key}' — falling back to Groq."
            )
            factory_fn = cls._PROVIDER_MAP["groq"]
        provider = factory_fn()
        logger.info(f"LLM provider selected: {provider.provider_name} / {provider.model_name}")
        return provider


# ---------------------------------------------------------------------------
# Convenience shim kept for any direct callers
# ---------------------------------------------------------------------------
async def fetch_ai_insights(transcript_text: str) -> dict:
    return await LLMProviderFactory.get_provider().fetch_insights(transcript_text)
