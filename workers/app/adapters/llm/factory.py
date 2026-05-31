import logging
from app import config
from app.adapters.llm.base import BaseLLMProvider
from app.adapters.llm.openai import OpenAICompatibleProvider

logger = logging.getLogger("llm_factory")

def get_llm_provider() -> BaseLLMProvider:
    provider = config.LLM_PROVIDER
    if provider == "openai":
        return OpenAICompatibleProvider(
            api_url=config.OPENAI_API_URL,
            api_key=config.OPENAI_API_KEY,
            model_name=config.OPENAI_MODEL,
            provider_name="openai"
        )
    elif provider == "gemini":
        return OpenAICompatibleProvider(
            api_url=config.GEMINI_API_URL,
            api_key=config.GEMINI_API_KEY,
            model_name=config.GEMINI_MODEL,
            provider_name="gemini"
        )
    elif provider == "ollama":
        return OpenAICompatibleProvider(
            api_url=config.OLLAMA_API_URL,
            api_key="",
            model_name=config.OLLAMA_MODEL,
            provider_name="ollama"
        )
    else:
        # Default to Groq
        return OpenAICompatibleProvider(
            api_url=config.GROQ_API_URL,
            api_key=config.GROQ_API_KEY,
            model_name=config.GROQ_MODEL,
            provider_name="groq"
        )

async def fetch_ai_insights(transcript_text: str) -> dict:
    provider = get_llm_provider()
    return await provider.fetch_insights(transcript_text)
