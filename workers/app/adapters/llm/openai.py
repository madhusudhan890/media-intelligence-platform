import json
import logging
import httpx
import asyncio
from app.adapters.llm.base import BaseLLMProvider, SYSTEM_PROMPT, USER_PROMPT, get_fallback_insight_structure

logger = logging.getLogger("openai_llm_adapter")

class OpenAICompatibleProvider(BaseLLMProvider):
    def __init__(self, api_url: str, api_key: str, model_name: str, provider_name: str):
        self.api_url = api_url
        self.api_key = api_key
        self.model_name = model_name
        self.provider_name = provider_name

    async def fetch_insights(self, transcript_text: str) -> dict:
        if not self.api_key and self.provider_name != "ollama":
            logger.error(f"API key missing for provider '{self.provider_name}'.")
            return get_fallback_insight_structure(f"{self.provider_name} API key missing")

        formatted_user_prompt = USER_PROMPT.format(transcript=transcript_text)
        
        headers = {
            "Content-Type": "application/json"
        }
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"

        payload = {
            "model": self.model_name,
            "messages": [
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": formatted_user_prompt}
            ],
            "temperature": 0.2
        }
        
        if self.provider_name in ["groq", "openai"]:
            payload["response_format"] = {"type": "json_object"}

        max_attempts = 3
        for attempt in range(1, max_attempts + 1):
            try:
                async with httpx.AsyncClient(timeout=20.0) as client:
                    response = await client.post(self.api_url, json=payload, headers=headers)
                    if response.status_code != 200:
                        logger.warning(f"{self.provider_name} API returned {response.status_code}: {response.text}. Attempt {attempt}/{max_attempts}")
                        await asyncio.sleep(2)
                        continue

                    res_json = response.json()
                    content = res_json["choices"][0]["message"]["content"]
                    parsed_data = json.loads(content)
                    
                    required_keys = ["summary", "topics", "decisions", "action_items", "deadlines", "risks"]
                    for key in required_keys:
                        if key not in parsed_data:
                            parsed_data[key] = [] if key != "summary" else ""
                    return parsed_data
            except Exception as e:
                logger.warning(f"{self.provider_name} API attempt {attempt} failed: {e}")
                await asyncio.sleep(2)

        return get_fallback_insight_structure(f"AI service ({self.provider_name}) unavailable after retries")
