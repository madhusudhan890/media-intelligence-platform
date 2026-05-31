# BaseLLMProvider Interface and prompt/fallback declarations

SYSTEM_PROMPT = """
You are a realtime meeting intelligence assistant.

Return ONLY valid JSON.

No markdown.
No explanations.
No extra text.
"""

USER_PROMPT = """
Analyze this realtime conversation transcript:

{transcript}

Extract:

1. Important discussion topics
2. Decisions made
3. Action items
4. Deadlines mentioned
5. Risks/issues discussed

Return EXACTLY this JSON format:

{{
"summary": "short summary",
"topics": ["topic1"],
"decisions": [{{"text": "decision"}}],
"action_items": [
{{
"owner": "name or Unknown",
"task": "task description"
}}
],
"deadlines": [{{"text": "deadline mentioned"}}],
"risks": [{{"text": "risk or issue"}}
]}}
"""

def get_fallback_insight_structure(error_msg: str) -> dict:
    return {
        "summary": f"Could not generate summary ({error_msg}).",
        "topics": ["Error"],
        "decisions": [],
        "action_items": [],
        "deadlines": [],
        "risks": [{"text": error_msg}]
    }

class BaseLLMProvider:
    async def fetch_insights(self, transcript_text: str) -> dict:
        raise NotImplementedError()
