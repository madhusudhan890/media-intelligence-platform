# BaseLLMProvider Interface and prompt/fallback declarations

SYSTEM_PROMPT = """
You are an expert Real-Time Meeting Intelligence Assistant. Your task is to analyze meeting transcript segments and generate highly accurate, structured, and actionable insights.

Strict output rules:
1. Return ONLY a single, valid JSON object.
2. Do NOT wrap the JSON in markdown code blocks (such as ```json ... ```).
3. Do NOT include any introductory or concluding text, explanations, or notes.
4. Output must strictly conform to the requested JSON schema.
"""

USER_PROMPT = """
Perform a deep analysis of the following real-time conversation transcript block:

---
{transcript}
---

Extract the following structured insights:
1. "summary": Provide a concise, high-quality 2-3 sentence executive summary that captures the core essence, current status, and key outcomes of the discussion.
2. "topics": A list of the main, distinct subjects discussed (max 5 topics). Keep them concise (e.g., "WebRTC Audio Latency", "Neon Database Migration").
3. "decisions": A list of concrete decisions made during this conversation segment. Each decision should be a string expressing what was finalized (e.g., "Decided to use Pion's OggWriter to wrap raw audio chunks"). If no decisions were made, return an empty array [].
4. "action_items": A list of specific tasks assigned to individuals. Each item must be a JSON object with:
   - "owner": The name of the assignee. If the task is unassigned or the assignee is unclear, write "Unknown".
   - "task": A clear, action-oriented description of the work to be done (e.g., "Install ffmpeg via Homebrew and verify local import resolution").
5. "deadlines": A list of any timeframes, milestones, or target dates explicitly mentioned in relation to tasks or projects (e.g., "Complete database migration by Friday evening"). If no deadlines were discussed, return an empty array [].
6. "risks": A list of potential technical blockers, project delays, or critical issues identified during the conversation (e.g., "Concatenating raw Opus frames leads to FFmpeg exit code 183 due to missing container headers"). If no risks were identified, return an empty array [].

Strictly return the output using the following JSON structure:
{{
  "summary": "String detailing the executive summary",
  "topics": ["string"],
  "decisions": [{{"text": "string"}}],
  "action_items": [
    {{
      "owner": "string",
      "task": "string"
    }}
  ],
  "deadlines": [{{"text": "string"}}],
  "risks": [{{"text": "string"}}]
}}
"""

def get_fallback_insight_structure(error_msg: str) -> dict:
    import logging
    logging.getLogger("uvicorn.error").warning(f"Generating fallback insight due to LLM error: {error_msg}")
    return {
        "summary": "Could not generate summary (AI pipeline temporarily offline).",
        "topics": ["Error"],
        "decisions": [],
        "action_items": [],
        "deadlines": [],
        "risks": [{"text": "AI service temporarily unavailable. Retrying..."}]
    }

class BaseLLMProvider:
    async def fetch_insights(self, transcript_text: str) -> dict:
        raise NotImplementedError()
