import os
import json
from google import genai
from google.genai import types

client = genai.Client(api_key="AIzaSyBkitDMvFodKpKYOT1c878umPIytDEGxbU")



SYSTEM_PROMPT = """
You are a financial content classifier.

Classify the transcript into exactly ONE category:

- crypto
- macro
- random

Return STRICT JSON only like:
{"domain": "crypto", "confidence": 0.95}
"""


def classify_domain(transcript: str) -> dict:
    prompt = f"""
Transcript:
\"\"\"
{transcript[:6000]}
\"\"\"
"""

    response = client.models.generate_content(
        model="gemini-2.5-flash-lite",
        config=types.GenerateContentConfig(
            temperature=0,
            system_instruction=SYSTEM_PROMPT
        ),
        contents=prompt,
    )

    if not response.text:
        raise RuntimeError("Empty response from Gemini")

    text = response.text.strip()

    # Gemini sometimes wraps JSON in backticks
    if text.startswith("```"):
        text = text.strip("`")
        text = text.replace("json", "").strip()

    try:
        return json.loads(text)
    except json.JSONDecodeError:
        raise RuntimeError(f"Invalid JSON from Gemini:\n{text}")
