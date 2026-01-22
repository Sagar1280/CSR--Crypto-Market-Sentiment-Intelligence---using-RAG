import json
from openai import OpenAI

client = OpenAI()

SYSTEM_PROMPT = """
You are a financial content classifier.

Classify the transcript into exactly ONE category:

- crypto: cryptocurrencies, Bitcoin, Ethereum, altcoins, funding rates, open interest, exchanges, on-chain, crypto markets
- macro: stocks, bonds, interest rates, inflation, CPI, Fed, recession, DXY, global economy
- random: motivation, personal talk, clickbait, unrelated content

Respond ONLY with valid JSON.
"""


def classify_domain(transcript: str) -> dict:
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {
                "role": "user",
                "content": f"""
Transcript:
\"\"\"
{transcript[:6000]}
\"\"\"

Return JSON exactly like:
{{"domain": "crypto|macro|random", "confidence": 0.0}}
"""
            }
        ],
        temperature=0,
    )

    message = response.choices[0].message.content

    if not message:
        raise RuntimeError("Empty response from OpenAI")

    try:
        return json.loads(message)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Invalid JSON from model: {message}") from e
