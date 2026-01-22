from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from datetime import datetime, timedelta ,timezone
from dateutil import parser

# ===== CONFIG =====
COLLECTION_NAME = "csr_youtube_chunks"
TOP_K = 20
# ==================

print("Loading embedding model...")
model = SentenceTransformer("BAAI/bge-small-en")

print("Connecting to Qdrant...")
client = QdrantClient(host="localhost", port=6333)


# =========================
# 🔹 SENTIMENT KEYWORDS
# =========================

BULLISH_WORDS = [
    "bullish", "breakout", "rally", "accumulation",
    "higher high", "support holding", "uptrend",
    "long position", "buying pressure", "bounce",
    "strength", "recovery", "short squeeze",
    "reversal to upside"
]

BEARISH_WORDS = [
    "bearish", "breakdown", "correction", "crash",
    "lower low", "resistance holding", "downtrend",
    "liquidation", "sell-off", "rejection",
    "weakness", "risk-off", "capitulation",
    "distribution"
]


def classify_sentiment(text: str):
    text = text.lower()

    bull_score = sum(word in text for word in BULLISH_WORDS)
    bear_score = sum(word in text for word in BEARISH_WORDS)

    if bull_score > bear_score:
        return "bullish"
    elif bear_score > bull_score:
        return "bearish"
    else:
        return "neutral"


# =========================
# 🔹 RECENCY WEIGHT
# =========================

def recency_weight(published_at):
    if not published_at:
        return 0

    published_date = parser.parse(published_at)
    days_old = (datetime.now(timezone.utc) - published_date).days

    return 1 / (days_old + 1)


# =========================
# 🔹 TIME WINDOW DETECTION
# =========================

def detect_time_window(query: str):
    query = query.lower()

    if "past few days" in query:
        return 5
    if "fortnight" in query:
        return 14
    if "1 month" in query:
        return 30
    if "7 days" in query:
        return 7
    if "1 week" in query:
        return 7
    if "24 hours" in query:
        return 1
    if "yesterday" in query:
        return 1
    if "48 hours" in query:
        return 2
    if "72 hours" in query:    
        return 3
    if "15 days" in query:
        return 15
    if "3 months" in query:
        return 90
    if "last week" in query:
        return 7
    if "today" in query:
        return 1

    return 30  # default


# =========================
# 🔹 MAIN SEARCH
# =========================

def search(query: str):
    print(f"\nQuery: {query}")

    # Detect time window
    days_window = detect_time_window(query)
    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_window)

    query_embedding = model.encode(
        "query: " + query,
        normalize_embeddings=True
    ).tolist()

    response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embedding,
        limit=TOP_K
    )

    sentiment_counts = {"bullish": 0, "bearish": 0, "neutral": 0}
    weighted_score = 0

    for point in response.points:
        published_at = point.payload.get("published_at")

        if published_at:
            pub_date = parser.parse(published_at)
            if pub_date < cutoff_date:
                continue  # skip old content


      
        text = point.payload.get("text")

        sentiment = classify_sentiment(text)
        weight = recency_weight(published_at)

        sentiment_counts[sentiment] += 1

        if sentiment == "bullish":
            weighted_score += weight
        elif sentiment == "bearish":
            weighted_score -= weight

    print("\n=== Sentiment Summary ===")
    print(f"Time window: last {days_window} days")
    print(sentiment_counts)

    if weighted_score > 0:
        print("Overall Bias: Bullish")
    elif weighted_score < 0:
        print("Overall Bias: Bearish")
    else:
        print("Overall Bias: Neutral")

    print("\n--- Top Relevant Chunks ---\n")

    for point in response.points[:5]:
        print(f"Title: {point.payload.get('title')}")
        print(f"Published: {point.payload.get('published_at')}")
        print(f"Preview: {point.payload.get('text')[:200]}...")
        print("-" * 80)


# =========================
# 🔹 CLI LOOP
# =========================

if __name__ == "__main__":
    while True:
        user_query = input("\nEnter query (or type 'exit'): ")

        if user_query.lower() == "exit":
            break

        search(user_query.lower())
