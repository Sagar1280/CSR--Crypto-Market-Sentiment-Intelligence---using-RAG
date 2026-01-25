from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from datetime import datetime, timedelta, timezone
from qdrant_client.models import Filter, FieldCondition, Range
from data.api.live_price import get_live_prices
from data.api.FR_LC import get_fear_greed

# ===== CONFIG =====
COLLECTION_NAME = "csr_youtube_chunks"
TOP_K = 10
# ==================

live_price = get_live_prices()
fear_greed_value, fear_greed_classification = get_fear_greed()

print("Loading embedding model...")
model = SentenceTransformer("BAAI/bge-small-en")

print("Connecting to Qdrant...")
client = QdrantClient(host="localhost", port=6333)


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
    if "last week" in query:
        return 7
    if "today" in query:
        return 1
    if "evening" in query:
        return 1
    if "morning" in query:
        return 1
    if "afternoon" in query:
        return 1
    if "this week" in query:
        return 7
    if "this month" in query:
        return 30

    return 45  # default window


# =========================
# 🔹 MAIN SEARCH
# =========================

def search(query: str):
    print(f"\nQuery: {query}")

    # 🔹 Detect time window
    days_window = detect_time_window(query)

    cutoff_date = datetime.now(timezone.utc) - timedelta(days=days_window)
    cutoff_timestamp = cutoff_date.timestamp()   # 🔥 IMPORTANT FIX

    # 🔹 Encode query
    query_embedding = model.encode(
        "query: " + query,
        normalize_embeddings=True
    ).tolist()

    # 🔹 Qdrant time filter (numeric)
    query_filter = Filter(
        must=[
            FieldCondition(
                key="published_at",
                range=Range(
                    gte=cutoff_timestamp
                )
            )
        ]
    )

    # 🔹 Query Qdrant
    response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embedding,
        limit=TOP_K,
        query_filter=query_filter
    )

    print("\n=== Retrieval Summary ===")
    print(f"Time window: last {days_window} days")
    print("\n--- Top Relevant Chunks ---\n")

    if not response.points:
        print("No results found in selected time window.")
        return
 
    for point in response.points:
        payload = point.payload

        published_ts = payload.get("published_at")
        published_readable = datetime.fromtimestamp(
            published_ts, tz=timezone.utc
        ).isoformat() if published_ts else "Unknown"

        print(f"Title: {payload.get('title')}")
        print(f"Published: {published_readable}")
        print(f"Preview: {payload.get('text')}...")
        print("-" * 80)
    print(live_price)
    print(f"Fear & Greed Index: {fear_greed_value} ({fear_greed_classification})")
    print("=========================\n")


# =========================
# 🔹 CLI LOOP
# =========================

if __name__ == "__main__":
    while True:
        user_query = input("\nEnter query (or type 'exit'): ")

        if user_query.lower() == "exit":
            break

        search(user_query.lower())
