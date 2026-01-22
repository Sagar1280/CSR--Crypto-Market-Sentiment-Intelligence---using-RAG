from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient

# ===== CONFIG =====
COLLECTION_NAME = "csr_youtube_chunks"
TOP_K = 5
# ==================

print("Loading embedding model...")
model = SentenceTransformer("BAAI/bge-small-en")

print("Connecting to Qdrant...")
client = QdrantClient(host="localhost", port=6333)

def search(query: str):
    print(f"\nQuery: {query}")

    # IMPORTANT: Use prefix + normalization for BGE
    query_embedding = model.encode(
        "query: " + query,
        normalize_embeddings=True
    ).tolist()

    response = client.query_points(
        collection_name=COLLECTION_NAME,
        query=query_embedding,
        limit=TOP_K
    )

    print("\nTop Results:\n")

    for i, point in enumerate(response.points):
        print(f"Result {i+1}")
        print(f"Score: {point.score:.4f}")
        print(f"Title: {point.payload.get('title')}")
        print(f"Text Preview: {point.payload.get('text')[:300]}...")

if __name__ == "__main__":
    while True:
        user_query = input("\nEnter your query (or type 'exit'): ")

        if user_query.lower() == "exit":
            break

        search(user_query)
