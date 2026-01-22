import json
from pathlib import Path
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct


# ===== CONFIG =====
CHUNK_DIR = Path("../data/youtube_chunks")
COLLECTION_NAME = "csr_youtube_chunks"
EMBEDDING_DIM = 384  # bge-small-en output size
# ==================

# Load embedding model
print("Loading embedding model...")
model = SentenceTransformer("BAAI/bge-small-en")

# Connect to local Qdrant
client = QdrantClient(host="localhost", port=6333)

# Create collection if it doesn't exist
collections = client.get_collections().collections
existing_names = [c.name for c in collections]

if COLLECTION_NAME not in existing_names:
    print("Creating Qdrant collection...")
    client.create_collection(
        collection_name=COLLECTION_NAME,
        vectors_config=VectorParams(
            size=EMBEDDING_DIM,
            distance=Distance.COSINE,
        ),
    )
else:
    print("Collection already exists.")

def embed_and_store():
    point_id = 0
    total_chunks = 0

    for file in CHUNK_DIR.glob("*.json"):
        print(f"\nProcessing file: {file.name}")

        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        video_id = data.get("video_id")
        title = data.get("title")
        published_at = data.get("published_at")
        channel = data.get("channel")
        url = data.get("url")

        for chunk in data.get("chunks", []):
            text = chunk["text"]

            # Generate embedding
            embedding = model.encode(text).tolist()

            # Store in Qdrant
            client.upsert(
                 collection_name=COLLECTION_NAME,
                 points=[
                 PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "video_id": video_id,
                        "title": title,
                        "channel": channel,
                        "published_at": published_at,
                        "url": url,
                        "chunk_id": chunk["chunk_id"],
                        "text": text,
                        "source": "youtube",
                     },
                )
            ],
         )


            point_id += 1
            total_chunks += 1

    print("\n=== Embedding Summary ===")
    print(f"Total chunks embedded: {total_chunks}")
    print("=========================\n")

if __name__ == "__main__":
    embed_and_store()
