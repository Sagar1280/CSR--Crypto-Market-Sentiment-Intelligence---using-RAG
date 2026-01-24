import json
from pathlib import Path
from sentence_transformers import SentenceTransformer
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, PayloadSchemaType
import uuid


# ===== CONFIG =====
BASE_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = BASE_DIR.parent
CHUNK_DIR = PROJECT_ROOT / "data" / "youtube_chunks"

COLLECTION_NAME = "csr_youtube_chunks"
EMBEDDING_DIM = 384 # BGE Small dimension
# ==================



print("Loading embedding model...")
model = SentenceTransformer("BAAI/bge-small-en")

print("Connecting to Qdrant...")
client = QdrantClient(host="localhost", port=6333)


# =========================
#   CREATE COLLECTION
# =========================

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

    # 🔥 Create numeric index for time filtering
    client.create_payload_index(
        collection_name=COLLECTION_NAME,
        field_name="published_at",
        field_schema=PayloadSchemaType.FLOAT
    )

else:
    print("Collection already exists.")


# =========================
# 🔹 GET EXISTING VIDEO IDS
# =========================

def get_existing_video_ids():
    existing_video_ids = set()

    scroll_result, _ = client.scroll(
        collection_name=COLLECTION_NAME,
        with_payload=True,
        limit=10000
    )

    for point in scroll_result:
        if point.payload is None:
            continue

        video_id = point.payload.get("video_id")
        if video_id:
            existing_video_ids.add(video_id)

    return existing_video_ids


# =========================
# 🔹 EMBED + STORE
# =========================

def embed_and_store():
    total_chunks = 0

    existing_video_ids = get_existing_video_ids()
    print(f"Already embedded videos: {len(existing_video_ids)}")

    for file in CHUNK_DIR.glob("*.json"):
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        video_id = data.get("video_id")

        # Skip already embedded videos
        if video_id in existing_video_ids:
            continue

        print(f"\nEmbedding video: {video_id}")

        title = data.get("title")
        channel = data.get("channel")
        url = data.get("url")

        # 🔥 Force numeric timestamp
        published_at = data.get("published_at")
        if published_at is not None:
            published_at = float(published_at)

        points = []

        for chunk in data.get("chunks", []):
            text = chunk["text"]
            chunk_id = chunk["chunk_id"]

            # 🔥 BGE best practice: prefix with "passage:"
            embedding = model.encode(
                "passage: " + text,
                normalize_embeddings=True
            ).tolist()

            raw_id = f"{video_id}_{chunk_id}"
            point_id = str(uuid.uuid5(uuid.NAMESPACE_DNS, raw_id))

            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload={
                        "video_id": video_id,
                        "title": title,
                        "channel": channel,
                        "published_at": published_at,  # numeric
                        "url": url,
                        "chunk_id": chunk_id,
                        "text": text,
                        "source": "youtube",
                    },
                )
            )

        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )

        total_chunks += len(points)

    print("\n=== Embedding Summary ===")
    print(f"New chunks embedded: {total_chunks}")
    print("=========================\n")


# =========================
# 🔹 ENTRY POINT
# =========================

if __name__ == "__main__":
    embed_and_store()
