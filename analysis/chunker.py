import os
import json
from pathlib import Path

RAW_DIR = Path("../data/youtube_raw")
CHUNK_DIR = Path("../data/youtube_chunks")

CHUNK_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE = 500        # words per chunk
OVERLAP = 100           # overlap words


def chunk_text(text: str):
    words = text.split()
    chunks = []

    start = 0
    chunk_id = 0

    while start < len(words):
        end = start + CHUNK_SIZE
        chunk_words = words[start:end]

        chunk_text = " ".join(chunk_words)

        chunks.append({
            "chunk_id": chunk_id,
            "text": chunk_text
        })

        start += CHUNK_SIZE - OVERLAP
        chunk_id += 1

    return chunks


def process_all_transcripts():
    total_files = 0
    total_chunks = 0

    for file in RAW_DIR.glob("*.json"):
        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        transcript = data.get("transcript")

        if not transcript:
            continue

        chunks = chunk_text(transcript)

        output = {
            "video_id": data.get("video_id"),
            "title": data.get("title"),
            "chunks": chunks
        }

        output_path = CHUNK_DIR / f"{file.stem}_chunks.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        total_files += 1
        total_chunks += len(chunks)

    print("\n=== Chunking Summary ===")
    print(f"Processed files : {total_files}")
    print(f"Total chunks    : {total_chunks}")
    print("========================\n")


if __name__ == "__main__":
    process_all_transcripts()
