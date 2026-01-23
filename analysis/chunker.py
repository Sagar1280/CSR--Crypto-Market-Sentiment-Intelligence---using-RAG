import json
from pathlib import Path
import nltk
from datetime import datetime
from dateutil import parser

# ===== DIRECTORIES =====
RAW_DIR = Path("../data/youtube_raw")
CHUNK_DIR = Path("../data/youtube_chunks")

CHUNK_DIR.mkdir(parents=True, exist_ok=True)

# ===== CONFIG =====
CHUNK_SIZE_WORDS = 800
OVERLAP_WORDS = 150


# =========================
# 🔹 SENTENCE-AWARE CHUNKER
# =========================

def chunk_text_sentence_aware(text: str):
    sentences = nltk.sent_tokenize(text)

    chunks = []
    current_chunk = []
    current_word_count = 0
    chunk_id = 0

    for sentence in sentences:
        word_count = len(sentence.split())

        if current_word_count + word_count > CHUNK_SIZE_WORDS:
            chunk_text = " ".join(current_chunk)

            chunks.append({
                "chunk_id": chunk_id,
                "text": chunk_text
            })

            chunk_id += 1

            # Overlap logic
            overlap_words = chunk_text.split()[-OVERLAP_WORDS:]
            current_chunk = [" ".join(overlap_words)]
            current_word_count = len(overlap_words)

        current_chunk.append(sentence)
        current_word_count += word_count

    # Save final chunk
    if current_chunk:
        chunks.append({
            "chunk_id": chunk_id,
            "text": " ".join(current_chunk)
        })

    return chunks


# =========================
# 🔹 MAIN PROCESSOR
# =========================

def process_all_transcripts():
    total_files = 0
    total_chunks = 0

    for file in RAW_DIR.glob("*.json"):
        output_path = CHUNK_DIR / f"{file.stem}_chunks.json"

        with open(file, "r", encoding="utf-8") as f:
            data = json.load(f)

        transcript = data.get("transcript")
        if not transcript:
            continue

        # 🔥 Convert published_at → UNIX timestamp (float)
        published_at_iso = data.get("published_at")

        if published_at_iso:
            published_dt = parser.parse(published_at_iso)
            published_timestamp = published_dt.timestamp()
        else:
            published_timestamp = None

        chunks = chunk_text_sentence_aware(transcript)

        output = {
            "video_id": data.get("video_id"),
            "title": data.get("title"),
            "channel": data.get("channel"),
            "published_at": published_timestamp,          # ← numeric
            "published_at_readable": published_at_iso,    # ← human readable
            "url": data.get("url"),
            "is_live": data.get("is_live"),
            "was_live": data.get("was_live"),
            "fetched_at": data.get("fetched_at"),
            "chunks": chunks
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        total_files += 1
        total_chunks += len(chunks)

    print("\n=== Sentence-Aware Chunking Summary ===")
    print(f"Processed files  : {total_files}")
    print(f"Total chunks     : {total_chunks}")
    print("========================\n")


# =========================
# 🔹 ENTRY POINT
# =========================

if __name__ == "__main__":
    process_all_transcripts()
