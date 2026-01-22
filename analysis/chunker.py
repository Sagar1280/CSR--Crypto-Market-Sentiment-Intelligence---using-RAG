import json
from pathlib import Path
import nltk

RAW_DIR = Path("../data/youtube_raw")
CHUNK_DIR = Path("../data/youtube_chunks")

CHUNK_DIR.mkdir(parents=True, exist_ok=True)

CHUNK_SIZE_WORDS = 800
OVERLAP_WORDS = 150


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

            # Overlap
            overlap_words = chunk_text.split()[-OVERLAP_WORDS:]
            current_chunk = [" ".join(overlap_words)]
            current_word_count = len(overlap_words)

        current_chunk.append(sentence)
        current_word_count += word_count

    if current_chunk:
        chunks.append({
            "chunk_id": chunk_id,
            "text": " ".join(current_chunk)
        })

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

        chunks = chunk_text_sentence_aware(transcript)

        output = {
            "video_id": data.get("video_id"),
            "title": data.get("title"),
            "channel": data.get("channel"),
            "published_at": data.get("published_at"),   # ✅ NEW
            "url": data.get("url"),
            "chunks": chunks
        }

        output_path = CHUNK_DIR / f"{file.stem}_chunks.json"

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        total_files += 1
        total_chunks += len(chunks)

    print("\n=== Sentence-Aware Chunking Summary ===")
    print(f"Processed files : {total_files}")
    print(f"Total chunks    : {total_chunks}")
    print("========================\n")


if __name__ == "__main__":
    process_all_transcripts()
