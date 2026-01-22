import json
from pathlib import Path
from datetime import datetime, timezone


BASE_DIR = Path("data/youtube_raw")
BASE_DIR.mkdir(parents=True, exist_ok=True)


def video_exists(video_id: str) -> bool:
    """
    Check if video already stored.
    """
    return (BASE_DIR / f"{video_id}.json").exists()


def save_video(video: dict, transcript: str, transcript_source: str = "youtube"):
    """
    Save video metadata + transcript to disk.
    """
    payload = {
        "video_id": video["video_id"],
        "title": video["title"],
        "channel": video["channel"],
        "published_at": video["published_at"],
        "url": video["url"],
        "is_live": video.get("is_live", False),
        "was_live": video.get("was_live", False),

        "transcript": transcript,
        "transcript_source": transcript_source,

        "fetched_at": datetime.now(timezone.utc).isoformat()
    }

    path = BASE_DIR / f"{video['video_id']}.json"

    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
