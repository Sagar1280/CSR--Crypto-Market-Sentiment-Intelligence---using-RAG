
import json
from pathlib import Path
from datetime import datetime, timezone

REGISTRY_PATH = Path("data/processed_videos.json")


def load_registry():
    if not REGISTRY_PATH.exists():
        return {}
    with open(REGISTRY_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_registry(registry):
    REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(REGISTRY_PATH, "w", encoding="utf-8") as f:
        json.dump(registry, f, indent=2)


def is_processed(video_id: str) -> bool:
    registry = load_registry()
    return video_id in registry


def mark_processed(video_id: str):
    registry = load_registry()
    registry[video_id] = {
        "processed_at": datetime.now(timezone.utc).isoformat()
    }
    save_registry(registry)
