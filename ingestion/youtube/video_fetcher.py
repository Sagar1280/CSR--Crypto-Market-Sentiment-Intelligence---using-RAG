
from datetime import datetime, timedelta, timezone
from typing import List, Dict
import subprocess
import json
from concurrent.futures import ThreadPoolExecutor, as_completed

from config.youtube_channels import (
    LOOKBACK_DAYS,
    MAX_VIDEOS_PER_CHANNEL,
    MAX_LIVES_PER_CHANNEL,
)


def _run_yt_dlp(cmd):
    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True
    )
    if process.stdout is None:
        return []
    return [json.loads(line) for line in process.stdout if line.strip()]


def _extract_publish_time(data):
    if data.get("release_timestamp"):
        return datetime.fromtimestamp(data["release_timestamp"], tz=timezone.utc)
    if data.get("timestamp"):
        return datetime.fromtimestamp(data["timestamp"], tz=timezone.utc)
    if data.get("upload_date"):
        return datetime.strptime(data["upload_date"], "%Y%m%d").replace(tzinfo=timezone.utc)
    return None


def _fetch_metadata(video_id: str):
    meta_cmd = [
        "yt-dlp",
        "--dump-json",
        "--skip-download",
        f"https://www.youtube.com/watch?v={video_id}"
    ]
    meta = _run_yt_dlp(meta_cmd)
    return meta[0] if meta else None


def fetch_videos_for_channel(channel_url: str) -> List[Dict]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=LOOKBACK_DAYS)

    uploads = _run_yt_dlp([
        "yt-dlp", "--dump-json", "--flat-playlist",
        "--playlist-items", f"1-{MAX_VIDEOS_PER_CHANNEL}",
        f"{channel_url}/videos"
    ])

    streams = _run_yt_dlp([
        "yt-dlp", "--dump-json", "--flat-playlist",
        "--playlist-items", f"1-{MAX_LIVES_PER_CHANNEL}",
        f"{channel_url}/streams"
    ])

    candidates = {}
    for e in uploads + streams:
        if e.get("id"):
            candidates[e["id"]] = e

    videos = []

    with ThreadPoolExecutor(max_workers=6) as pool:
        futures = {}

        for vid, entry in candidates.items():
            rough_time = _extract_publish_time(entry)
            if rough_time and rough_time < cutoff:
                continue

            if rough_time:
                videos.append({
                    "video_id": vid,
                    "title": entry.get("title"),
                    "channel": entry.get("channel"),
                    "published_at": rough_time.isoformat(),
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "is_live": entry.get("is_live", False),
                    "was_live": entry.get("was_live", False),
                })
            else:
                futures[pool.submit(_fetch_metadata, vid)] = vid

        for future in as_completed(futures):
            data = future.result()
            if not data:
                continue

            published = _extract_publish_time(data)
            if not published or published < cutoff:
                continue

            videos.append({
                "video_id": data["id"],
                "title": data.get("title"),
                "channel": data.get("channel"),
                "published_at": published.isoformat(),
                "url": data.get("webpage_url"),
                "is_live": data.get("is_live", False),
                "was_live": data.get("was_live", False),
            })

    return videos
