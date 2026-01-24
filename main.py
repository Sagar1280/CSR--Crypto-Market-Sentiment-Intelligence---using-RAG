import time

from ingestion.youtube.video_fetcher import fetch_videos_for_channel
from ingestion.youtube.transcript_fetcher import fetch_transcript
from config.youtube_channels import YOUTUBE_CHANNELS
from storage.youtube_raw_store import video_exists, save_video


def run_youtube_pipeline():

    total_videos_found = 0
    total_transcription_attempts = 0
    total_transcripts_success = 0
    total_files_saved = 0
    total_skipped = 0

    for channel_url in YOUTUBE_CHANNELS:
        print(f"\nChannel: {channel_url}")

        videos = fetch_videos_for_channel(channel_url)
        print(f"Videos found: {len(videos)}")

        total_videos_found += len(videos)

        for video in videos:
            video_id = video["video_id"]
            title = video["title"]

            if video_exists(video_id):
                total_skipped += 1
                print(f"⏭️  Skipped (already exists): {title}")
                continue

            print(f"\n🎥 Processing: {title}")
            total_transcription_attempts += 1

            transcript = fetch_transcript(video_id)

            if not transcript:
                print("❌ Transcript not available")
                time.sleep(3)
                continue

            total_transcripts_success += 1

            save_video(video, transcript)
            total_files_saved += 1

            print(f"✅ Transcript saved: {video_id}.json")

            # ---- Rate limiting ----
            time.sleep(1)

        print("-" * 70)

    # ---- Final Summary ----
    print("\n=== INGESTION SUMMARY ===")
    print(f"Total videos found           : {total_videos_found}")
    print(f"Total transcription attempts : {total_transcription_attempts}")
    print(f"Transcripts fetched success  : {total_transcripts_success}")
    print(f"Transcript files saved       : {total_files_saved}")
    print(f"Videos skipped (existing)    : {total_skipped}")
    print("===========================\n")


if __name__ == "__main__":
    run_youtube_pipeline()
