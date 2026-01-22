from youtube_transcript_api import YouTubeTranscriptApi


def fetch_transcript(video_id: str) -> str | None:
    """
    Fetch transcript using YouTubeTranscriptApi instance method.
    Returns full transcript text or None.
    """

    try:
        ytt_api = YouTubeTranscriptApi()
        segments = ytt_api.fetch(video_id)

        if not segments:
            return None

        # segments are objects with .text
        return " ".join(seg.text for seg in segments)

    except Exception as e:
        print(f"[Transcript Error] {video_id}: {e}")
        return None
