import subprocess
from pathlib import Path
import json
import logging
import re

def _clean_transcript_text(raw_text: str) -> str:
    """
    Turns a raw VTT file content into a single, clean, readable string.
    """
    if not raw_text:
        return ""

    text_parts = []
    for line in raw_text.strip().splitlines():
        if not line.strip() or "WEBVTT" in line or ("-->" in line and "align:start" not in line):
            continue
        
        # ❗ FIX: The hyphen in '-->' is now escaped with a backslash ' \- '
        clean_line = re.sub(r'^[\d:.,\s\-->]+align:start position:\d+%\s*', '', line)
        text_parts.append(clean_line.strip())

    unique_parts = list(dict.fromkeys(text_parts))
    
    return " ".join(unique_parts)


def get_video_list(channel_url: str, limit: int) -> list:
    logging.info(f"Fetching last {limit} video details from channel...")
    cmd = [
        "yt-dlp", "--print-json", "--playlist-end", str(limit),
        "--dateafter", "now-1year", "--no-warnings", channel_url
    ]
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    return [json.loads(line) for line in result.stdout.strip().split('\n')]

def get_captions_only(video_id: str, download_dir: Path) -> str | None:
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    output_template = download_dir / f"{video_id}"
    caption_file = output_template.with_suffix(".en.vtt")

    try:
        cmd_caption = [
            "yt-dlp", "--write-subs", "--write-auto-subs", "--sub-langs", "en.*",
            "--skip-download", "--output", str(output_template), "--no-warnings",
            video_url
        ]
        subprocess.run(cmd_caption, check=True, capture_output=True, timeout=60)

        if caption_file.exists():
            with open(caption_file, 'r', encoding='utf-8') as f:
                raw_text = f.read()
            
            clean_text = _clean_transcript_text(raw_text)

            caption_file.unlink()
            
            return clean_text

    except Exception as e:
        logging.error(f"Could not download or clean captions for {video_id}. Error: {e}")
        if caption_file.exists():
            caption_file.unlink()
        return None
    
    return None