import logging
from pathlib import Path
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
import glob

# Your other imports
from dotenv import load_dotenv
from caption_downloader import get_video_list, get_captions_only
from caption_analyzer import analyze_text, save_analysis_to_db

# --- 1. Enhanced Logging Setup ---
# (No changes needed here)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', datefmt='%H:%M:%S')
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("transformers").setLevel(logging.ERROR)


# --- 2. Configuration ---
# (No changes needed here)
load_dotenv()
CHANNEL_URL    = os.getenv("CHANNEL_URL", "https://www.youtube.com/c/YOUR_CHANNEL_NAME")
DOWNLOAD_LIMIT = int(os.getenv("DOWNLOAD_LIMIT", 30))
DOWNLOAD_DIR   = Path(os.getenv("DOWNLOAD_DIR", "downloads"))
DB_FILE        = "finance_data.db"
# Set the maximum number of parallel jobs
MAX_WORKERS = 5 


def process_single_video(video_details: dict, index: int, total: int):
    """
    Handles the entire download and analysis process for a single video.
    This function is designed to be run in a separate thread.
    """
    video_id = video_details.get("id")
    publish_date = video_details.get("upload_date")

    if not video_id or not publish_date:
        logging.warning(f"[{index}/{total}] Skipping an item due to missing video ID or publish date.")
        return

    try:
        logging.info(f"[{index}/{total}] Downloading transcript for video {video_id}...")
        transcript = get_captions_only(video_id, DOWNLOAD_DIR)

        if not transcript:
            logging.warning(f"[{index}/{total}] No captions found for video {video_id}. Skipping.")
            return

        logging.info(f"[{index}/{total}] Analyzing transcript for video {video_id}...")
        analysis = analyze_text(transcript, DB_FILE)
        
        logging.info(f"Analysis for video {video_id} complete:")
        print(json.dumps(analysis, indent=2))

        save_analysis_to_db(video_id, publish_date, analysis, DB_FILE)
        logging.info(f"💾 [{index}/{total}] Results for {video_id} saved to database.")

    except Exception as e:
        logging.error(f"🚨 Failed processing video {video_id} at index {index}. Error: {e}")


def cleanup_media_files():
    """Deletes leftover media files from the download directory."""
    logging.info("Cleaning up temporary media files...")
    # Patterns for files to delete (VTT subtitles, MP4s, etc.)
    patterns_to_delete = ["*.vtt", "*.mp4", "*.m4a", "*.webm"]
    files_deleted = 0
    
    for pattern in patterns_to_delete:
        # Search for files in the download directory
        for file_path in glob.glob(str(pattern),recursive=True):
            try:
                os.remove(f"{file_path}")
                logging.info(f"Deleted: {file_path}")
                files_deleted += 1
            except OSError as e:
                logging.error(f"Error deleting file {file_path}: {e}")
    
    if files_deleted == 0:
        logging.info("No temporary files found to delete.")
    else:
        logging.info(f"✅ Cleanup complete. Deleted {files_deleted} files.")


def process_all():
    """
    Main function to fetch video list and process them in parallel.
    """
    if not Path(DB_FILE).exists():
        logging.error(f"Database file '{DB_FILE}' not found. Please run the setup script first.")
        return

    DOWNLOAD_DIR.mkdir(exist_ok=True)
    
    video_details_list = get_video_list(CHANNEL_URL, DOWNLOAD_LIMIT)
    num_videos = len(video_details_list)
    
    if num_videos == 0:
        logging.info("No new videos found to process.")
        return

    logging.info(f"🔥 Found {num_videos} videos. Starting parallel processing with {MAX_WORKERS} workers...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Create a future for each video processing task
        futures = [
            executor.submit(process_single_video, video, i + 1, num_videos)
            for i, video in enumerate(video_details_list)
        ]
        # as_completed will yield futures as they finish
        for future in as_completed(futures):
            # You can check for results or exceptions here if needed
            try:
                future.result() 
            except Exception as e:
                logging.error(f"A video processing task failed: {e}")

    logging.info("🎉 All videos processed successfully!")
    
    # Run cleanup after all threads are done
    cleanup_media_files()

if __name__ == "__main__":
    process_all()