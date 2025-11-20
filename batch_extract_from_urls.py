import argparse
import csv
import math
import os
import sys
import tempfile
import time
from typing import Optional, List

import cv2
import numpy as np
from PIL import Image

from app import ensure_dir, embed_image_clip_to_npy


def safe_slug(text: str) -> str:
    s = (text or "").strip()
    allowed = []
    for ch in s:
        if ch.isalnum() or ch in " .-_()[]{}":
            allowed.append(ch)
        else:
            allowed.append("_")
    slug = "".join(allowed).strip()
    return slug or "item"


def read_urls(csv_path: str, column: Optional[str]) -> list[str]:
    urls: list[str] = []
    with open(csv_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if column and column in reader.fieldnames:
            for row in reader:
                u = (row.get(column) or "").strip()
                if u:
                    urls.append(u)
        else:
            # Fallback: first column
            f.seek(0)
            reader2 = csv.reader(f)
            for i, row in enumerate(reader2):
                if not row:
                    continue
                cell = row[0].strip()
                # skip header-like first row
                if i == 0 and cell.lower() in {"decoded_url", "url", "tvc"}:
                    continue
                if cell:
                    urls.append(cell)
    return urls


def download_video(url: str, dest_path: str) -> None:
    # Try requests first, fallback to urllib
    try:
        import requests  # type: ignore
        with requests.get(url, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return
    except Exception:
        pass

    # Fallback urllib
    try:
        from urllib.request import urlopen
        with urlopen(url, timeout=60) as r, open(dest_path, "wb") as f:
            while True:
                chunk = r.read(8192)
                if not chunk:
                    break
                f.write(chunk)
    except Exception as e:
        raise RuntimeError(f"Download failed: {e}")


def extract_frame_at_position(cap: cv2.VideoCapture, position: float) -> Optional[Image.Image]:
    """Extract a single frame at given position (0.0 = start, 0.5 = middle, 0.9 = near end)"""
    try:
        total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
        fps = cap.get(cv2.CAP_PROP_FPS)
        if total_frames <= 0 or fps <= 0:
            return None
        
        target_frame = int(total_frames * position)
        cap.set(cv2.CAP_PROP_POS_FRAMES, target_frame)
        success, frame = cap.read()
        
        if success and frame is not None:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            return Image.fromarray(rgb)
    except Exception:
        pass
    return None


def get_video_duration(cap: cv2.VideoCapture) -> float:
    """Get video duration in seconds"""
    try:
        fps = cap.get(cv2.CAP_PROP_FPS)
        frame_count = cap.get(cv2.CAP_PROP_FRAME_COUNT)
        if fps > 0 and frame_count > 0:
            return frame_count / fps
    except Exception:
        pass
    return 0.0


def calculate_frame_positions(duration_seconds: float) -> List[float]:
    """
    Calculate frame positions (0.0-1.0) to extract based on video duration.
    
    Strategy:
    - Video < 30s: 5 frames (0%, 25%, 50%, 75%, 90%)
    - Video 30s-2min: 10 frames (distributed evenly, avoid 100% to skip credits)
    - Video > 2min: 15 frames (distributed evenly, avoid 100%)
    """
    if duration_seconds < 30:
        # 5 frames: 0%, 25%, 50%, 75%, 90%
        return [0.0, 0.25, 0.5, 0.75, 0.9]
    elif duration_seconds < 120:  # 2 minutes
        # 10 frames: distributed evenly (0% to 90%)
        num_frames = 10
        return [i / (num_frames - 1) * 0.9 for i in range(num_frames)]
    else:
        # 15 frames: distributed evenly (0% to 90%)
        num_frames = 15
        return [i / (num_frames - 1) * 0.9 for i in range(num_frames)]


def extract_frames_distributed(cap: cv2.VideoCapture) -> List[Image.Image]:
    """
    Extract frames distributed evenly across video duration.
    Returns list of PIL Images.
    """
    frames = []
    duration = get_video_duration(cap)
    if duration <= 0:
        # Fallback: try to get first frame
        cap.set(cv2.CAP_PROP_POS_MSEC, 0)
        success, frame = cap.read()
        if success and frame is not None:
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            frames.append(Image.fromarray(rgb))
        return frames
    
    positions = calculate_frame_positions(duration)
    for pos in positions:
        img = extract_frame_at_position(cap, pos)
        if img:
            frames.append(img)
    
    return frames


def create_thumbnail_grid(frames: List[Image.Image], target_size: int = 224) -> Image.Image:
    """
    Create a grid thumbnail from multiple frames.
    
    Args:
        frames: List of PIL Images
        target_size: Size to resize each frame before arranging (default: 224)
    
    Returns:
        Combined thumbnail as PIL Image
    """
    if not frames:
        # Return a blank image if no frames
        return Image.new('RGB', (target_size, target_size), color='black')
    
    if len(frames) == 1:
        # Single frame: just resize
        return frames[0].resize((target_size, target_size), Image.Resampling.LANCZOS)
    
    # Calculate grid dimensions
    num_frames = len(frames)
    if num_frames <= 4:
        cols = 2
        rows = 2
    elif num_frames <= 9:
        cols = 3
        rows = 3
    elif num_frames <= 16:
        cols = 4
        rows = 4
    else:
        # For 15 frames: 4x4 grid (last cell empty)
        cols = 4
        rows = 4
    
    # Resize all frames to target_size
    resized_frames = []
    for frame in frames:
        # Resize maintaining aspect ratio, then crop to square
        frame.thumbnail((target_size, target_size), Image.Resampling.LANCZOS)
        # Create square image with black padding if needed
        square = Image.new('RGB', (target_size, target_size), color='black')
        # Center the frame
        x_offset = (target_size - frame.width) // 2
        y_offset = (target_size - frame.height) // 2
        square.paste(frame, (x_offset, y_offset))
        resized_frames.append(square)
    
    # Create grid
    grid_width = cols * target_size
    grid_height = rows * target_size
    grid = Image.new('RGB', (grid_width, grid_height), color='black')
    
    # Paste frames into grid
    for idx, frame in enumerate(resized_frames):
        if idx >= cols * rows:
            break
        row = idx // cols
        col = idx % cols
        x = col * target_size
        y = row * target_size
        grid.paste(frame, (x, y))
    
    return grid


def process_url(index: int, url: str, out_root: str, overwrite: bool, num_frames: int = 3) -> tuple[str, int, Optional[str]]:
    """
    Process a video URL: extract frames distributed across video, create thumbnail grid, and embed.
    
    Args:
        num_frames: 1 (fast, only first frame) or use distributed sampling (5-15 frames based on duration)
                   Note: num_frames > 1 now uses distributed sampling and creates thumbnail grid
    """
    job_id = f"url_{index:04d}"
    job_dir = os.path.join(out_root, job_id)
    
    # Output: single embedding file (from thumbnail grid)
    embed_path = os.path.join(job_dir, "thumbnail_embedding.npy")
    url_txt_path = os.path.join(job_dir, "url.txt")
    
    ensure_dir(job_dir)
    if overwrite:
        for path in [embed_path, url_txt_path]:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except Exception:
                pass

    # Persist the source URL into the job folder
    try:
        with open(url_txt_path, "w", encoding="utf-8") as f:
            f.write(url)
    except Exception:
        pass

    with tempfile.TemporaryDirectory() as tdir:
        # METHOD 1: Thử mở trực tiếp từ URL (không cần download)
        try:
            cap = cv2.VideoCapture(url)
            if cap.isOpened():
                if num_frames == 1:
                    # Fast mode: only first frame
                    cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                    success, frame = cap.read()
                    if success and frame is not None:
                        temp_path = os.path.join(tdir, "frame_0.png")
                        rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                        img = Image.fromarray(rgb)
                        img.save(temp_path)
                        embed_image_clip_to_npy(temp_path, embed_path, model_id="openai/clip-vit-base-patch32")
                        cap.release()
                        return job_id, 1, None
                else:
                    # New mode: extract frames distributed across video, create thumbnail, embed
                    frames = extract_frames_distributed(cap)
                    cap.release()
                    
                    if frames:
                        # Create thumbnail grid from frames
                        thumbnail = create_thumbnail_grid(frames, target_size=224)
                        thumbnail_path = os.path.join(tdir, "thumbnail.png")
                        thumbnail.save(thumbnail_path)
                        
                        # Embed the thumbnail
                        embed_image_clip_to_npy(thumbnail_path, embed_path, model_id="openai/clip-vit-base-patch32")
                        return job_id, len(frames), None
                    else:
                        return job_id, 0, "no_frames_extracted"
        except Exception:
            pass  # Nếu thất bại, thử phương án 2
        
        # METHOD 2: Download video nếu không mở được trực tiếp (dự phòng)
        tmp_path = os.path.join(tdir, safe_slug(os.path.basename(url)) or "video.mp4")
        try:
            download_video(url, tmp_path)
        except Exception as e:
            return job_id, 0, f"download_error: {e}"

        # Extract frames from downloaded video
        try:
            cap = cv2.VideoCapture(tmp_path)
            if not cap.isOpened():
                return job_id, 0, "video_open_error"
            
            if num_frames == 1:
                # Fast mode: only first frame
                cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                success, frame = cap.read()
                if success and frame is not None:
                    temp_path = os.path.join(tdir, "frame_0.png")
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    img = Image.fromarray(rgb)
                    img.save(temp_path)
                    embed_image_clip_to_npy(temp_path, embed_path, model_id="openai/clip-vit-base-patch32")
                    cap.release()
                    return job_id, 1, None
            else:
                # New mode: extract frames distributed across video, create thumbnail, embed
                frames = extract_frames_distributed(cap)
                cap.release()
                
                if frames:
                    # Create thumbnail grid from frames
                    thumbnail = create_thumbnail_grid(frames, target_size=224)
                    thumbnail_path = os.path.join(tdir, "thumbnail.png")
                    thumbnail.save(thumbnail_path)
                    
                    # Embed the thumbnail
                    embed_image_clip_to_npy(thumbnail_path, embed_path, model_id="openai/clip-vit-base-patch32")
                    return job_id, len(frames), None
                else:
                    return job_id, 0, "no_frames_extracted"
                
        except Exception as e:
            return job_id, 0, f"frame_extract_error: {e}"

    return job_id, 0, "unknown_error"


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch download videos from CSV URLs, extract frames and create CLIP embeddings")
    parser.add_argument("--input", default="url-tvc.unique.csv", help="CSV path containing URLs (default: url-tvc.unique.csv)")
    parser.add_argument("--column", default="decoded_url", help="Column name containing URLs (default: decoded_url)")
    parser.add_argument("--out_dir", default="batch_outputs", help="Output directory for per-URL frames (default: batch_outputs)")
    parser.add_argument("--start", type=int, default=0, help="Start index (inclusive) in URL list (default: 0)")
    parser.add_argument("--end", type=int, default=None, help="End index (exclusive) in URL list (default: None)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing frames for a job")
    parser.add_argument("--num_frames", type=int, default=1, 
                       help="Frame extraction mode: 1=fast (first frame only), >1=distributed sampling (5-15 frames based on video duration, creates thumbnail grid for better accuracy) (default: 1)")
    args = parser.parse_args()

    # Validate input file exists
    if not os.path.isfile(args.input):
        print(f"❌ ERROR: Input file not found: {args.input}", file=sys.stderr)
        print(f"Please make sure the file exists before running this script.", file=sys.stderr)
        sys.exit(1)

    ensure_dir(args.out_dir)
    urls = read_urls(args.input, args.column)
    
    if not urls:
        print(f"❌ ERROR: No URLs found in {args.input}", file=sys.stderr)
        print(f"Please check the file format and column name (current: '{args.column}').", file=sys.stderr)
        sys.exit(1)
    if args.end is None or args.end > len(urls):
        end = len(urls)
    else:
        end = args.end
    start = max(0, args.start)
    if start >= end:
        print("No URLs to process in the given range.", file=sys.stderr)
        sys.exit(1)

    mode_desc = "first frame only" if args.num_frames == 1 else "distributed frames (5-15 frames based on duration) -> thumbnail grid -> embed"
    print(f"🎬 Processing URLs {start}..{end-1} out of {len(urls)} total")
    print(f"📊 Mode: {mode_desc}")
    successes = 0
    failures = 0
    t0 = time.time()
    for i in range(start, end):
        url = urls[i]
        job_id, num_frames, err = process_url(i, url, args.out_dir, args.overwrite, args.num_frames)
        if err:
            failures += 1
            print(f"[{i}] {job_id} FAIL ({err})")
        else:
            successes += 1
            print(f"[{i}] {job_id} OK - {num_frames} frame(s)")

    dt = time.time() - t0
    print(f"\n✅ Done. OK={successes}, FAIL={failures}, elapsed={dt:.1f}s")


if __name__ == "__main__":
    main()


