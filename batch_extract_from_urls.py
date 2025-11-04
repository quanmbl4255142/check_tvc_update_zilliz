import argparse
import csv
import os
import sys
import tempfile
import time
from typing import Optional

import cv2
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


def process_url(index: int, url: str, out_root: str, overwrite: bool) -> tuple[str, int, Optional[str]]:
    job_id = f"url_{index:04d}"
    job_dir = os.path.join(out_root, job_id)
    # Save only the embedding (no frame image saved in repo)
    first_embed_path = os.path.join(job_dir, "first_frame.npy")
    url_txt_path = os.path.join(job_dir, "url.txt")
    ensure_dir(job_dir)
    if overwrite:
        try:
            if os.path.exists(first_embed_path):
                os.remove(first_embed_path)
        except Exception:
            pass
        try:
            if os.path.exists(url_txt_path):
                os.remove(url_txt_path)
        except Exception:
            pass

    # Persist the source URL into the job folder
    try:
        with open(url_txt_path, "w", encoding="utf-8") as f:
            f.write(url)
    except Exception:
        pass

    # Download to temp file
    with tempfile.TemporaryDirectory() as tdir:
        tmp_path = os.path.join(tdir, safe_slug(os.path.basename(url)) or "video.mp4")
        try:
            download_video(url, tmp_path)
        except Exception as e:
            return job_id, 0, f"download_error: {e}"

        # Capture only the first frame (kept in temp only)
        try:
            cap = cv2.VideoCapture(tmp_path)
            if not cap.isOpened():
                return job_id, 0, "video_open_error"
            cap.set(cv2.CAP_PROP_POS_MSEC, 0)
            success, frame = cap.read()
            if not success or frame is None:
                cap.release()
                return job_id, 0, "read_first_frame_error"
            # Save first frame to temp path (not in repo) only to compute embedding
            temp_frame_path = os.path.join(tdir, "first_frame.png")
            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            img = Image.fromarray(rgb)
            img.save(temp_frame_path)
            cap.release()
        except Exception as e:
            return job_id, 0, f"first_frame_error: {e}"

        # Compute embedding for the first frame
        try:
            embed_image_clip_to_npy(temp_frame_path, first_embed_path, model_id="openai/clip-vit-base-patch32")
        except Exception as e:
            return job_id, 1, f"embed_error: {e}"

    return job_id, 1, None


def main() -> None:
    parser = argparse.ArgumentParser(description="Batch download videos from CSV URLs, save only the first frame and its CLIP embedding")
    parser.add_argument("--input", default="url-tvc.unique.csv", help="CSV path containing URLs (default: url-tvc.unique.csv)")
    parser.add_argument("--column", default="decoded_url", help="Column name containing URLs (default: decoded_url)")
    parser.add_argument("--out_dir", default="batch_outputs", help="Output directory for per-URL frames (default: batch_outputs)")
    parser.add_argument("--start", type=int, default=0, help="Start index (inclusive) in URL list (default: 0)")
    parser.add_argument("--end", type=int, default=None, help="End index (exclusive) in URL list (default: None)")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing frames for a job")
    args = parser.parse_args()

    ensure_dir(args.out_dir)
    urls = read_urls(args.input, args.column)
    if args.end is None or args.end > len(urls):
        end = len(urls)
    else:
        end = args.end
    start = max(0, args.start)
    if start >= end:
        print("No URLs to process in the given range.", file=sys.stderr)
        sys.exit(1)

    print(f"Processing URLs {start}..{end-1} out of {len(urls)} total (first frame only)")
    successes = 0
    failures = 0
    t0 = time.time()
    for i in range(start, end):
        url = urls[i]
        job_id, num_frames, err = process_url(i, url, args.out_dir, args.overwrite)
        if err:
            failures += 1
            print(f"[{i}] {job_id} FAIL ({err})")
        else:
            successes += 1
            print(f"[{i}] {job_id} OK - {num_frames} frame(s)")

    dt = time.time() - t0
    print(f"Done. OK={successes}, FAIL={failures}, elapsed={dt:.1f}s")


if __name__ == "__main__":
    main()


