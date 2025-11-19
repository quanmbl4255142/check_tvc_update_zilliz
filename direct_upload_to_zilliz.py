"""
Direct upload CSV URLs to Zilliz Cloud
Extract embeddings on-the-fly and upload immediately (no persistent storage)
"""

import argparse
import csv
import os
import sys
import tempfile
import time
import logging
from typing import Optional, List

import cv2
import numpy as np
from PIL import Image
from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
)

from app import embed_image_clip_to_npy
from milvus_config import (
    get_connection_params,
    EMBEDDING_DIM,
    MAX_URL_LENGTH,
    MAX_JOB_ID_LENGTH,
    INDEX_PARAMS,
    BATCH_SIZE,
    CONSISTENCY_LEVEL,
    print_config,
)


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


def read_urls_from_csv(csv_path: str, column: Optional[str]) -> List[str]:
    """Read URLs from CSV file"""
    urls: List[str] = []
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
                # skip header
                if i == 0 and cell.lower() in {"decoded_url", "url", "tvc"}:
                    continue
                if cell:
                    urls.append(cell)
    return urls


def download_video(url: str, dest_path: str, max_retries: int = 3, timeout: int = 120) -> None:
    """
    Download video from URL with retry and better error handling
    
    Args:
        url: Video URL
        dest_path: Destination file path
        max_retries: Maximum number of retry attempts
        timeout: Timeout in seconds (default: 120)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Sec-Fetch-Dest': 'video',
        'Sec-Fetch-Mode': 'no-cors',
        'Sec-Fetch-Site': 'cross-site',
        'Referer': 'https://www.google.com/',
        'Origin': 'https://www.google.com',
    }
    
    # For Google CDN URLs, try to extract referer from URL or use default
    if 'gcdn.2mdn.net' in url or 'googlevideo.com' in url or 'googleusercontent.com' in url:
        headers['Referer'] = 'https://www.google.com/'
        headers['Origin'] = 'https://www.google.com'
        # Try to preserve IP from URL if present
        # Note: Google CDN URLs often have ip/0.0.0.0 which means any IP, but server may still check
    
    last_error = None
    
    # Try with requests first (better error handling)
    for attempt in range(max_retries):
        try:
            import requests
            response = requests.get(
                url, 
                stream=True, 
                timeout=timeout,
                headers=headers,
                allow_redirects=True
            )
            response.raise_for_status()
            
            # Check content type
            content_type = response.headers.get('Content-Type', '').lower()
            if 'video' not in content_type and 'application/octet-stream' not in content_type:
                if 'image' in content_type:
                    raise RuntimeError(f"URL points to image, not video: {content_type}")
            
            with open(dest_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Verify file was written
            if os.path.getsize(dest_path) == 0:
                raise RuntimeError("Downloaded file is empty")
            
            return  # Success
            
        except requests.exceptions.Timeout as e:
            last_error = f"Timeout after {timeout}s (attempt {attempt + 1}/{max_retries})"
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)  # Exponential backoff
                continue
        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP {e.response.status_code}: {str(e)}"
            if e.response.status_code >= 500 and attempt < max_retries - 1:
                # Retry on server errors
                time.sleep(2 ** attempt)
                continue
            raise RuntimeError(last_error)
        except requests.exceptions.RequestException as e:
            last_error = f"Request error: {str(e)}"
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
        except Exception as e:
            last_error = f"Unexpected error: {str(e)}"
            if attempt < max_retries - 1:
                time.sleep(2 ** attempt)
                continue
    
    # Fallback to urllib if requests failed
    try:
        from urllib.request import Request, urlopen
        req = Request(url, headers=headers)
        with urlopen(req, timeout=timeout) as r, open(dest_path, "wb") as f:
            while True:
                chunk = r.read(8192)
                if not chunk:
                    break
                f.write(chunk)
        
        if os.path.getsize(dest_path) == 0:
            raise RuntimeError("Downloaded file is empty")
        
        return  # Success with urllib
        
    except Exception as e:
        error_msg = f"Download failed after {max_retries} retries. Last error: {last_error or str(e)}"
        raise RuntimeError(error_msg)


def extract_first_frame_embedding(url: str, verbose: bool = False) -> tuple[Optional[np.ndarray], Optional[str]]:
    """
    Extract first frame from video URL and return embedding
    
    Args:
        url: Video URL
        verbose: If True, return error message along with None
    
    Returns:
        (embedding, error_message) - embedding is None if failed, error_message is None if success
    """
    error_messages = []
    
    with tempfile.TemporaryDirectory() as tdir:
        # METHOD 1: Try opening directly from URL
        try:
            cap = cv2.VideoCapture(url)
            if cap.isOpened():
                # Set timeout for reading (some URLs hang)
                cap.set(cv2.CAP_PROP_OPEN_TIMEOUT_MSEC, 30000)  # 30 seconds
                cap.set(cv2.CAP_PROP_READ_TIMEOUT_MSEC, 30000)  # 30 seconds
                
                cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                success, frame = cap.read()
                
                if success and frame is not None:
                    # Check if frame is valid
                    if frame.size == 0:
                        error_messages.append("Frame is empty")
                        cap.release()
                    else:
                        temp_img_path = os.path.join(tdir, "frame.png")
                        temp_npy_path = os.path.join(tdir, "embedding.npy")
                        
                        try:
                            rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                            img = Image.fromarray(rgb)
                            img.save(temp_img_path)
                            
                            # Create embedding
                            embed_image_clip_to_npy(temp_img_path, temp_npy_path, model_id="openai/clip-vit-base-patch32")
                            
                            # Load and normalize
                            vec = np.load(temp_npy_path).astype(np.float32).flatten()
                            norm = np.linalg.norm(vec)
                            if norm > 0:
                                vec = vec / norm
                                cap.release()
                                return (vec, None)
                            else:
                                error_messages.append("Embedding norm is zero")
                        except Exception as e:
                            error_messages.append(f"Embedding creation failed: {str(e)}")
                        
                        cap.release()
                else:
                    error_messages.append("Failed to read frame from URL (direct access)")
                    cap.release()
            else:
                error_messages.append("Failed to open URL with OpenCV (direct access)")
        except cv2.error as e:
            error_messages.append(f"OpenCV error (direct access): {str(e)}")
        except Exception as e:
            error_messages.append(f"Unexpected error (direct access): {str(e)}")
        
        # METHOD 2: Download video if direct access fails
        tmp_video_path = os.path.join(tdir, safe_slug(os.path.basename(url)) or "video.mp4")
        try:
            download_video(url, tmp_video_path, max_retries=3, timeout=120)
        except RuntimeError as e:
            error_msg = f"Download failed: {str(e)}"
            error_messages.append(error_msg)
            if verbose:
                return (None, "; ".join(error_messages))
            return (None, error_msg)
        except Exception as e:
            error_msg = f"Download error: {str(e)}"
            error_messages.append(error_msg)
            if verbose:
                return (None, "; ".join(error_messages))
            return (None, error_msg)
        
        # Extract frame from downloaded video
        try:
            cap = cv2.VideoCapture(tmp_video_path)
            if not cap.isOpened():
                error_msg = "Failed to open downloaded video file"
                error_messages.append(error_msg)
                if verbose:
                    return (None, "; ".join(error_messages))
                return (None, error_msg)
            
            cap.set(cv2.CAP_PROP_POS_MSEC, 0)
            success, frame = cap.read()
            
            if success and frame is not None:
                if frame.size == 0:
                    error_msg = "Frame from downloaded video is empty"
                    error_messages.append(error_msg)
                    cap.release()
                    if verbose:
                        return (None, "; ".join(error_messages))
                    return (None, error_msg)
                
                temp_img_path = os.path.join(tdir, "frame.png")
                temp_npy_path = os.path.join(tdir, "embedding.npy")
                
                try:
                    rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
                    img = Image.fromarray(rgb)
                    img.save(temp_img_path)
                    
                    # Create embedding
                    embed_image_clip_to_npy(temp_img_path, temp_npy_path, model_id="openai/clip-vit-base-patch32")
                    
                    # Load and normalize
                    vec = np.load(temp_npy_path).astype(np.float32).flatten()
                    norm = np.linalg.norm(vec)
                    if norm > 0:
                        vec = vec / norm
                        cap.release()
                        return (vec, None)
                    else:
                        error_msg = "Embedding norm is zero"
                        error_messages.append(error_msg)
                except Exception as e:
                    error_msg = f"Embedding creation failed: {str(e)}"
                    error_messages.append(error_msg)
                
                cap.release()
            else:
                error_msg = "Failed to read frame from downloaded video"
                error_messages.append(error_msg)
                cap.release()
                
        except Exception as e:
            error_msg = f"Frame extraction error: {str(e)}"
            error_messages.append(error_msg)
    
    # All methods failed
    final_error = "; ".join(error_messages) if error_messages else "Unknown error"
    return (None, final_error)


def create_collection_if_not_exists(collection_name: str) -> Collection:
    """Create collection if it doesn't exist"""
    if utility.has_collection(collection_name):
        print(f"📦 Collection '{collection_name}' already exists. Loading...")
        collection = Collection(collection_name)
        collection.load()
        return collection
    
    print(f"🆕 Creating new collection '{collection_name}'...")
    
    fields = [
        FieldSchema(
            name="id",
            dtype=DataType.INT64,
            is_primary=True,
            auto_id=True,
            description="Auto-generated primary key"
        ),
        FieldSchema(
            name="url",
            dtype=DataType.VARCHAR,
            max_length=MAX_URL_LENGTH,
            description="Source video URL"
        ),
        FieldSchema(
            name="job_id",
            dtype=DataType.VARCHAR,
            max_length=MAX_JOB_ID_LENGTH,
            description="Job ID (url_XXXX)"
        ),
        FieldSchema(
            name="embedding",
            dtype=DataType.FLOAT_VECTOR,
            dim=EMBEDDING_DIM,
            description="CLIP embedding from first frame"
        ),
    ]
    
    schema = CollectionSchema(
        fields,
        description="Video embeddings (1 vector per video, direct upload)"
    )
    
    collection = Collection(
        name=collection_name,
        schema=schema,
        consistency_level=CONSISTENCY_LEVEL
    )
    
    # Create index
    print(f"🔨 Building index ({INDEX_PARAMS['index_type']})...")
    collection.create_index(
        field_name="embedding",
        index_params=INDEX_PARAMS
    )
    
    collection.load()
    print(f"✅ Collection '{collection_name}' created!")
    return collection


def direct_upload(
    csv_path: str,
    column: str,
    collection_name: str,
    start: int = 0,
    end: Optional[int] = None,
    overwrite: bool = False
):
    """
    Direct upload: CSV → Extract → Upload → No storage
    """
    # Read URLs
    print(f"📖 Reading URLs from {csv_path}...")
    all_urls = read_urls_from_csv(csv_path, column)
    
    if not all_urls:
        print(f"❌ No URLs found in {csv_path}")
        sys.exit(1)
    
    # Determine range
    if end is None or end > len(all_urls):
        end = len(all_urls)
    start = max(0, start)
    
    if start >= end:
        print("❌ Invalid range")
        sys.exit(1)
    
    urls = all_urls[start:end]
    print(f"📊 Processing {len(urls)} URLs (index {start} to {end-1})")
    
    # Connect to Milvus
    print("\n🔌 Connecting to Zilliz Cloud...")
    params = get_connection_params()
    connections.connect("default", **params)
    print("✅ Connected!")
    
    # Create or get collection
    collection = create_collection_if_not_exists(collection_name)
    
    # Check existing data
    if not overwrite and collection.num_entities > 0:
        print(f"\n⚠️  Collection already has {collection.num_entities} vectors!")
        response = input("Continue adding more? (yes/no): ").strip().lower()
        if response != "yes":
            print("❌ Upload cancelled.")
            return
    
    # Process and upload in batches
    print(f"\n🚀 Starting direct upload (batch size: {BATCH_SIZE})...\n")
    
    batch_urls = []
    batch_job_ids = []
    batch_embeddings = []
    
    success_count = 0
    fail_count = 0
    
    t0 = time.time()
    
    for idx, url in enumerate(urls):
        global_idx = start + idx
        job_id = f"url_{global_idx:04d}"
        
        # Extract embedding
        embedding, error_msg = extract_first_frame_embedding(url, verbose=True)
        
        if embedding is None:
            fail_count += 1
            # Truncate URL for display (max 80 chars)
            url_display = url[:80] + "..." if len(url) > 80 else url
            if error_msg:
                print(f"❌ [{global_idx}] {job_id} FAIL: {error_msg}")
                print(f"   URL: {url_display}")
            else:
                print(f"❌ [{global_idx}] {job_id} FAIL: Unknown error")
                print(f"   URL: {url_display}")
            continue
        
        # Add to batch
        batch_urls.append(url)
        batch_job_ids.append(job_id)
        batch_embeddings.append(embedding.tolist())
        success_count += 1
        
        print(f"✅ [{global_idx}] {job_id} OK")
        
        # Insert batch when full
        if len(batch_embeddings) >= BATCH_SIZE:
            try:
                collection.insert([batch_urls, batch_job_ids, batch_embeddings])
                collection.flush()
                print(f"📤 Uploaded batch: {success_count} vectors uploaded so far\n")
                batch_urls, batch_job_ids, batch_embeddings = [], [], []
            except Exception as e:
                print(f"❌ Error uploading batch: {e}")
                fail_count += len(batch_embeddings)
                batch_urls, batch_job_ids, batch_embeddings = [], [], []
        
        # Progress update
        if (idx + 1) % 50 == 0:
            elapsed = time.time() - t0
            rate = (idx + 1) / elapsed
            remaining = (len(urls) - idx - 1) / rate if rate > 0 else 0
            print(f"📊 Progress: {idx+1}/{len(urls)} | Rate: {rate:.2f} videos/s | ETA: {remaining/60:.1f} min\n")
    
    # Upload remaining
    if batch_embeddings:
        try:
            collection.insert([batch_urls, batch_job_ids, batch_embeddings])
            collection.flush()
            print(f"📤 Uploaded final batch\n")
        except Exception as e:
            print(f"❌ Error uploading final batch: {e}")
    
    # Summary
    total_time = time.time() - t0
    final_count = collection.num_entities
    
    print("\n" + "="*60)
    print("✅ UPLOAD COMPLETE!")
    print("="*60)
    print(f"Total vectors in collection: {final_count}")
    print(f"Successful uploads: {success_count}")
    print(f"Failed: {fail_count}")
    print(f"Total time: {total_time/60:.1f} minutes")
    print(f"Average rate: {success_count/total_time:.2f} videos/second")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(
        description="Direct upload from CSV to Zilliz (no local storage)"
    )
    parser.add_argument(
        "--input",
        default="tvcQc.unique.csv",
        help="CSV file containing URLs"
    )
    parser.add_argument(
        "--column",
        default="decoded_url",
        help="Column name with URLs (default: decoded_url)"
    )
    parser.add_argument(
        "--collection",
        default="video_dedup_direct",
        help="Collection name (default: video_dedup_direct)"
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="Start index (inclusive)"
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="End index (exclusive, default: all)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Allow adding to existing collection"
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only print configuration"
    )
    args = parser.parse_args()
    
    # Print config
    print_config()
    print("\n💡 DIRECT UPLOAD MODE:")
    print("   ✅ No local storage (saves disk space)")
    print("   ✅ Faster overall process")
    print("   ✅ Extract → Upload → Done")
    print()
    
    if args.config_only:
        return
    
    # Validate CSV
    if not os.path.isfile(args.input):
        print(f"❌ ERROR: File not found: {args.input}", file=sys.stderr)
        sys.exit(1)
    
    # Run direct upload
    try:
        direct_upload(
            args.input,
            args.column,
            args.collection,
            args.start,
            args.end,
            args.overwrite
        )
    except KeyboardInterrupt:
        print("\n\n⚠️  Upload interrupted by user. Progress has been saved to Zilliz.")
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

