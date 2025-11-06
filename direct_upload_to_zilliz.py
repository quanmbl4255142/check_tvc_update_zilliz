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


def download_video(url: str, dest_path: str) -> None:
    """Download video from URL"""
    try:
        import requests
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


def extract_first_frame_embedding(url: str) -> Optional[np.ndarray]:
    """
    Extract first frame from video URL and return embedding
    Returns None if failed
    """
    with tempfile.TemporaryDirectory() as tdir:
        # METHOD 1: Try opening directly from URL
        try:
            cap = cv2.VideoCapture(url)
            if cap.isOpened():
                cap.set(cv2.CAP_PROP_POS_MSEC, 0)
                success, frame = cap.read()
                if success and frame is not None:
                    temp_img_path = os.path.join(tdir, "frame.png")
                    temp_npy_path = os.path.join(tdir, "embedding.npy")
                    
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
                    return vec
                cap.release()
        except Exception:
            pass
        
        # METHOD 2: Download video if direct access fails
        tmp_video_path = os.path.join(tdir, safe_slug(os.path.basename(url)) or "video.mp4")
        try:
            download_video(url, tmp_video_path)
        except Exception:
            return None
        
        # Extract frame
        try:
            cap = cv2.VideoCapture(tmp_video_path)
            if not cap.isOpened():
                return None
            
            cap.set(cv2.CAP_PROP_POS_MSEC, 0)
            success, frame = cap.read()
            
            if success and frame is not None:
                temp_img_path = os.path.join(tdir, "frame.png")
                temp_npy_path = os.path.join(tdir, "embedding.npy")
                
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
                return vec
            
            cap.release()
        except Exception:
            pass
    
    return None


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
        embedding = extract_first_frame_embedding(url)
        
        if embedding is None:
            fail_count += 1
            print(f"❌ [{global_idx}] {job_id} FAIL")
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

