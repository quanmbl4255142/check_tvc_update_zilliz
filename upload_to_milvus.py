"""
Upload video frame embeddings to Milvus/Zilliz Cloud
Reads from batch_outputs/ directory and uploads all embeddings
"""

import argparse
import os
import sys
import glob
import numpy as np
from typing import List, Tuple

from pymilvus import (
    connections,
    Collection,
    CollectionSchema,
    FieldSchema,
    DataType,
    utility,
)

from milvus_config import (
    get_connection_params,
    COLLECTION_NAME,
    EMBEDDING_DIM,
    MAX_URL_LENGTH,
    MAX_JOB_ID_LENGTH,
    MAX_FRAME_TYPE_LENGTH,
    INDEX_PARAMS,
    SEARCH_PARAMS,
    BATCH_SIZE,
    CONSISTENCY_LEVEL,
    print_config,
)


def load_url(job_dir: str) -> str:
    """Load URL from url.txt file"""
    url_path = os.path.join(job_dir, "url.txt")
    try:
        with open(url_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def load_embedding(npy_path: str) -> np.ndarray:
    """Load and normalize embedding vector"""
    vec = np.load(npy_path).astype(np.float32).flatten()
    # L2 normalize for cosine similarity (using IP metric)
    norm = np.linalg.norm(vec)
    if norm > 0:
        vec = vec / norm
    return vec


def create_collection_if_not_exists(collection_name: str) -> Collection:
    """Create collection if it doesn't exist, or return existing one"""
    # Check if collection exists
    if utility.has_collection(collection_name):
        print(f"📦 Collection '{collection_name}' already exists. Loading...")
        collection = Collection(collection_name)
        collection.load()
        return collection
    
    print(f"🆕 Creating new collection '{collection_name}'...")
    
    # Define schema
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
            description="Job folder ID (e.g., url_0000)"
        ),
        FieldSchema(
            name="frame_type",
            dtype=DataType.VARCHAR,
            max_length=MAX_FRAME_TYPE_LENGTH,
            description="Frame type: first, middle, or last"
        ),
        FieldSchema(
            name="embedding",
            dtype=DataType.FLOAT_VECTOR,
            dim=EMBEDDING_DIM,
            description="CLIP embedding vector (L2-normalized)"
        ),
    ]
    
    schema = CollectionSchema(
        fields,
        description="Video frame embeddings for deduplication"
    )
    
    # Create collection
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
    
    # Load collection
    collection.load()
    
    print(f"✅ Collection '{collection_name}' created and ready!")
    return collection


def upload_vectors(batch_outputs_dir: str, collection_name: str, overwrite: bool = False) -> Tuple[int, int]:
    """
    Upload all embeddings from batch_outputs to Milvus
    
    Returns:
        (total_vectors, total_videos)
    """
    # Connect to Milvus
    print("🔌 Connecting to Milvus...")
    params = get_connection_params()
    connections.connect("default", **params)
    print("✅ Connected!")
    
    # Create or get collection
    collection = create_collection_if_not_exists(collection_name)
    
    # Check if collection has data
    if not overwrite and collection.num_entities > 0:
        print(f"\n⚠️  Collection already has {collection.num_entities} vectors!")
        response = input("Do you want to overwrite? (yes/no): ").strip().lower()
        if response != "yes":
            print("❌ Upload cancelled.")
            return 0, 0
        print("🗑️  Deleting existing collection...")
        utility.drop_collection(collection_name)
        collection = create_collection_if_not_exists(collection_name)
    
    # Find all job directories
    pattern = os.path.join(batch_outputs_dir, "url_*")
    job_dirs = sorted([d for d in glob.glob(pattern) if os.path.isdir(d)])
    
    if not job_dirs:
        print(f"❌ No job directories found in {batch_outputs_dir}")
        return 0, 0
    
    print(f"\n📁 Found {len(job_dirs)} job directories")
    print(f"📤 Starting upload (batch size: {BATCH_SIZE})...\n")
    
    # Prepare data for batch insert
    urls = []
    job_ids = []
    frame_types = []
    embeddings = []
    
    total_vectors = 0
    total_videos = 0
    
    for idx, job_dir in enumerate(job_dirs):
        job_id = os.path.basename(job_dir)
        url = load_url(job_dir)
        
        if not url:
            print(f"⚠️  [{idx+1}/{len(job_dirs)}] {job_id} - No URL found, skipping")
            continue
        
        # Load all available frames
        frame_files = [
            ("first_frame.npy", "first"),
            ("middle_frame.npy", "middle"),
            ("last_frame.npy", "last")
        ]
        
        frames_loaded = 0
        for frame_file, frame_type in frame_files:
            npy_path = os.path.join(job_dir, frame_file)
            if os.path.exists(npy_path):
                try:
                    vec = load_embedding(npy_path)
                    urls.append(url)
                    job_ids.append(job_id)
                    frame_types.append(frame_type)
                    embeddings.append(vec.tolist())
                    frames_loaded += 1
                except Exception as e:
                    print(f"⚠️  [{idx+1}/{len(job_dirs)}] {job_id} - Error loading {frame_file}: {e}")
        
        total_videos += 1
        
        # Batch insert when reaching batch size
        if len(embeddings) >= BATCH_SIZE:
            try:
                collection.insert([urls, job_ids, frame_types, embeddings])
                total_vectors += len(embeddings)
                print(f"✅ Inserted batch: {total_vectors} vectors ({total_videos} videos processed)")
                urls, job_ids, frame_types, embeddings = [], [], [], []
            except Exception as e:
                print(f"❌ Error inserting batch: {e}")
                return total_vectors, total_videos
        
        # Progress update
        if (idx + 1) % 50 == 0:
            print(f"📊 Progress: {idx+1}/{len(job_dirs)} jobs processed...")
    
    # Insert remaining vectors
    if embeddings:
        try:
            collection.insert([urls, job_ids, frame_types, embeddings])
            total_vectors += len(embeddings)
            print(f"✅ Inserted final batch: {total_vectors} vectors")
        except Exception as e:
            print(f"❌ Error inserting final batch: {e}")
            return total_vectors, total_videos
    
    # Flush to ensure all data is written
    print("\n💾 Flushing data to disk...")
    collection.flush()
    
    # Get final count
    final_count = collection.num_entities
    print(f"\n✅ Upload complete!")
    print(f"   Total vectors: {final_count}")
    print(f"   Total videos: {total_videos}")
    print(f"   Average frames per video: {final_count / total_videos:.2f}")
    
    return final_count, total_videos


def main():
    parser = argparse.ArgumentParser(
        description="Upload video frame embeddings to Milvus/Zilliz Cloud"
    )
    parser.add_argument(
        "--root",
        default="batch_outputs",
        help="Root directory containing url_* folders (default: batch_outputs)"
    )
    parser.add_argument(
        "--collection",
        default=COLLECTION_NAME,
        help=f"Collection name (default: {COLLECTION_NAME})"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing collection if it exists"
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only print configuration, don't upload"
    )
    args = parser.parse_args()
    
    # Print configuration
    print_config()
    
    if args.config_only:
        return
    
    # Validate root directory
    if not os.path.isdir(args.root):
        print(f"❌ ERROR: Directory not found: {args.root}", file=sys.stderr)
        print(f"Please run batch_extract_from_urls.py first to generate embeddings.", file=sys.stderr)
        sys.exit(1)
    
    # Upload vectors
    try:
        total_vectors, total_videos = upload_vectors(
            args.root,
            args.collection,
            args.overwrite
        )
        
        if total_vectors > 0:
            print(f"\n🎉 Successfully uploaded {total_vectors} vectors from {total_videos} videos!")
        else:
            print("\n⚠️  No vectors were uploaded.")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

