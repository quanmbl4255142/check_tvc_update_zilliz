"""
Upload video embeddings to Milvus - 1 thumbnail per video
Load thumbnail_embedding.npy (from distributed frames grid) and upload to Zilliz (1 vector per video)
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
    EMBEDDING_DIM,
    MAX_URL_LENGTH,
    MAX_JOB_ID_LENGTH,
    INDEX_PARAMS,
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


def load_embedding(job_dir: str) -> np.ndarray:
    """
    Load thumbnail embedding from job directory
    Supports both new format (thumbnail_embedding.npy) and old format (first_frame.npy) for backward compatibility
    
    Returns:
        Normalized embedding vector (512 dims) or None if not found
    """
    # Try new format first (thumbnail from distributed frames)
    npy_path = os.path.join(job_dir, "thumbnail_embedding.npy")
    
    # Fallback to old format for backward compatibility
    if not os.path.exists(npy_path):
        npy_path = os.path.join(job_dir, "first_frame.npy")
    
    if not os.path.exists(npy_path):
        return None
    
    try:
        vec = np.load(npy_path).astype(np.float32).flatten()
        # L2 normalize for cosine similarity (using IP metric)
        norm = np.linalg.norm(vec)
        if norm > 0:
            vec = vec / norm
        return vec
    except Exception:
        return None


def create_collection_if_not_exists(collection_name: str) -> Collection:
    """Create simplified collection with single vector per video"""
    if utility.has_collection(collection_name):
        print(f"📦 Collection '{collection_name}' already exists. Loading...")
        collection = Collection(collection_name)
        collection.load()
        return collection
    
    print(f"🆕 Creating new collection '{collection_name}'...")
    
    # Simplified schema - NO frame_type field
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
            name="embedding",
            dtype=DataType.FLOAT_VECTOR,
            dim=EMBEDDING_DIM,
            description="CLIP embedding from thumbnail grid (distributed frames)"
        ),
    ]
    
    schema = CollectionSchema(
        fields,
        description="Video embeddings (1 vector per video, thumbnail grid from distributed frames)"
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


def upload_vectors(
    batch_outputs_dir: str,
    collection_name: str,
    overwrite: bool = False
) -> Tuple[int, int]:
    """Upload video embeddings to Milvus (1 frame per video)"""
    
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
    embeddings = []
    
    total_videos = 0
    skipped = 0
    
    for idx, job_dir in enumerate(job_dirs):
        job_id = os.path.basename(job_dir)
        url = load_url(job_dir)
        
        if not url:
            skipped += 1
            continue
        
        # Load embedding from thumbnail (or first frame for backward compatibility)
        embedding_vec = load_embedding(job_dir)
        
        if embedding_vec is None:
            skipped += 1
            print(f"⚠️  [{idx+1}/{len(job_dirs)}] {job_id} - No embeddings found")
            continue
        
        urls.append(url)
        job_ids.append(job_id)
        embeddings.append(embedding_vec.tolist())
        total_videos += 1
        
        # Batch insert
        if len(embeddings) >= BATCH_SIZE:
            try:
                collection.insert([urls, job_ids, embeddings])
                print(f"✅ Inserted batch: {total_videos} videos")
                urls, job_ids, embeddings = [], [], []
            except Exception as e:
                print(f"❌ Error inserting batch: {e}")
                return total_videos, skipped
        
        # Progress
        if (idx + 1) % 50 == 0:
            print(f"📊 Progress: {idx+1}/{len(job_dirs)} jobs processed...")
    
    # Insert remaining
    if embeddings:
        try:
            collection.insert([urls, job_ids, embeddings])
            print(f"✅ Inserted final batch: {total_videos} videos")
        except Exception as e:
            print(f"❌ Error inserting final batch: {e}")
            return total_videos, skipped
    
    # Flush
    print("\n💾 Flushing data...")
    collection.flush()
    
    final_count = collection.num_entities
    print(f"\n✅ Upload complete!")
    print(f"   Total vectors: {final_count}")
    print(f"   Skipped: {skipped}")
    print(f"   Mode: 1 thumbnail per video (thumbnail_embedding.npy or first_frame.npy)")
    
    return final_count, skipped


def main():
    parser = argparse.ArgumentParser(
        description="Upload video embeddings to Milvus (1 vector per video, thumbnail grid from distributed frames)"
    )
    parser.add_argument(
        "--root",
        default="batch_outputs",
        help="Root directory containing url_* folders (default: batch_outputs)"
    )
    parser.add_argument(
        "--collection",
        default="product_embeddings",
        help="Collection name (default: product_embeddings)"
    )
    parser.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite existing collection"
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only print configuration"
    )
    args = parser.parse_args()
    
    # Print config
    print_config()
    print(f"\n🎯 Upload mode: 1 thumbnail per video")
    print(f"   → Loads thumbnail_embedding.npy from each job directory (or first_frame.npy for backward compatibility)")
    print(f"   → 1 vector per video (thumbnail grid from 5-15 distributed frames)")
    
    if args.config_only:
        return
    
    # Validate
    if not os.path.isdir(args.root):
        print(f"❌ ERROR: Directory not found: {args.root}", file=sys.stderr)
        sys.exit(1)
    
    # Upload
    try:
        total, skipped = upload_vectors(
            args.root,
            args.collection,
            args.overwrite
        )
        
        if total > 0:
            print(f"\n🎉 Successfully uploaded {total} vectors!")
            print(f"   → 1 vector per video (from thumbnail_embedding.npy or first_frame.npy)")
        else:
            print("\n⚠️  No vectors were uploaded.")
            
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

