"""
Search for duplicate videos using Milvus vector similarity search
Uses multi-frame comparison: compares all frame pairs and takes max similarity
"""

import argparse
import os
import sys
import csv
from typing import List, Dict, Set

from pymilvus import (
    connections,
    Collection,
    utility,
)

from milvus_config import (
    USE_CLOUD,
    get_connection_params,
    COLLECTION_NAME,
    DEFAULT_SIMILARITY_THRESHOLD,
    DEFAULT_TOP_K,
    SEARCH_PARAMS,
    print_config,
)


def search_duplicates(
    collection_name: str,
    similarity_threshold: float,
    unique_csv: str,
    report_csv: str,
    top_k: int = None,
) -> tuple[int, int]:
    """
    Search for duplicate videos using multi-frame comparison
    
    Algorithm:
    1. For each video, get all its frames (first/middle/last)
    2. For each frame, search top-K similar frames
    3. For each similar frame found, check if it belongs to a different video
    4. If max similarity >= threshold, mark as duplicate
    
    Returns:
        (unique_count, duplicate_count)
    """
    if top_k is None:
        top_k = DEFAULT_TOP_K
    
    print("🔌 Connecting to Milvus...")
    params = get_connection_params()
    connections.connect("default", **params)
    print("✅ Connected!")
    
    # Load collection
    if not utility.has_collection(collection_name):
        print(f"❌ ERROR: Collection '{collection_name}' not found!", file=sys.stderr)
        print(f"Please run upload_to_milvus.py first to create and populate the collection.", file=sys.stderr)
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    total_entities = collection.num_entities
    print(f"📊 Collection has {total_entities} vectors")
    
    # Get all unique job_ids (videos)
    print("🔍 Fetching all videos...")
    all_data = collection.query(
        expr="id >= 0",
        output_fields=["job_id", "url", "frame_type"],
        limit=total_entities
    )
    
    # Group by job_id
    videos: Dict[str, Dict] = {}
    for item in all_data:
        job_id = item["job_id"]
        if job_id not in videos:
            videos[job_id] = {
                "url": item["url"],
                "frames": [],
                "frame_vectors": [],
                "frame_ids": []
            }
        videos[job_id]["frames"].append(item["frame_type"])
        videos[job_id]["frame_vectors"].append(None)  # Will be loaded later
        videos[job_id]["frame_ids"].append(item["id"])
    
    print(f"📹 Found {len(videos)} unique videos")
    print(f"🎯 Searching for duplicates (threshold: {similarity_threshold})...\n")
    
    # Get all vectors with their IDs
    print("📥 Loading all vectors...")
    all_vectors_data = collection.query(
        expr="id >= 0",
        output_fields=["id", "job_id", "url", "frame_type", "embedding"],
        limit=total_entities
    )
    
    # Create mapping: id -> vector
    id_to_vector = {}
    id_to_job = {}
    for item in all_vectors_data:
        vec_id = item["id"]
        id_to_vector[vec_id] = item["embedding"]
        id_to_job[vec_id] = item["job_id"]
    
    print(f"✅ Loaded {len(id_to_vector)} vectors")
    print(f"🔎 Starting similarity search...\n")
    
    # Track duplicates
    seen_videos: Set[str] = set()
    unique_videos: List[str] = []
    duplicates: List[Dict] = []
    
    processed = 0
    for job_id, video_data in videos.items():
        if job_id in seen_videos:
            continue  # Already marked as duplicate
        
        processed += 1
        if processed % 50 == 0:
            print(f"📊 Progress: {processed}/{len(videos)} videos processed...")
        
        # Get all frame vectors for this video
        frame_vectors = []
        for frame_id in video_data["frame_ids"]:
            if frame_id in id_to_vector:
                frame_vectors.append(id_to_vector[frame_id])
        
        if not frame_vectors:
            continue
        
        # Search for similar frames
        max_similarity = 0.0
        best_match_job_id = None
        
        # For each frame of current video, search for similar frames
        for frame_vec in frame_vectors:
            # Search top-K similar vectors
            try:
                search_results = collection.search(
                    data=[frame_vec],
                    anns_field="embedding",
                    param=SEARCH_PARAMS,
                    limit=top_k,
                    output_fields=["job_id", "url"]
                )
                
                # Check results
                for hit in search_results[0]:
                    hit_job_id = hit.entity.get("job_id")
                    
                    # Skip if same video
                    if hit_job_id == job_id:
                        continue
                    
                    # Skip if already processed
                    if hit_job_id in seen_videos:
                        continue
                    
                    # Update max similarity
                    if hit.score > max_similarity:
                        max_similarity = hit.score
                        best_match_job_id = hit_job_id
            except Exception as e:
                print(f"⚠️  Error searching for {job_id}: {e}")
                continue
        
        # Check if duplicate
        if max_similarity >= similarity_threshold and best_match_job_id:
            # Mark as duplicate
            seen_videos.add(job_id)
            duplicates.append({
                "duplicate_url": video_data["url"],
                "duplicate_job_id": job_id,
                "original_job_id": best_match_job_id,
                "original_url": videos[best_match_job_id]["url"],
                "max_similarity": f"{max_similarity:.6f}"
            })
        else:
            # Unique video
            unique_videos.append(video_data["url"])
    
    # Write results
    print(f"\n💾 Writing results...")
    
    # Unique URLs
    os.makedirs(os.path.dirname(unique_csv) or ".", exist_ok=True)
    with open(unique_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["decoded_url"])
        for url in unique_videos:
            writer.writerow([url])
    
    # Duplicate report
    os.makedirs(os.path.dirname(report_csv) or ".", exist_ok=True)
    with open(report_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "duplicate_url",
            "duplicate_job_id",
            "original_job_id",
            "original_url",
            "max_similarity"
        ])
        for dup in duplicates:
            writer.writerow([
                dup["duplicate_url"],
                dup["duplicate_job_id"],
                dup["original_job_id"],
                dup["original_url"],
                dup["max_similarity"]
            ])
    
    print(f"✅ Results written!")
    print(f"   Unique videos: {len(unique_videos)}")
    print(f"   Duplicates: {len(duplicates)}")
    
    return len(unique_videos), len(duplicates)


def main():
    parser = argparse.ArgumentParser(
        description="Search for duplicate videos using Milvus vector similarity"
    )
    parser.add_argument(
        "--collection",
        default=COLLECTION_NAME,
        help=f"Collection name (default: {COLLECTION_NAME})"
    )
    parser.add_argument(
        "--cosine_thresh",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"Cosine similarity threshold (default: {DEFAULT_SIMILARITY_THRESHOLD})"
    )
    parser.add_argument(
        "--unique_csv",
        default="FINAL_RESULT_MILVUS.csv",
        help="Output CSV of unique URLs (default: FINAL_RESULT_MILVUS.csv)"
    )
    parser.add_argument(
        "--report_csv",
        default="duplicate_videos_milvus.csv",
        help="Report CSV of duplicates (default: duplicate_videos_milvus.csv)"
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=DEFAULT_TOP_K,
        help=f"Top K results per query (default: {DEFAULT_TOP_K})"
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only print configuration, don't search"
    )
    args = parser.parse_args()
    
    # Validate threshold
    if not 0.0 <= args.cosine_thresh <= 1.0:
        print(f"❌ ERROR: Cosine threshold must be between 0.0 and 1.0 (got {args.cosine_thresh})", file=sys.stderr)
        sys.exit(1)
    
    # Print configuration
    print_config()
    
    if args.config_only:
        return
    
    try:
        unique_count, dup_count = search_duplicates(
            args.collection,
            args.cosine_thresh,
            args.unique_csv,
            args.report_csv,
            args.top_k
        )
        
        print(f"\n🎉 Search complete!")
        print(f"   ✅ Unique videos: {unique_count}")
        print(f"   ❌ Duplicates found: {dup_count}")
        print(f"   → Unique URLs: {args.unique_csv}")
        print(f"   → Duplicates report: {args.report_csv}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

