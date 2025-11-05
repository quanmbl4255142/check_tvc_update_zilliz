"""
Search duplicates with AGGREGATED vectors - Đơn giản & Nhanh hơn 3×
Mỗi video chỉ có 1 vector → query đơn giản, không cần multi-frame comparison
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
    get_connection_params,
    DEFAULT_SIMILARITY_THRESHOLD,
    DEFAULT_TOP_K,
    SEARCH_PARAMS,
    print_config,
)


def search_duplicates_aggregated(
    collection_name: str,
    similarity_threshold: float,
    unique_csv: str,
    report_csv: str,
    top_k: int = None,
) -> tuple[int, int]:
    """
    Search duplicates với aggregated vectors - ĐƠN GIẢN HƠN NHIỀU!
    Mỗi video chỉ có 1 vector → không cần group frames
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
        print(f"Please run upload_aggregated_to_milvus.py first.", file=sys.stderr)
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    total_entities = collection.num_entities
    print(f"📊 Collection has {total_entities} vectors")
    print(f"🎯 Each video = 1 vector (aggregated from 3 frames)")
    
    # Get all videos
    print(f"\n🔍 Fetching all videos from Zilliz...")
    all_data = collection.query(
        expr="id >= 0",
        output_fields=["job_id", "url", "embedding"],
        limit=total_entities
    )
    
    print(f"✅ Loaded {len(all_data)} videos")
    print(f"🎯 Searching for duplicates (threshold: {similarity_threshold})...\n")
    
    # Track duplicates
    seen_jobs: Set[str] = set()
    unique_videos: List[Dict] = []
    duplicates: List[Dict] = []
    
    processed = 0
    for idx, video in enumerate(all_data):
        job_id = video["job_id"]
        
        if job_id in seen_jobs:
            continue  # Already marked as duplicate
        
        processed += 1
        if processed % 50 == 0:
            print(f"📊 Progress: {processed}/{len(all_data)} videos processed...")
        
        # Search similar videos
        embedding = video["embedding"]
        
        try:
            search_results = collection.search(
                data=[embedding],
                anns_field="embedding",
                param=SEARCH_PARAMS,
                limit=top_k,
                output_fields=["job_id", "url"]
            )
            
            # Find best match (excluding self)
            max_similarity = 0.0
            best_match = None
            
            for hit in search_results[0]:
                hit_job_id = hit.entity.get("job_id")
                
                # Skip self
                if hit_job_id == job_id:
                    continue
                
                # Skip already processed
                if hit_job_id in seen_jobs:
                    continue
                
                # Check similarity
                if hit.score > max_similarity:
                    max_similarity = hit.score
                    best_match = {
                        "job_id": hit_job_id,
                        "url": hit.entity.get("url"),
                        "score": hit.score
                    }
            
            # Check if duplicate
            if max_similarity >= similarity_threshold and best_match:
                # Mark as duplicate
                seen_jobs.add(job_id)
                duplicates.append({
                    "duplicate_url": video["url"],
                    "duplicate_job_id": job_id,
                    "original_job_id": best_match["job_id"],
                    "original_url": best_match["url"],
                    "similarity": f"{max_similarity:.6f}"
                })
            else:
                # Unique video
                unique_videos.append({
                    "url": video["url"],
                    "job_id": job_id
                })
        
        except Exception as e:
            print(f"⚠️  Error searching {job_id}: {e}")
            continue
    
    # Write results
    print(f"\n💾 Writing results...")
    
    # Unique URLs
    os.makedirs(os.path.dirname(unique_csv) or ".", exist_ok=True)
    with open(unique_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["decoded_url"])
        for video in unique_videos:
            writer.writerow([video["url"]])
    
    # Duplicate report
    os.makedirs(os.path.dirname(report_csv) or ".", exist_ok=True)
    with open(report_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "duplicate_url",
            "duplicate_job_id",
            "original_job_id",
            "original_url",
            "similarity"
        ])
        for dup in duplicates:
            writer.writerow([
                dup["duplicate_url"],
                dup["duplicate_job_id"],
                dup["original_job_id"],
                dup["original_url"],
                dup["similarity"]
            ])
    
    print(f"✅ Results written!")
    print(f"   Unique videos: {len(unique_videos)}")
    print(f"   Duplicates: {len(duplicates)}")
    
    return len(unique_videos), len(duplicates)


def main():
    parser = argparse.ArgumentParser(
        description="Search duplicates with aggregated vectors (fast & simple)"
    )
    parser.add_argument(
        "--collection",
        default="video_dedup_v2",
        help="Collection name (default: video_dedup_v2)"
    )
    parser.add_argument(
        "--cosine_thresh",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"Similarity threshold (default: {DEFAULT_SIMILARITY_THRESHOLD})"
    )
    parser.add_argument(
        "--unique_csv",
        default="FINAL_RESULT_AGG.csv",
        help="Output CSV of unique URLs (default: FINAL_RESULT_AGG.csv)"
    )
    parser.add_argument(
        "--report_csv",
        default="duplicate_videos_agg.csv",
        help="Report CSV of duplicates (default: duplicate_videos_agg.csv)"
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
        help="Only print configuration"
    )
    args = parser.parse_args()
    
    # Validate
    if not 0.0 <= args.cosine_thresh <= 1.0:
        print(f"❌ ERROR: Threshold must be 0.0-1.0 (got {args.cosine_thresh})", file=sys.stderr)
        sys.exit(1)
    
    # Print config
    print_config()
    print(f"\n🎯 AGGREGATED MODE:")
    print(f"   → 1 vector per video (instead of 3)")
    print(f"   → 3× faster search")
    print(f"   → Simpler query logic")
    
    if args.config_only:
        return
    
    try:
        unique_count, dup_count = search_duplicates_aggregated(
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

