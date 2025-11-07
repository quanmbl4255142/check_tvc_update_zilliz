"""
Merge tất cả chunk results và check duplicates lần nữa (cross-chunk)
"""

import argparse
import csv
import glob
import os
import sys
from typing import List, Set, Dict

from pymilvus import connections, Collection, utility
from milvus_config import get_connection_params, DEFAULT_SIMILARITY_THRESHOLD, DEFAULT_TOP_K, SEARCH_PARAMS, print_config


def merge_chunk_files(pattern: str, output_file: str) -> List[str]:
    """Merge tất cả chunk CSV files thành 1 file"""
    print(f"📦 Merging chunk files matching: {pattern}")
    
    chunk_files = sorted(glob.glob(pattern))
    if not chunk_files:
        print(f"❌ No files found matching: {pattern}")
        sys.exit(1)
    
    print(f"   Found {len(chunk_files)} chunk files")
    
    all_urls = []
    seen_urls: Set[str] = set()
    
    for chunk_file in chunk_files:
        print(f"   Reading: {os.path.basename(chunk_file)}")
        with open(chunk_file, 'r', encoding='utf-8', newline='') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('decoded_url', '').strip()
                if url and url not in seen_urls:
                    all_urls.append(url)
                    seen_urls.add(url)
    
    # Write merged file
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['decoded_url'])
        for url in all_urls:
            writer.writerow([url])
    
    print(f"✅ Merged {len(all_urls)} unique URLs → {output_file}")
    return all_urls


def get_embeddings_from_zilliz(collection_name: str, urls: List[str]) -> Dict[str, List[float]]:
    """Query embeddings từ Zilliz cho các URLs"""
    print(f"\n🔍 Fetching embeddings from Zilliz for {len(urls)} URLs...")
    
    # Connect
    params = get_connection_params()
    connections.connect("default", **params)
    
    if not utility.has_collection(collection_name):
        print(f"❌ Collection '{collection_name}' not found!", file=sys.stderr)
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    # Query by URLs
    # Note: Cần query tất cả và filter theo URL
    total_entities = collection.num_entities
    print(f"   Collection has {total_entities} vectors")
    print(f"   Querying all videos to find matching URLs...")
    
    url_to_embedding = {}
    batch_size = 1500
    offset = 0
    
    while offset < total_entities:
        limit = min(batch_size, total_entities - offset)
        
        try:
            batch_data = collection.query(
                expr="id >= 0",
                output_fields=["job_id", "url", "embedding"],
                limit=limit,
                offset=offset
            )
            
            for item in batch_data:
                item_url = item["url"].strip()
                if item_url in urls:
                    url_to_embedding[item_url] = item["embedding"]
            
            offset += len(batch_data)
            if len(batch_data) < limit:
                break
                
        except Exception as e:
            print(f"   ⚠️  Error querying batch: {e}")
            break
    
    print(f"   ✅ Found embeddings for {len(url_to_embedding)}/{len(urls)} URLs")
    
    connections.disconnect("default")
    return url_to_embedding


def check_duplicates_final(
    collection_name: str,
    urls: List[str],
    url_to_embedding: Dict[str, List[float]],
    similarity_threshold: float,
    top_k: int,
    unique_csv: str,
    report_csv: str
) -> tuple[int, int]:
    """Check duplicates trong merged URLs"""
    print(f"\n🔍 Checking duplicates in merged {len(urls)} URLs...")
    
    # Connect
    params = get_connection_params()
    connections.connect("default", **params)
    
    collection = Collection(collection_name)
    collection.load()
    
    # Find duplicates
    seen_urls: Set[str] = set()
    unique_urls: List[str] = []
    duplicates: List[Dict] = []
    
    processed = 0
    for url in urls:
        if url in seen_urls:
            continue
        
        if url not in url_to_embedding:
            # URL không có embedding → skip hoặc mark as unique
            unique_urls.append(url)
            continue
        
        embedding = url_to_embedding[url]
        
        try:
            # Search similar
            search_results = collection.search(
                data=[embedding],
                anns_field="embedding",
                param=SEARCH_PARAMS,
                limit=top_k,
                output_fields=["job_id", "url"]
            )
            
            # Find best match
            max_similarity = 0.0
            best_match_url = None
            
            for hit in search_results[0]:
                hit_url = hit.entity.get("url", "").strip()
                
                if hit_url == url:
                    continue
                
                if hit_url not in urls:
                    continue  # Only check within merged URLs
                
                if hit.score > max_similarity:
                    max_similarity = hit.score
                    best_match_url = hit_url
            
            # Check if duplicate
            if max_similarity >= similarity_threshold and best_match_url:
                # Mark as duplicate
                seen_urls.add(url)
                duplicates.append({
                    "duplicate_url": url,
                    "original_url": best_match_url,
                    "similarity": f"{max_similarity:.6f}"
                })
            else:
                # Unique
                unique_urls.append(url)
                seen_urls.add(url)
        
        except Exception as e:
            print(f"⚠️  Error searching {url[:50]}...: {e}")
            unique_urls.append(url)  # Add as unique if error
            continue
        
        processed += 1
        if processed % 100 == 0:
            print(f"   📊 Processed {processed}/{len(urls)} URLs...")
    
    # Write results
    print(f"\n💾 Writing results...")
    
    with open(unique_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['decoded_url'])
        for url in unique_urls:
            writer.writerow([url])
    
    with open(report_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['duplicate_url', 'original_url', 'similarity'])
        for dup in duplicates:
            writer.writerow([dup['duplicate_url'], dup['original_url'], dup['similarity']])
    
    connections.disconnect("default")
    
    print(f"✅ Final results:")
    print(f"   Unique videos: {len(unique_urls)}")
    print(f"   Duplicates: {len(duplicates)}")
    
    return len(unique_urls), len(duplicates)


def main():
    parser = argparse.ArgumentParser(
        description="Merge chunk results and check duplicates again"
    )
    parser.add_argument(
        "--chunk_pattern",
        default="FINAL_RESULT_chunk_*.csv",
        help="Pattern to match chunk files (default: FINAL_RESULT_chunk_*.csv)"
    )
    parser.add_argument(
        "--merged_csv",
        default="merged_unique.csv",
        help="Output merged CSV (default: merged_unique.csv)"
    )
    parser.add_argument(
        "--collection",
        default="video_dedup_direct",
        help="Zilliz collection name"
    )
    parser.add_argument(
        "--cosine_thresh",
        type=float,
        default=DEFAULT_SIMILARITY_THRESHOLD,
        help=f"Similarity threshold (default: {DEFAULT_SIMILARITY_THRESHOLD})"
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=DEFAULT_TOP_K,
        help=f"Top K results per query (default: {DEFAULT_TOP_K})"
    )
    parser.add_argument(
        "--final_unique_csv",
        default="FINAL_RESULT_FINAL.csv",
        help="Final unique URLs after dedupe (default: FINAL_RESULT_FINAL.csv)"
    )
    parser.add_argument(
        "--final_report_csv",
        default="duplicates_final.csv",
        help="Final duplicates report (default: duplicates_final.csv)"
    )
    parser.add_argument(
        "--skip_dedupe",
        action="store_true",
        help="Skip deduplication, only merge chunks"
    )
    
    args = parser.parse_args()
    
    print("="*70)
    print("🔗 MERGE & DEDUPE CHUNKS")
    print("="*70)
    
    # Step 1: Merge chunks
    urls = merge_chunk_files(args.chunk_pattern, args.merged_csv)
    
    if args.skip_dedupe:
        print("\n✅ Merge complete! (Skipped deduplication)")
        return
    
    # Step 2: Get embeddings
    url_to_embedding = get_embeddings_from_zilliz(args.collection, urls)
    
    if len(url_to_embedding) == 0:
        print("❌ No embeddings found! Cannot check duplicates.")
        sys.exit(1)
    
    # Step 3: Check duplicates
    print_config()
    unique_count, dup_count = check_duplicates_final(
        args.collection,
        urls,
        url_to_embedding,
        args.cosine_thresh,
        args.top_k,
        args.final_unique_csv,
        args.final_report_csv
    )
    
    print("\n" + "="*70)
    print("🎉 COMPLETE!")
    print("="*70)
    print(f"   ✅ Final unique videos: {unique_count}")
    print(f"   ❌ Final duplicates: {dup_count}")
    print(f"   → Final unique: {args.final_unique_csv}")
    print(f"   → Final duplicates: {args.final_report_csv}")


if __name__ == "__main__":
    main()

