"""
Script để chạy search_duplicates_aggregated.py trên toàn bộ collection
Mỗi lần xử lý 5000 videos và append vào file CSV đang tồn tại
"""

import argparse
import subprocess
import sys
import time
from pymilvus import connections, Collection, utility
from milvus_config import get_connection_params


def get_total_videos(collection_name: str) -> int:
    """Lấy tổng số videos trong collection"""
    print("🔌 Connecting to Milvus...")
    try:
        params = get_connection_params()
        connections.connect("default", **params)
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)
    
    if not utility.has_collection(collection_name):
        print(f"❌ Collection '{collection_name}' not found!")
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    total = collection.num_entities
    connections.disconnect("default")
    return total


def run_chunk(collection: str, cosine_thresh: float, chunk_start: int, chunk_end: int,
              unique_csv: str, report_csv: str, auto_clean: bool, invalid_csv: str,
              batch_size: int, num_threads: int, fast_mode: bool, top_k: int) -> bool:
    """Chạy search cho một chunk và append vào file CSV"""
    
    print(f"\n{'='*80}")
    print(f"📦 PROCESSING CHUNK: {chunk_start} to {chunk_end-1} ({chunk_end - chunk_start} videos)")
    print(f"{'='*80}\n")
    
    cmd = [
        sys.executable,
        "search_duplicates_aggregated.py",
        "--collection", collection,
        "--cosine_thresh", str(cosine_thresh),
        "--unique_csv", unique_csv,
        "--report_csv", report_csv,
        "--chunk_start", str(chunk_start),
        "--chunk_end", str(chunk_end),
        "--batch_size", str(batch_size),
        "--num_threads", str(num_threads),
        "--top_k", str(top_k),
        "--append_mode",  # Enable append mode
    ]
    
    if auto_clean:
        cmd.append("--auto_clean")
        if invalid_csv:
            cmd.extend(["--invalid_csv", invalid_csv])
    
    if fast_mode:
        cmd.append("--fast_mode")
    
    try:
        result = subprocess.run(cmd, check=True, capture_output=False)
        print(f"\n✅ Chunk {chunk_start}-{chunk_end} completed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"\n❌ Chunk {chunk_start}-{chunk_end} failed with exit code {e.returncode}")
        return False
    except KeyboardInterrupt:
        print(f"\n⚠️  Chunk {chunk_start}-{chunk_end} interrupted by user")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Chạy search_duplicates_aggregated.py trên toàn bộ collection, mỗi lần 5000 videos và append vào CSV"
    )
    parser.add_argument(
        "--collection",
        default="video_dedup_tri_quan",
        help="Collection name (default: video_dedup_tri_quan)"
    )
    parser.add_argument(
        "--cosine_thresh",
        type=float,
        default=0.85,
        help="Similarity threshold (default: 0.85)"
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5000,
        help="Size of each chunk (default: 5000)"
    )
    parser.add_argument(
        "--unique_csv",
        default="FINAL_RESULT.csv",
        help="Output CSV for unique URLs (default: FINAL_RESULT.csv). Results will be appended."
    )
    parser.add_argument(
        "--report_csv",
        default="duplicates.csv",
        help="Output CSV for duplicates (default: duplicates.csv). Results will be appended."
    )
    parser.add_argument(
        "--invalid_csv",
        default="invalid_urls.csv",
        help="Output CSV for invalid URLs (default: invalid_urls.csv). Results will be appended."
    )
    parser.add_argument(
        "--auto_clean",
        action="store_true",
        help="Enable auto-clean for invalid URLs"
    )
    parser.add_argument(
        "--batch_size",
        type=int,
        default=10,
        help="Batch size for search (default: 10)"
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=4,
        help="Number of threads (default: 4)"
    )
    parser.add_argument(
        "--top_k",
        type=int,
        default=50,
        help="Top K results per query (default: 50)"
    )
    parser.add_argument(
        "--fast_mode",
        action="store_true",
        help="Enable fast mode (2-4x speedup)"
    )
    parser.add_argument(
        "--start_from",
        type=int,
        default=0,
        help="Start from chunk index (default: 0). Useful for resuming."
    )
    
    args = parser.parse_args()
    
    # Get total videos in collection
    print("📊 Getting total videos in collection...")
    total_videos = get_total_videos(args.collection)
    
    print(f"\n{'='*80}")
    print(f"📊 PROCESSING CONFIGURATION")
    print(f"{'='*80}")
    print(f"   Collection:        {args.collection}")
    print(f"   Total videos:      {total_videos:,}")
    print(f"   Chunk size:        {args.chunk_size:,}")
    print(f"   Total chunks:     {(total_videos + args.chunk_size - 1) // args.chunk_size}")
    print(f"   Start from chunk:  {args.start_from}")
    print(f"   Threshold:         {args.cosine_thresh}")
    print(f"   Output files:")
    print(f"     - Unique:        {args.unique_csv} (append mode)")
    print(f"     - Duplicates:    {args.report_csv} (append mode)")
    if args.auto_clean:
        print(f"     - Invalid:        {args.invalid_csv} (append mode)")
    print(f"{'='*80}\n")
    
    # Calculate chunks
    total_chunks = (total_videos + args.chunk_size - 1) // args.chunk_size
    start_chunk = args.start_from
    
    # Delete output files if starting from beginning (fresh start)
    if start_chunk == 0:
        import os
        if os.path.exists(args.unique_csv):
            print(f"🗑️  Deleting existing {args.unique_csv} (fresh start)...")
            os.remove(args.unique_csv)
        if os.path.exists(args.report_csv):
            print(f"🗑️  Deleting existing {args.report_csv} (fresh start)...")
            os.remove(args.report_csv)
        if args.auto_clean and os.path.exists(args.invalid_csv):
            print(f"🗑️  Deleting existing {args.invalid_csv} (fresh start)...")
            os.remove(args.invalid_csv)
        print()
    
    # Run chunks
    successful_chunks = 0
    failed_chunks = 0
    start_time = time.time()
    
    for chunk_idx in range(start_chunk, total_chunks):
        chunk_start = chunk_idx * args.chunk_size
        chunk_end = min((chunk_idx + 1) * args.chunk_size, total_videos)
        
        chunk_start_time = time.time()
        success = run_chunk(
            args.collection,
            args.cosine_thresh,
            chunk_start,
            chunk_end,
            args.unique_csv,
            args.report_csv,
            args.auto_clean,
            args.invalid_csv,
            args.batch_size,
            args.num_threads,
            args.fast_mode,
            args.top_k
        )
        
        chunk_elapsed = time.time() - chunk_start_time
        
        if success:
            successful_chunks += 1
            print(f"   ⏱️  Chunk completed in {chunk_elapsed:.1f}s ({chunk_elapsed/60:.1f} min)")
        else:
            failed_chunks += 1
            print(f"   ❌ Chunk failed after {chunk_elapsed:.1f}s")
            print(f"   💡 You can resume from chunk {chunk_idx} using --start_from {chunk_idx}")
            
            # Ask user if they want to continue
            response = input(f"\n   Continue with next chunk? (y/n): ").strip().lower()
            if response != 'y':
                print(f"\n   ⚠️  Stopped by user. Processed {successful_chunks} chunks, {failed_chunks} failed")
                print(f"   💡 Resume with: --start_from {chunk_idx}")
                break
        
        # Progress summary
        elapsed_total = time.time() - start_time
        chunks_remaining = total_chunks - chunk_idx - 1
        if chunks_remaining > 0 and successful_chunks > 0:
            avg_time_per_chunk = elapsed_total / (successful_chunks + failed_chunks)
            estimated_remaining = avg_time_per_chunk * chunks_remaining
            print(f"\n   📊 Progress: {chunk_idx + 1}/{total_chunks} chunks")
            print(f"   ⏱️  Elapsed: {elapsed_total/60:.1f} min, Estimated remaining: {estimated_remaining/60:.1f} min")
    
    # Final summary
    total_elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"🎉 PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"   Successful chunks: {successful_chunks}")
    print(f"   Failed chunks:     {failed_chunks}")
    print(f"   Total time:        {total_elapsed/60:.1f} min ({total_elapsed/3600:.1f} hours)")
    print(f"   Output files:")
    print(f"     - Unique:        {args.unique_csv}")
    print(f"     - Duplicates:    {args.report_csv}")
    if args.auto_clean:
        print(f"     - Invalid:        {args.invalid_csv}")
    print(f"{'='*80}\n")


if __name__ == "__main__":
    main()


