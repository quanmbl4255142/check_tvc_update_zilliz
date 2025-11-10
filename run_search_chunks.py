"""
Script helper để chạy search_duplicates_aggregated.py trên nhiều chunks
Tự động chia 90k dữ liệu thành nhiều chunks và chạy tuần tự
"""

import argparse
import subprocess
import sys
import os
import time
from pathlib import Path


def run_chunk(collection: str, cosine_thresh: float, chunk_start: int, chunk_end: int, 
              base_unique_csv: str, base_report_csv: str, auto_clean: bool,
              batch_size: int, num_threads: int, fast_mode: bool) -> bool:
    """Chạy search cho một chunk và trả về True nếu thành công"""
    
    print(f"\n{'='*80}")
    print(f"📦 PROCESSING CHUNK: {chunk_start} to {chunk_end-1} ({chunk_end - chunk_start} videos)")
    print(f"{'='*80}\n")
    
    cmd = [
        sys.executable,
        "search_duplicates_aggregated.py",
        "--collection", collection,
        "--cosine_thresh", str(cosine_thresh),
        "--unique_csv", base_unique_csv,
        "--report_csv", base_report_csv,
        "--chunk_start", str(chunk_start),
        "--chunk_end", str(chunk_end),
        "--batch_size", str(batch_size),
        "--num_threads", str(num_threads),
    ]
    
    if auto_clean:
        cmd.append("--auto_clean")
    
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


def merge_results(chunk_size: int, total_videos: int, base_unique_csv: str, base_report_csv: str, 
                  output_unique_csv: str, output_report_csv: str):
    """Merge kết quả từ nhiều chunks vào 2 file cuối cùng"""
    import csv
    
    print(f"\n{'='*80}")
    print(f"🔄 MERGING RESULTS FROM CHUNK FILES")
    print(f"{'='*80}\n")
    
    # Find all chunk result files by pattern
    # Files are named: {base}_chunk_{start}_{end}.csv
    base_dir = Path(".")
    unique_files = sorted(base_dir.glob(f"{base_unique_csv}_chunk_*.csv"))
    report_files = sorted(base_dir.glob(f"{base_report_csv}_chunk_*.csv"))
    
    print(f"   Found {len(unique_files)} unique CSV files")
    print(f"   Found {len(report_files)} report CSV files")
    
    if not unique_files and not report_files:
        print(f"   ❌ No chunk files found!")
        print(f"   💡 Make sure chunk files exist in current directory")
        return
    
    # Merge unique videos (deduplicate URLs)
    print(f"\n📝 Merging unique videos...")
    all_unique_urls = set()
    unique_files_processed = 0
    
    for unique_file in unique_files:
        try:
            with open(unique_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                urls_in_file = 0
                for row in reader:
                    url = row.get("decoded_url", "").strip()
                    if url:
                        all_unique_urls.add(url)
                        urls_in_file += 1
                if urls_in_file > 0:
                    unique_files_processed += 1
                    print(f"   ✅ {unique_file.name}: {urls_in_file} URLs")
        except Exception as e:
            print(f"   ⚠️  Error reading {unique_file}: {e}")
    
    # Write merged unique CSV
    with open(output_unique_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["decoded_url"])
        for url in sorted(all_unique_urls):
            writer.writerow([url])
    
    print(f"   ✅ Merged {len(all_unique_urls)} unique URLs from {unique_files_processed} files -> {output_unique_csv}")
    
    # Merge report files (all duplicates)
    print(f"\n📝 Merging duplicate reports...")
    all_duplicates = []
    report_files_processed = 0
    
    for report_file in report_files:
        try:
            with open(report_file, "r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                rows_in_file = 0
                for row in reader:
                    all_duplicates.append(row)
                    rows_in_file += 1
                if rows_in_file > 0:
                    report_files_processed += 1
                    print(f"   ✅ {report_file.name}: {rows_in_file} duplicates")
        except Exception as e:
            print(f"   ⚠️  Error reading {report_file}: {e}")
    
    # Write merged report CSV
    if all_duplicates:
        fieldnames = list(all_duplicates[0].keys())
        with open(output_report_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_duplicates)
        print(f"   ✅ Merged {len(all_duplicates)} duplicate records from {report_files_processed} files -> {output_report_csv}")
    else:
        print(f"   ℹ️  No duplicates found across all chunks")
        # Create empty file
        with open(output_report_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["duplicate_url", "duplicate_job_id", "original_job_id", "original_url", "similarity"])
    
    print(f"\n✅ Merge completed!")
    print(f"   📊 Summary:")
    print(f"      - Unique videos: {len(all_unique_urls):,}")
    print(f"      - Duplicates: {len(all_duplicates):,}")
    if total_videos > 0:
        dedup_rate = (1 - len(all_unique_urls) / total_videos) * 100
        print(f"      - Deduplication rate: {dedup_rate:.1f}%")


def main():
    parser = argparse.ArgumentParser(
        description="Chạy search_duplicates_aggregated.py trên nhiều chunks tự động"
    )
    parser.add_argument(
        "--collection",
        default="video_dedup_tri_quan",
        help="Collection name (default: video_dedup_tri_quan). Run 'python list_collections.py' to see all collections"
    )
    parser.add_argument(
        "--cosine_thresh",
        type=float,
        default=0.85,
        help="Similarity threshold (default: 0.85)"
    )
    parser.add_argument(
        "--total_videos",
        type=int,
        required=True,
        help="Total number of videos in collection (e.g., 90000)"
    )
    parser.add_argument(
        "--chunk_size",
        type=int,
        default=5000,
        help="Size of each chunk (default: 5000). Recommend: 5000-10000"
    )
    parser.add_argument(
        "--start_chunk",
        type=int,
        default=0,
        help="Start from chunk index (default: 0). Useful for resuming"
    )
    parser.add_argument(
        "--end_chunk",
        type=int,
        default=None,
        help="End at chunk index (default: None = process all). Useful for testing"
    )
    parser.add_argument(
        "--base_unique_csv",
        default="FINAL_RESULT_chunk",
        help="Base name for unique CSV files (default: FINAL_RESULT_chunk)"
    )
    parser.add_argument(
        "--base_report_csv",
        default="duplicates_chunk",
        help="Base name for report CSV files (default: duplicates_chunk)"
    )
    parser.add_argument(
        "--output_unique_csv",
        default="FINAL_RESULT_MERGED.csv",
        help="Output merged unique CSV (default: FINAL_RESULT_MERGED.csv)"
    )
    parser.add_argument(
        "--output_report_csv",
        default="duplicates_MERGED.csv",
        help="Output merged report CSV (default: duplicates_MERGED.csv)"
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
        "--fast_mode",
        action="store_true",
        help="Enable fast mode (2-4x speedup)"
    )
    parser.add_argument(
        "--merge_only",
        action="store_true",
        help="Only merge existing chunk results, don't run new chunks"
    )
    parser.add_argument(
        "--skip_merge",
        action="store_true",
        help="Skip merging results (keep separate chunk files)"
    )
    
    args = parser.parse_args()
    
    # Calculate chunks
    total_chunks = (args.total_videos + args.chunk_size - 1) // args.chunk_size
    start_chunk = args.start_chunk
    end_chunk = args.end_chunk if args.end_chunk is not None else total_chunks
    
    print(f"\n{'='*80}")
    print(f"📊 CHUNK PROCESSING CONFIGURATION")
    print(f"{'='*80}")
    print(f"   Total videos:      {args.total_videos:,}")
    print(f"   Chunk size:        {args.chunk_size:,}")
    print(f"   Total chunks:      {total_chunks}")
    print(f"   Processing chunks: {start_chunk} to {end_chunk-1} ({end_chunk - start_chunk} chunks)")
    print(f"   Collection:        {args.collection}")
    print(f"   Threshold:         {args.cosine_thresh}")
    print(f"{'='*80}\n")
    
    if args.merge_only:
        print("🔄 MERGE-ONLY MODE: Merging existing chunk results...")
        merge_results(args.chunk_size, args.total_videos, args.base_unique_csv, args.base_report_csv,
                     args.output_unique_csv, args.output_report_csv)
        return
    
    # Run chunks
    successful_chunks = 0
    failed_chunks = 0
    start_time = time.time()
    
    for chunk_idx in range(start_chunk, end_chunk):
        chunk_start = chunk_idx * args.chunk_size
        chunk_end = min((chunk_idx + 1) * args.chunk_size, args.total_videos)
        
        chunk_start_time = time.time()
        success = run_chunk(
            args.collection,
            args.cosine_thresh,
            chunk_start,
            chunk_end,
            args.base_unique_csv,
            args.base_report_csv,
            args.auto_clean,
            args.batch_size,
            args.num_threads,
            args.fast_mode
        )
        
        chunk_elapsed = time.time() - chunk_start_time
        
        if success:
            successful_chunks += 1
            print(f"   ⏱️  Chunk completed in {chunk_elapsed:.1f}s ({chunk_elapsed/60:.1f} min)")
        else:
            failed_chunks += 1
            print(f"   ❌ Chunk failed after {chunk_elapsed:.1f}s")
            print(f"   💡 You can resume from chunk {chunk_idx} using --start_chunk {chunk_idx}")
            
            # Ask user if they want to continue
            response = input(f"\n   Continue with next chunk? (y/n): ").strip().lower()
            if response != 'y':
                print(f"\n   ⚠️  Stopped by user. Processed {successful_chunks} chunks, {failed_chunks} failed")
                print(f"   💡 Resume with: --start_chunk {chunk_idx}")
                break
        
        # Progress summary
        elapsed_total = time.time() - start_time
        chunks_remaining = (end_chunk - chunk_idx - 1)
        if chunks_remaining > 0 and successful_chunks > 0:
            avg_time_per_chunk = elapsed_total / (successful_chunks + failed_chunks)
            estimated_remaining = avg_time_per_chunk * chunks_remaining
            print(f"\n   📊 Progress: {chunk_idx + 1 - start_chunk}/{end_chunk - start_chunk} chunks")
            print(f"   ⏱️  Elapsed: {elapsed_total/60:.1f} min, Estimated remaining: {estimated_remaining/60:.1f} min")
    
    # Final summary
    total_elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"🎉 CHUNK PROCESSING COMPLETE")
    print(f"{'='*80}")
    print(f"   Successful chunks: {successful_chunks}")
    print(f"   Failed chunks:     {failed_chunks}")
    print(f"   Total time:        {total_elapsed/60:.1f} min ({total_elapsed/3600:.1f} hours)")
    print(f"{'='*80}\n")
    
    # Merge results if requested
    if not args.skip_merge and successful_chunks > 0:
        print("\n🔄 Merging results from all chunks...")
        merge_results(args.chunk_size, args.total_videos, args.base_unique_csv, args.base_report_csv,
                     args.output_unique_csv, args.output_report_csv)
        print(f"\n✅ All done! Check {args.output_unique_csv} and {args.output_report_csv}")
    else:
        print(f"\n💡 Chunk files are saved separately.")
        print(f"   Run with --merge_only to merge them:")
        print(f"   python run_search_chunks.py --merge_only --total_videos {args.total_videos} --base_unique_csv {args.base_unique_csv} --base_report_csv {args.base_report_csv}")


if __name__ == "__main__":
    main()

