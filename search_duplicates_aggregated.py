"""
Search duplicates with AGGREGATED vectors - Đơn giản & Nhanh hơn 3×
Mỗi video chỉ có 1 vector → query đơn giản, không cần multi-frame comparison

✨ NEW: Tích hợp auto-clean để loại bỏ PNG/ảnh và URLs lỗi (--auto_clean)
📊 NEW: Performance tracking với RAM/CPU và breakdown từng phase
"""

import argparse
import os
import sys
import csv
import re
import time
import platform
from typing import List, Dict, Set
from urllib.parse import urlparse

import psutil
from tqdm import tqdm

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


class PerformanceTracker:
    """Track performance metrics: timing, RAM, CPU"""
    
    def __init__(self):
        self.process = psutil.Process()
        self.start_time = time.time()
        self.phase_times = {}
        self.phase_start = None
        self.current_phase = None
        
        # Initial stats
        self.initial_ram_mb = self.process.memory_info().rss / 1024 / 1024
        self.peak_ram_mb = self.initial_ram_mb
        self.cpu_samples = []
        
    def start_phase(self, phase_name: str):
        """Start tracking a phase"""
        self.current_phase = phase_name
        self.phase_start = time.time()
        
    def end_phase(self):
        """End current phase and record time"""
        if self.phase_start and self.current_phase:
            elapsed = time.time() - self.phase_start
            self.phase_times[self.current_phase] = elapsed
            self.current_phase = None
            self.phase_start = None
            
    def update_stats(self):
        """Update RAM and CPU stats"""
        # RAM
        current_ram_mb = self.process.memory_info().rss / 1024 / 1024
        if current_ram_mb > self.peak_ram_mb:
            self.peak_ram_mb = current_ram_mb
        
        # CPU
        try:
            cpu_percent = self.process.cpu_percent(interval=0.1)
            self.cpu_samples.append(cpu_percent)
        except:
            pass
    
    def get_total_time(self) -> float:
        """Get total elapsed time"""
        return time.time() - self.start_time
    
    def get_system_info(self) -> Dict:
        """Get system information"""
        cpu_info = {}
        try:
            cpu_info['cpu_model'] = platform.processor() or "Unknown"
            cpu_info['cpu_cores'] = psutil.cpu_count(logical=False)
            cpu_info['cpu_threads'] = psutil.cpu_count(logical=True)
            cpu_info['ram_total_gb'] = round(psutil.virtual_memory().total / 1024 / 1024 / 1024, 1)
            cpu_info['os'] = f"{platform.system()} {platform.release()}"
        except:
            pass
        return cpu_info
    
    def get_stats(self) -> Dict:
        """Get all statistics"""
        total_time = self.get_total_time()
        final_ram_mb = self.process.memory_info().rss / 1024 / 1024
        
        stats = {
            'total_time': total_time,
            'phase_times': self.phase_times,
            'ram': {
                'initial_mb': round(self.initial_ram_mb, 1),
                'peak_mb': round(self.peak_ram_mb, 1),
                'final_mb': round(final_ram_mb, 1),
                'used_mb': round(self.peak_ram_mb - self.initial_ram_mb, 1)
            },
            'cpu': {
                'avg_percent': round(sum(self.cpu_samples) / len(self.cpu_samples), 1) if self.cpu_samples else 0,
                'peak_percent': round(max(self.cpu_samples), 1) if self.cpu_samples else 0
            },
            'system': self.get_system_info()
        }
        
        return stats
    
    def print_report(self, total_videos: int, unique_count: int, dup_count: int):
        """Print beautiful performance report"""
        stats = self.get_stats()
        total_time = stats['total_time']
        videos_per_sec = total_videos / total_time if total_time > 0 else 0
        
        print("\n" + "="*70)
        print("📊 PERFORMANCE REPORT")
        print("="*70)
        
        # Processing stats
        print(f"\n⏱️  PROCESSING:")
        print(f"   Total videos:     {total_videos:,}")
        print(f"   Unique videos:    {unique_count:,}")
        print(f"   Duplicates:       {dup_count:,}")
        print(f"   Total time:       {total_time:.1f}s ({total_time/60:.1f} min)")
        print(f"   Average rate:     {videos_per_sec:.2f} videos/second")
        
        # Phase breakdown
        if stats['phase_times']:
            print(f"\n📋 PHASE BREAKDOWN:")
            for phase, duration in stats['phase_times'].items():
                percent = (duration / total_time * 100) if total_time > 0 else 0
                print(f"   {phase:20s}: {duration:6.1f}s ({percent:5.1f}%)")
        
        # System info
        system = stats['system']
        if system:
            print(f"\n💻 SYSTEM INFO:")
            if 'cpu_model' in system:
                print(f"   CPU: {system['cpu_model']}")
            if 'cpu_cores' in system:
                print(f"   Cores: {system['cpu_cores']} cores, {system['cpu_threads']} threads")
            if 'ram_total_gb' in system:
                print(f"   RAM: {system['ram_total_gb']} GB total")
            if 'os' in system:
                print(f"   OS: {system['os']}")
        
        # Resource usage
        ram = stats['ram']
        cpu = stats['cpu']
        ram_percent = (ram['peak_mb'] / (stats['system'].get('ram_total_gb', 16) * 1024) * 100) if stats['system'].get('ram_total_gb') else 0
        
        print(f"\n📊 RESOURCE USAGE:")
        print(f"   Peak RAM:         {ram['peak_mb']:.1f} MB ({ram_percent:.1f}% of total)")
        print(f"   RAM used:         {ram['used_mb']:.1f} MB")
        print(f"   Average CPU:      {cpu['avg_percent']:.1f}%")
        print(f"   Peak CPU:         {cpu['peak_percent']:.1f}%")
        
        print("\n" + "="*70 + "\n")


def is_valid_video_url(url: str) -> tuple[bool, str]:
    """
    Kiểm tra URL có phải video hợp lệ không
    Returns: (is_valid, reason)
    """
    # Loại bỏ URLs quá ngắn hoặc lỗi
    if not url or len(url) < 20:
        return False, "URL quá ngắn hoặc lỗi"
    
    # Phải bắt đầu với http/https
    if not url.startswith(('http://', 'https://')):
        return False, "Không phải URL hợp lệ"
    
    # Loại bỏ file ảnh
    image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp']
    url_lower = url.lower()
    for ext in image_extensions:
        if url_lower.endswith(ext) or ext in url_lower.split('?')[0]:
            return False, f"File ảnh ({ext})"
    
    # Phải có video extension hoặc streaming format
    video_indicators = [
        '.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv',
        '.m3u8', 'video', 'play_', 'videoplayback'
    ]
    
    has_video_indicator = any(indicator in url_lower for indicator in video_indicators)
    if not has_video_indicator:
        return False, "Không có indicator video"
    
    # Kiểm tra domain hợp lệ
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return False, "Domain không hợp lệ"
    except:
        return False, "Parse URL thất bại"
    
    return True, "OK"


def search_duplicates_aggregated(
    collection_name: str,
    similarity_threshold: float,
    unique_csv: str,
    report_csv: str,
    top_k: int = None,
    auto_clean: bool = False,
    invalid_csv: str = None,
    enable_tracking: bool = True,
) -> tuple[int, int]:
    """
    Search duplicates với aggregated vectors - ĐƠN GIẢN HƠN NHIỀU!
    Mỗi video chỉ có 1 vector → không cần group frames
    """
    if top_k is None:
        top_k = DEFAULT_TOP_K
    
    # Initialize performance tracker
    tracker = PerformanceTracker() if enable_tracking else None
    
    if tracker:
        tracker.start_phase("Connect to Milvus")
    
    print("🔌 Connecting to Milvus...")
    params = get_connection_params()
    connections.connect("default", **params)
    print("✅ Connected!")
    
    if tracker:
        tracker.end_phase()
    
    # Load collection
    if tracker:
        tracker.start_phase("Load collection")
    
    if not utility.has_collection(collection_name):
        print(f"❌ ERROR: Collection '{collection_name}' not found!", file=sys.stderr)
        print(f"Please run upload_aggregated_to_milvus.py first.", file=sys.stderr)
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    total_entities = collection.num_entities
    print(f"📊 Collection has {total_entities} vectors")
    print(f"🎯 Each video = 1 vector (aggregated from 3 frames)")
    
    # Get all videos - Query in batches using ID range (avoid offset+limit limit)
    # Note: gRPC message size limit is ~4MB
    # Each vector: 512 dims × 4 bytes = 2KB
    # Safe batch size: ~1500 vectors (3MB) to stay under 4MB limit
    # Also: Zilliz limit: offset + limit <= 16384, so we use ID range instead
    MAX_QUERY_LIMIT = 1500
    print(f"\n🔍 Fetching all videos from Zilliz (in batches of {MAX_QUERY_LIMIT})...")
    
    # First, get all IDs (lightweight, no embeddings)
    # Query in small batches to avoid offset+limit <= 16384 limit
    print("   Step 1: Fetching all IDs...")
    all_ids = []
    id_offset = 0
    id_batch_size = 10000  # Safe: offset + limit = 10000 < 16384
    
    while id_offset < total_entities:
        remaining = total_entities - id_offset
        current_limit = min(id_batch_size, remaining)
        
        # Ensure offset + limit <= 16384
        if id_offset + current_limit > 16384:
            current_limit = 16384 - id_offset
            if current_limit <= 0:
                # Switch to ID range query
                max_id_so_far = max(all_ids) if all_ids else -1
                remaining_batch = collection.query(
                    expr=f"id > {max_id_so_far}",
                    output_fields=["id"],
                    limit=16384
                )
                if remaining_batch:
                    all_ids.extend([item["id"] for item in remaining_batch])
                    # Continue with ID range
                    while len(remaining_batch) == 16384:
                        new_max = max([item["id"] for item in remaining_batch])
                        remaining_batch = collection.query(
                            expr=f"id > {new_max}",
                            output_fields=["id"],
                            limit=16384
                        )
                        if remaining_batch:
                            all_ids.extend([item["id"] for item in remaining_batch])
                        else:
                            break
                break
        
        id_batch = collection.query(
            expr="id >= 0",
            output_fields=["id"],
            limit=current_limit,
            offset=id_offset
        )
        if not id_batch:
            break
        all_ids.extend([item["id"] for item in id_batch])
        id_offset += len(id_batch)
        
        if len(id_batch) < current_limit:
            break
    
    print(f"   ✅ Found {len(all_ids)} IDs")
    print(f"   Step 2: Fetching embeddings in batches...")
    
    # Now query embeddings by ID ranges
    all_data = []
    batch_num = 1
    
    for i in range(0, len(all_ids), MAX_QUERY_LIMIT):
        batch_ids = all_ids[i:i + MAX_QUERY_LIMIT]
        
        if tracker:
            tracker.update_stats()
        
        try:
            # Query by ID list (more efficient than offset)
            id_list_str = ",".join([str(id_val) for id_val in batch_ids])
            batch_data = collection.query(
                expr=f"id in [{id_list_str}]",
                output_fields=["job_id", "url", "embedding"]
            )
        except Exception as e:
            # If ID list too large, query in smaller chunks
            if "message larger than max" in str(e) or "RESOURCE_EXHAUSTED" in str(e) or len(batch_ids) > 1000:
                print(f"   ⚠️  Batch too large, splitting...")
                # Split into smaller chunks
                chunk_size = 500
                batch_data = []
                for j in range(0, len(batch_ids), chunk_size):
                    chunk_ids = batch_ids[j:j + chunk_size]
                    chunk_id_str = ",".join([str(id_val) for id_val in chunk_ids])
                    chunk_data = collection.query(
                        expr=f"id in [{chunk_id_str}]",
                        output_fields=["job_id", "url", "embedding"]
                    )
                    batch_data.extend(chunk_data)
            else:
                raise
        
        all_data.extend(batch_data)
        
        print(f"   Batch {batch_num}: Loaded {len(batch_data)} videos (Total: {len(all_data)}/{total_entities})")
        
        if len(batch_data) == 0:
            break
        
        batch_num += 1
    
    print(f"✅ Loaded {len(all_data)} videos total")
    
    if tracker:
        tracker.end_phase()
        tracker.start_phase("Search duplicates")
    
    print(f"🎯 Searching for duplicates (threshold: {similarity_threshold})...\n")
    
    # Track duplicates
    seen_jobs: Set[str] = set()
    unique_videos: List[Dict] = []
    duplicates: List[Dict] = []
    
    # Use tqdm for progress bar if available
    video_iterator = tqdm(all_data, desc="🔍 Searching", unit="video") if tracker else all_data
    
    processed = 0
    for idx, video in enumerate(video_iterator):
        job_id = video["job_id"]
        
        if job_id in seen_jobs:
            continue  # Already marked as duplicate
        
        processed += 1
        
        # Update resource stats periodically
        if tracker and processed % 100 == 0:
            tracker.update_stats()
        
        # Show progress without tqdm (fallback)
        if not tracker and processed % 50 == 0:
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
    
    if tracker:
        tracker.end_phase()
        tracker.start_phase("Clean URLs" if auto_clean else "Write results")
    
    # Write results
    print(f"\n💾 Writing results...")
    
    # Auto-clean: Filter invalid URLs (PNG, images, broken URLs)
    final_unique_videos = unique_videos
    invalid_urls = []
    
    if auto_clean:
        print(f"\n🧼 Auto-clean enabled - filtering invalid URLs...")
        valid_videos = []
        
        for video in unique_videos:
            url = video["url"]
            
            # Remove quotes if exists
            if url.startswith('"') and url.endswith('"'):
                url = url[1:-1]
            
            is_valid, reason = is_valid_video_url(url)
            
            if is_valid:
                valid_videos.append(video)
            else:
                invalid_urls.append({
                    'url': url,
                    'job_id': video.get('job_id', ''),
                    'reason': reason
                })
        
        final_unique_videos = valid_videos
        print(f"   ✅ Valid videos: {len(valid_videos)}")
        print(f"   ❌ Invalid URLs removed: {len(invalid_urls)}")
        
        if tracker:
            tracker.end_phase()
            tracker.start_phase("Write results")
    
    # Unique URLs
    os.makedirs(os.path.dirname(unique_csv) or ".", exist_ok=True)
    with open(unique_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["decoded_url"])
        for video in final_unique_videos:
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
    
    # Write invalid URLs report (if auto-clean enabled)
    if auto_clean and invalid_urls:
        if invalid_csv is None:
            invalid_csv = "invalid_urls.csv"
        
        os.makedirs(os.path.dirname(invalid_csv) or ".", exist_ok=True)
        with open(invalid_csv, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(['invalid_url', 'job_id', 'reason'])
            for item in invalid_urls:
                writer.writerow([item['url'], item['job_id'], item['reason']])
        
        print(f"   📄 Invalid URLs report: {invalid_csv}")
    
    print(f"✅ Results written!")
    print(f"   Unique videos: {len(final_unique_videos)}")
    print(f"   Duplicates: {len(duplicates)}")
    
    if tracker:
        tracker.end_phase()
        # Print performance report
        tracker.print_report(len(all_data), len(final_unique_videos), len(duplicates))
    
    return len(final_unique_videos), len(duplicates)


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
        "--auto_clean",
        action="store_true",
        help="Auto-clean: remove PNG/images and invalid URLs from results"
    )
    parser.add_argument(
        "--invalid_csv",
        default="invalid_urls.csv",
        help="Invalid URLs report (default: invalid_urls.csv)"
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
            args.top_k,
            args.auto_clean,
            args.invalid_csv,
            enable_tracking=True  # Always enable tracking
        )
        
        print(f"\n🎉 Search complete!")
        print(f"   ✅ Unique videos: {unique_count}")
        print(f"   ❌ Duplicates found: {dup_count}")
        print(f"   → Unique URLs: {args.unique_csv}")
        print(f"   → Duplicates report: {args.report_csv}")
        
        if args.auto_clean:
            print(f"\n🧼 Auto-clean was enabled:")
            print(f"   → Invalid URLs removed (PNG, images, broken URLs)")
            print(f"   → Check {args.invalid_csv} for details")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

