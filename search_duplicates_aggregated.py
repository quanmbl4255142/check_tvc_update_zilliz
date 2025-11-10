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
from typing import List, Dict, Set, Tuple
from urllib.parse import urlparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

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
    INDEX_TYPE,
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
    batch_size: int = 10,
    num_threads: int = 4,
    chunk_start: int = None,
    chunk_end: int = None,
    fast_mode: bool = False,
    append_mode: bool = False,
) -> tuple[int, int]:
    """
    Search duplicates với aggregated vectors - ĐƠN GIẢN HƠN NHIỀU!
    Mỗi video chỉ có 1 vector → không cần group frames
    
    ⚡ OPTIMIZED: Batch search + Parallel processing
    - Batch size: Search nhiều videos cùng lúc (max 10 per Zilliz limit)
    - Parallel: Nhiều threads chạy song song (tận dụng I/O wait)
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
        print(f"Please run direct_upload_to_zilliz.py or upload_aggregated_to_milvus.py first.", file=sys.stderr)
        print(f"\n💡 TIP: Make sure you're using the correct collection name.", file=sys.stderr)
        print(f"   If you uploaded with direct_upload_to_zilliz.py, use that collection name.", file=sys.stderr)
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    total_entities = collection.num_entities
    print(f"📊 Collection '{collection_name}' has {total_entities} vectors")
    print(f"🎯 Each video = 1 vector (from first frame or aggregated)")
    
    # Get all videos - Query in batches using ID range (avoid offset+limit limit)
    # Note: gRPC message size limit is ~4MB
    # Each vector: 512 dims × 4 bytes = 2KB
    # Safe batch size: ~1500 vectors (3MB) to stay under 4MB limit
    # Also: Zilliz limit: offset + limit <= 16384, so we use ID range instead
    MAX_QUERY_LIMIT = 1500
    print(f"\n🔍 Fetching all videos from Zilliz (in batches of {MAX_QUERY_LIMIT})...")
    
    # Optimize: If chunk mode, only load IDs in range
    if chunk_start is not None or chunk_end is not None:
        start_idx = chunk_start if chunk_start is not None else 0
        end_idx = chunk_end if chunk_end is not None else total_entities
        start_idx = max(0, start_idx)
        end_idx = min(total_entities, end_idx)
        
        print(f"📦 CHUNK MODE: Will only load videos {start_idx} to {end_idx-1} ({end_idx - start_idx} videos)")
        
        # STRATEGY: Use position-based query (offset/limit) for chunks < 16384
        # This is more reliable than job_id range query when job_ids are not sequential or not starting from 0
        chunk_size = end_idx - start_idx
        all_data = []
        
        if start_idx + chunk_size <= 16384:
            # Safe to use offset/limit (within 16384 limit)
            print(f"   💡 Using position-based query (offset/limit) - more reliable for sequential access")
            print(f"   📊 Querying videos at positions [{start_idx}, {end_idx}) using offset/limit...")
            
            # Use position-based query: query by offset and limit
            # This gets videos in insertion order, not by job_id
            try:
                # Query in batches to avoid message size limit
                batch_size_query = min(MAX_QUERY_LIMIT, chunk_size)
                offset = start_idx
                remaining = chunk_size
                
                while remaining > 0:
                    current_limit = min(batch_size_query, remaining)
                    
                    print(f"   📦 Fetching batch: offset={offset}, limit={current_limit}...", end=" ", flush=True)
                    
                    batch_data = collection.query(
                        expr="id >= 0",  # Query all, then use offset/limit
                        output_fields=["job_id", "url", "embedding"],
                        limit=current_limit,
                        offset=offset
                    )
                    
                    if not batch_data:
                        print(f"❌ No more data at offset {offset}")
                        break
                    
                    all_data.extend(batch_data)
                    print(f"✅ Got {len(batch_data)} videos (Total: {len(all_data)}/{chunk_size})")
                    
                    offset += len(batch_data)
                    remaining -= len(batch_data)
                    
                    if len(batch_data) < current_limit:
                        # Got less than requested, reached end
                        break
                
                if len(all_data) != chunk_size:
                    print(f"   ⚠️  WARNING: Expected {chunk_size} videos but got {len(all_data)}")
                    print(f"   💡 This might be normal if collection has less than {end_idx} videos")
                else:
                    print(f"   ✅ Successfully loaded {len(all_data)} videos using position-based query")
                
                use_position_query = True
                use_job_id_query = False
                
            except Exception as e:
                print(f"   ❌ ERROR with position-based query: {e}")
                import traceback
                traceback.print_exc()
                print(f"   💡 Falling back to job_id range query...")
                use_position_query = False
                use_job_id_query = True
        else:
            # Must use job_id range query (offset + limit would exceed 16384)
            print(f"   💡 Using job_id range query (offset + limit would exceed 16384 limit)")
            use_position_query = False
            use_job_id_query = True
        
        # Only use job_id range query if position-based query is not available or failed
        if use_job_id_query:
            # CRITICAL FIX: Query by job_id range instead of position (offset/limit)
            # When uploading with direct_upload_to_zilliz.py --start 24500, job_ids use format :04d
            # Format: url_0000 to url_9999 (4 digits), url_10000 to url_99999 (5 digits, no leading zero)
            # 
            # IMPORTANT: String comparison issue when range spans < 10000 and >= 10000
            # Example: "url_9999" < "url_10000" → False (wrong! because "9" > "1" in string comparison)
            # Solution: If range includes < 10000, we need to handle it specially
            # But if range is all >= 10000, string comparison works correctly
            
            # Try multiple job_id formats to handle different upload scenarios
            # Format 1: :04d (4 digits with leading zeros) - used by direct_upload_to_zilliz.py
            # Format 2: No leading zeros for numbers >= 10000
            # Format 3: Always no leading zeros
            
            def try_job_id_formats(start_idx, end_idx):
                """Try different job_id formats and return successful query result"""
                formats_to_try = [
                    # Format 1: Always 4 digits with leading zeros (url_0000, url_0066, url_66500)
                    (f"url_{start_idx:04d}", f"url_{end_idx:04d}", ":04d format"),
                    # Format 2: No leading zeros (url_0, url_66, url_66500)
                    (f"url_{start_idx}", f"url_{end_idx}", "no leading zeros"),
                    # Format 3: Try with underscore variations
                    (f"url_{start_idx:05d}", f"url_{end_idx:05d}", ":05d format"),
                ]
                
                # First, detect which format works
                working_format = None
                format_func = None
                for start_job_id, end_job_id, format_name in formats_to_try:
                    try:
                        print(f"   🔍 Trying job_id format: {format_name} ('{start_job_id}' to '{end_job_id}')...")
                        query_result = collection.query(
                            expr=f'job_id >= "{start_job_id}" and job_id < "{end_job_id}"',
                            output_fields=["job_id", "url", "embedding"],
                            limit=10  # Just check if query works
                        )
                        if query_result:
                            print(f"   ✅ Format {format_name} works! Found {len(query_result)} sample results")
                            working_format = format_name
                            # Create format function for this format (capture format_name in closure)
                            captured_format = format_name  # Capture in closure
                            def make_job_id(num):
                                if captured_format == ":04d format":
                                    return f"url_{num:04d}"
                                elif captured_format == ":05d format":
                                    return f"url_{num:05d}"
                                else:  # no leading zeros
                                    return f"url_{num}"
                            format_func = make_job_id
                            break
                    except Exception as format_err:
                        print(f"   ⚠️  Format {format_name} failed: {format_err}")
                        continue
                
                if working_format is None:
                    return None, None, None, None
                
                # Now query full range in batches to avoid gRPC message size limit
                # gRPC limit is ~4MB, so we query in batches of ~1000-2000 videos
                BATCH_SIZE = 1500  # Query ~1500 videos per batch to stay under 4MB limit
                all_results = []
                
                print(f"   📦 Querying range in batches of ~{BATCH_SIZE} videos to avoid gRPC size limit...")
                for batch_start in range(start_idx, end_idx, BATCH_SIZE):
                    batch_end = min(batch_start + BATCH_SIZE, end_idx)
                    batch_start_job_id = format_func(batch_start)
                    batch_end_job_id = format_func(batch_end)
                    
                    try:
                        batch_result = collection.query(
                            expr=f'job_id >= "{batch_start_job_id}" and job_id < "{batch_end_job_id}"',
                            output_fields=["job_id", "url", "embedding"]
                        )
                        if batch_result:
                            all_results.extend(batch_result)
                            print(f"      ✅ Batch {batch_start}-{batch_end}: Found {len(batch_result)} videos (total: {len(all_results)})")
                        else:
                            print(f"      ⚠️  Batch {batch_start}-{batch_end}: No results")
                    except Exception as batch_err:
                        error_msg = str(batch_err)
                        if "message larger than max" in error_msg:
                            # Even batch is too large, reduce batch size
                            print(f"      ⚠️  Batch {batch_start}-{batch_end} too large, trying smaller batch...")
                            # Try with smaller batch
                            SMALLER_BATCH = 500
                            for small_start in range(batch_start, batch_end, SMALLER_BATCH):
                                small_end = min(small_start + SMALLER_BATCH, batch_end)
                                small_start_job_id = format_func(small_start)
                                small_end_job_id = format_func(small_end)
                                try:
                                    small_batch_result = collection.query(
                                        expr=f'job_id >= "{small_start_job_id}" and job_id < "{small_end_job_id}"',
                                        output_fields=["job_id", "url", "embedding"]
                                    )
                                    if small_batch_result:
                                        all_results.extend(small_batch_result)
                                        print(f"         ✅ Small batch {small_start}-{small_end}: Found {len(small_batch_result)} videos")
                                except Exception as small_err:
                                    print(f"         ❌ Small batch {small_start}-{small_end} failed: {small_err}")
                                    raise
                        else:
                            print(f"      ❌ Batch {batch_start}-{batch_end} failed: {batch_err}")
                            raise
                
                start_job_id_used = format_func(start_idx)
                end_job_id_used = format_func(end_idx)
                return all_results, start_job_id_used, end_job_id_used, working_format
            
            # Query by job_id range - try multiple formats
            query_result = None
            start_job_id_used = None
            end_job_id_used = None
            format_used = None
            
            try:
                # First, try to detect job_id format by querying a sample
                print(f"   🔍 Detecting job_id format in collection...")
                try:
                    sample_query = collection.query(
                        expr='job_id like "url_%"',
                        output_fields=["job_id"],
                        limit=10
                    )
                    if sample_query:
                        sample_job_ids = [item['job_id'] for item in sample_query]
                        print(f"   📊 Sample job_ids found: {sample_job_ids[:3]}...")
                        # Analyze format
                        sample_nums = []
                        for jid in sample_job_ids:
                            try:
                                num = int(jid.split('_')[1])
                                sample_nums.append(num)
                            except:
                                pass
                        if sample_nums:
                            print(f"   💡 Detected numeric range in samples: {min(sample_nums)} to {max(sample_nums)}")
                except Exception as sample_err:
                    print(f"   ⚠️  Could not query samples: {sample_err}")
                
                # Try different formats
                query_result, start_job_id_used, end_job_id_used, format_used = try_job_id_formats(start_idx, end_idx)
                
                if query_result is None:
                    raise Exception("All job_id formats failed. Could not find matching format.")
                
                print(f"   ✅ Using format: {format_used}")
                print(f"   📋 Querying job_id range: '{start_job_id_used}' to '{end_job_id_used}' (exclusive)")
                
                if query_result:
                    # CRITICAL: Always filter by numeric comparison to ensure accuracy
                    # String comparison can fail when job_ids have different digit counts
                    def extract_job_num(job_id_str):
                        try:
                            return int(job_id_str.split('_')[1])
                        except:
                            return -1
                    
                    # Filter to ensure all job_ids are in numeric range
                    filtered_data = [
                        item for item in query_result
                        if start_idx <= extract_job_num(item["job_id"]) < end_idx
                    ]
                    
                    if len(filtered_data) != len(query_result):
                        print(f"   ⚠️  Filtered {len(query_result) - len(filtered_data)} videos outside numeric range [{start_idx}, {end_idx})")
                    
                    all_data = filtered_data
                    print(f"   ✅ Found {len(all_data)} videos with job_id in range [{start_job_id_used}, {end_job_id_used})")
                    
                    # Verify job_ids are in expected range
                    if all_data:
                        job_ids_found = [item["job_id"] for item in all_data]
                        job_ids_sorted = sorted(job_ids_found)
                        print(f"   📊 Job ID range found: {job_ids_sorted[0]} to {job_ids_sorted[-1]}")
                        
                        job_nums = [extract_job_num(jid) for jid in job_ids_found]
                        if job_nums and all(n >= 0 for n in job_nums):
                            min_job_num = min(job_nums)
                            max_job_num = max(job_nums)
                            print(f"   📊 Numeric range: {min_job_num} to {max_job_num} (expected: {start_idx} to {end_idx-1})")
                            
                            # Check if range matches (should always match after filtering)
                            if min_job_num < start_idx or max_job_num >= end_idx:
                                print(f"   ⚠️  WARNING: Job ID range doesn't match expected range!")
                                print(f"      Expected: [{start_idx}, {end_idx})")
                                print(f"      Found: [{min_job_num}, {max_job_num+1})")
                        
                        # Check count
                        expected_count = end_idx - start_idx
                        if len(all_data) != expected_count:
                            print(f"   ⚠️  WARNING: Expected {expected_count} videos but got {len(all_data)}")
                            print(f"   💡 This might be normal if some videos failed to upload")
                    else:
                        print(f"   ❌ ERROR: No videos found after filtering to numeric range [{start_idx}, {end_idx})")
                        sys.exit(1)
                else:
                    print(f"   ❌ ERROR: No videos found with job_id in range")
                    if start_job_id_used:
                        print(f"   📋 Tried range: '{start_job_id_used}' to '{end_job_id_used}'")
                    print(f"   💡 TIP: Check if you uploaded with the correct --start parameter")
                    print(f"   💡 TIP: Verify job_id format in Zilliz collection")
                    print(f"   💡 TIP: Check if chunk range [{start_idx}, {end_idx}) contains any data")
                    sys.exit(1)
            except Exception as e:
                print(f"   ❌ ERROR querying by job_id range: {e}")
                import traceback
                print(f"   🔍 Full error traceback:")
                traceback.print_exc()
                sys.exit(1)
        
        # Verify we have data
        if not all_data:
            print(f"   ❌ ERROR: No videos loaded! Check chunk range and collection data.")
            sys.exit(1)
        
        print(f"   ✅ Successfully loaded {len(all_data)} videos (expected: {chunk_size})")
        
    else:
        # Full mode (not chunk mode) - load all videos
        print("   Step 1: Fetching all IDs...")
        all_ids = []
        id_offset = 0
        id_batch_size = 10000  # Safe: offset + limit = 10000 < 16384
        
        # Use offset/limit until we hit the 16384 limit, then switch to ID range query
        while id_offset < total_entities:
            remaining = total_entities - id_offset
            current_limit = min(id_batch_size, remaining)
            
            # Check if we're about to hit the 16384 limit
            if id_offset + current_limit > 16384:
                # Use the remaining offset/limit space
                current_limit = 16384 - id_offset
                if current_limit <= 0:
                    # We've exhausted offset/limit, switch to ID range query
                    break
                
                # Last batch using offset/limit
                id_batch = collection.query(
                    expr="id >= 0",
                    output_fields=["id"],
                    limit=current_limit,
                    offset=id_offset
                )
                if id_batch:
                    all_ids.extend([item["id"] for item in id_batch])
                    id_offset += len(id_batch)
                break
            
            # Normal offset/limit query
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
        
        # If we haven't fetched all IDs yet, continue with ID range query
        if len(all_ids) < total_entities:
            print(f"   ⚠️  Fetched {len(all_ids)}/{total_entities} using offset/limit, switching to ID range query...")
            max_id_so_far = max(all_ids) if all_ids else -1
            
            # Continue fetching using ID range query
            while len(all_ids) < total_entities:
                remaining_batch = collection.query(
                    expr=f"id > {max_id_so_far}",
                    output_fields=["id"],
                    limit=16384
                )
                if not remaining_batch:
                    break
                
                batch_ids = [item["id"] for item in remaining_batch]
                all_ids.extend(batch_ids)
                max_id_so_far = max(batch_ids)
                
                if len(remaining_batch) < 16384:
                    break
        
        print(f"   ✅ Found {len(all_ids)} IDs (expected: {total_entities})")
        
        print(f"   Step 2: Fetching embeddings in batches...")
        print(f"   📊 Will fetch embeddings for {len(all_ids)} videos in batches of {MAX_QUERY_LIMIT}...")
        
        # Now query embeddings by ID ranges
        all_data = []
        batch_num = 1
        total_batches = (len(all_ids) + MAX_QUERY_LIMIT - 1) // MAX_QUERY_LIMIT
        
        for i in range(0, len(all_ids), MAX_QUERY_LIMIT):
            batch_ids = all_ids[i:i + MAX_QUERY_LIMIT]
            
            if tracker:
                tracker.update_stats()
            
            print(f"   📦 Batch {batch_num}/{total_batches}: Querying {len(batch_ids)} IDs...", end=" ", flush=True)
            
            try:
                # Query by ID list (more efficient than offset)
                # Use smaller chunks to avoid message size limit
                chunk_size = 500  # Safe size to avoid gRPC message limit
                batch_data = []
                
                for j in range(0, len(batch_ids), chunk_size):
                    chunk_ids = batch_ids[j:j + chunk_size]
                    id_list_str = ",".join([str(id_val) for id_val in chunk_ids])
                    
                    try:
                        chunk_data = collection.query(
                            expr=f"id in [{id_list_str}]",
                            output_fields=["job_id", "url", "embedding"]
                        )
                        batch_data.extend(chunk_data)
                    except Exception as e:
                        # If still too large, split even smaller
                        if "message larger than max" in str(e) or "RESOURCE_EXHAUSTED" in str(e):
                            print(f"\n   ⚠️  Chunk still too large, splitting to 100 IDs...")
                            micro_chunk_size = 100
                            for k in range(0, len(chunk_ids), micro_chunk_size):
                                micro_chunk_ids = chunk_ids[k:k + micro_chunk_size]
                                micro_id_str = ",".join([str(id_val) for id_val in micro_chunk_ids])
                                micro_data = collection.query(
                                    expr=f"id in [{micro_id_str}]",
                                    output_fields=["job_id", "url", "embedding"]
                                )
                                batch_data.extend(micro_data)
                        else:
                            print(f"\n   ⚠️  Error querying chunk: {e}")
                            raise
            
            except Exception as e:
                print(f"\n   ❌ ERROR fetching batch {batch_num}: {e}")
                print(f"   ⚠️  Skipping this batch and continuing...")
                continue
            
            all_data.extend(batch_data)
            print(f"✅ Loaded {len(batch_data)} videos (Total: {len(all_data)}/{len(all_ids)})")
            
            if len(batch_data) == 0:
                print(f"   ⚠️  No data returned for batch {batch_num}, stopping...")
                break
            
            batch_num += 1
        
        print(f"✅ Loaded {len(all_data)} videos total")
    
    if tracker:
        tracker.end_phase()
        tracker.start_phase("Search duplicates")
    
    print(f"🎯 Searching for duplicates (threshold: {similarity_threshold})...")
    print(f"📋 Using Two-Pass Algorithm for maximum accuracy")
    print(f"⚡ OPTIMIZED: Batch size={batch_size}, Threads={num_threads}")
    if fast_mode:
        print(f"⚡ FAST MODE: Using optimized search params for 2-4x speedup")
    print()
    
    # ============================================================
    # PASS 1: Find all duplicate pairs (without classification)
    # ============================================================
    print("🔍 PASS 1: Finding all duplicate pairs...")
    
    # Store all duplicate pairs: (job_id1, job_id2, similarity)
    duplicate_pairs: List[tuple] = []
    duplicate_pairs_lock = Lock()  # Thread-safe lock
    video_info: Dict[str, Dict] = {}  # job_id -> {url, embedding}
    
    # Build video info map
    for video in all_data:
        video_info[video["job_id"]] = {
            "url": video["url"],
            "embedding": video["embedding"]
        }
    
    # OPTIMIZATION: Use faster search params if fast_mode
    # Lower nprobe = faster but slightly less accurate
    fast_search_params = SEARCH_PARAMS.copy()
    if INDEX_TYPE == "IVF_FLAT":
        # Reduce nprobe for speed (default 16 -> 8 for 2x speed, 4 for 4x speed)
        fast_search_params["params"]["nprobe"] = 8
    elif INDEX_TYPE == "HNSW":
        # Reduce ef for speed (default 64 -> 32 for 2x speed)
        fast_search_params["params"]["ef"] = 32
    
    # Helper function to search a batch of videos
    def search_batch(batch_videos: List[Dict], use_fast: bool = False) -> List[tuple]:
        """Search a batch of videos and return duplicate pairs"""
        batch_pairs = []
        
        try:
            # Extract embeddings from batch
            batch_embeddings = [v["embedding"] for v in batch_videos]
            batch_job_ids = [v["job_id"] for v in batch_videos]
            
            # Choose search params based on mode
            search_params = fast_search_params if use_fast else SEARCH_PARAMS
            
            # Batch search on Zilliz
            search_results = collection.search(
                data=batch_embeddings,
                anns_field="embedding",
                param=search_params,
                limit=top_k,
                output_fields=["job_id", "url"]
            )
            
            # Process results for each video in batch
            for idx, (job_id, results) in enumerate(zip(batch_job_ids, search_results)):
                for hit in results:
                    hit_job_id = hit.entity.get("job_id")
                    
                    # Skip self
                    if hit_job_id == job_id:
                        continue
                    
                    # IMPROVEMENT 5: Validate similarity score
                    # For IP metric, score should be in range [0, 1] for normalized vectors
                    # But due to floating point precision, can be slightly > 1.0
                    score = float(hit.score)
                    if score > 1.1:  # Sanity check: if way too high, skip
                        continue
                    # Clamp to [0, 1] for safety
                    score = max(0.0, min(1.0, score))
                    
                    # Only add if similarity >= threshold
                    if score >= similarity_threshold:
                        # Add pair (normalize: always smaller job_id first to avoid duplicates)
                        pair = tuple(sorted([job_id, hit_job_id])) + (score,)
                        batch_pairs.append(pair)
        
        except Exception as e:
            # Log error but continue with other batches
            batch_ids_str = ", ".join([v["job_id"] for v in batch_videos[:3]])
            print(f"⚠️  Error searching batch (first IDs: {batch_ids_str}...): {e}")
        
        return batch_pairs
    
    # Divide videos into batches
    # CRITICAL: Ensure batch_size is valid (max 10 per Zilliz limit)
    search_batch_size = min(batch_size, 10)  # Zilliz limit: max 10 vectors per search
    if search_batch_size != batch_size:
        print(f"   ⚠️  WARNING: batch_size ({batch_size}) > 10, limiting to 10 (Zilliz limit)")
    
    batches = []
    for i in range(0, len(all_data), search_batch_size):
        batch = all_data[i:i + search_batch_size]
        batches.append(batch)
    
    total_batches = len(batches)
    print(f"   📦 Divided into {total_batches} batches (batch size: {search_batch_size})")
    print(f"   🚀 Starting parallel search with {num_threads} threads...\n")
    
    # Parallel batch search
    processed_batches = 0
    with ThreadPoolExecutor(max_workers=num_threads) as executor:
        # Submit all batches (use fast_mode if enabled)
        future_to_batch = {
            executor.submit(search_batch, batch, fast_mode): batch 
            for batch in batches
        }
        
        # Process completed batches with progress bar
        progress_bar = tqdm(total=total_batches, desc="   Pass 1", unit="batch") if tracker else None
        
        for future in as_completed(future_to_batch):
            batch = future_to_batch[future]
            processed_batches += 1
            
            try:
                batch_pairs = future.result()
                
                # Thread-safe: Add pairs to global list
                with duplicate_pairs_lock:
                    duplicate_pairs.extend(batch_pairs)
                
                # Update progress
                if progress_bar:
                    progress_bar.update(1)
                    if processed_batches % 10 == 0:
                        tracker.update_stats() if tracker else None
                elif processed_batches % 50 == 0:
                    print(f"   📊 Processed {processed_batches}/{total_batches} batches...")
            
            except Exception as e:
                print(f"⚠️  Error processing batch: {e}")
        
        if progress_bar:
            progress_bar.close()
    
    # Remove duplicate pairs (same pair with different order)
    duplicate_pairs = list(set(duplicate_pairs))
    print(f"   ✅ Found {len(duplicate_pairs)} duplicate pairs (including cross-chunk)")
    
    # ============================================================
    # PASS 2: Group into clusters and select originals
    # ============================================================
    print(f"\n🔗 PASS 2: Grouping into clusters and selecting originals...")
    
    # Separate within-chunk and cross-chunk pairs
    all_job_ids = set(video_info.keys())
    
    # DEBUG: Check first few pairs to understand the issue
    if duplicate_pairs:
        sample_pairs = duplicate_pairs[:5]
        print(f"   🔍 DEBUG: Sample duplicate pairs (first 5):")
        for job_id1, job_id2, sim in sample_pairs:
            in_chunk1 = job_id1 in all_job_ids
            in_chunk2 = job_id2 in all_job_ids
            print(f"      Pair: {job_id1} (in_chunk={in_chunk1}) <-> {job_id2} (in_chunk={in_chunk2}), sim={sim:.4f}")
        print(f"   🔍 DEBUG: Total job_ids in chunk: {len(all_job_ids)}")
        print(f"   🔍 DEBUG: Sample job_ids: {list(all_job_ids)[:5]}")
    
    # CRITICAL FIX: Properly separate within-chunk and cross-chunk pairs
    chunk_duplicate_pairs = []
    cross_chunk_pairs_list = []
    
    for job_id1, job_id2, similarity in duplicate_pairs:
        in_chunk1 = job_id1 in all_job_ids
        in_chunk2 = job_id2 in all_job_ids
        
        if in_chunk1 and in_chunk2:
            # Both in chunk: within-chunk pair
            chunk_duplicate_pairs.append((job_id1, job_id2, similarity))
        elif in_chunk1 or in_chunk2:
            # One in chunk, one outside: cross-chunk pair
            cross_chunk_pairs_list.append((job_id1, job_id2, similarity))
        # If neither in chunk, skip (both outside current chunk)
    
    cross_chunk_pairs_count = len(cross_chunk_pairs_list)
    if cross_chunk_pairs_count > 0 or len(chunk_duplicate_pairs) > 0:
        print(f"   📊 Found: {len(chunk_duplicate_pairs)} pairs within chunk, {cross_chunk_pairs_count} cross-chunk pairs")
    
    # ============================================================
    # IMPROVEMENT 1: Handle cross-chunk pairs immediately
    # ============================================================
    # Helper function to extract numeric part from job_id (e.g., "url_14814" -> 14814)
    def extract_job_id_number(job_id: str) -> int:
        """Extract numeric part from job_id like 'url_14814' -> 14814"""
        try:
            # Extract number after last underscore
            parts = job_id.split('_')
            if len(parts) > 1:
                return int(parts[-1])
            # If no underscore, try to extract number from end
            import re
            match = re.search(r'\d+$', job_id)
            if match:
                return int(match.group())
            # Fallback: use string comparison
            return 0
        except:
            # Fallback: use string comparison
            return 0
    
    # Track videos that should be marked as duplicates due to cross-chunk pairs
    # CRITICAL FIX: If a video in current chunk has a duplicate with ANY video outside chunk,
    # we need to check if the outside video has a smaller job_id. If yes, mark current video as duplicate.
    # If the outside video has a larger job_id, we still mark current video as duplicate (because original is outside).
    cross_chunk_duplicates: Set[str] = set()
    cross_chunk_originals: Dict[str, Tuple[str, float]] = {}  # job_id -> (original_job_id, similarity)
    
    # DEBUG: Track statistics
    pairs_with_duplicate_in_chunk = 0
    pairs_with_original_in_chunk = 0
    
    for job_id1, job_id2, similarity in cross_chunk_pairs_list:
        # Determine which is original (smaller numeric job_id)
        # Extract numbers for proper comparison
        num1 = extract_job_id_number(job_id1)
        num2 = extract_job_id_number(job_id2)
        
        if num1 > 0 and num2 > 0:
            # Both have valid numbers, compare numerically
            if num1 < num2:
                original_id, duplicate_id = job_id1, job_id2
            else:
                original_id, duplicate_id = job_id2, job_id1
        else:
            # Fallback to string comparison
            if job_id1 < job_id2:
                original_id, duplicate_id = job_id1, job_id2
            else:
                original_id, duplicate_id = job_id2, job_id1
        
        # Track statistics
        if duplicate_id in all_job_ids:
            pairs_with_duplicate_in_chunk += 1
        if original_id in all_job_ids:
            pairs_with_original_in_chunk += 1
        
        # CRITICAL FIX: If duplicate is in current chunk and original is outside, mark as duplicate
        # Also: If original is in current chunk but duplicate is outside, we don't mark anything
        # (because we want to keep the original in current chunk)
        if duplicate_id in all_job_ids and original_id not in all_job_ids:
            cross_chunk_duplicates.add(duplicate_id)
            # Store the best similarity for this duplicate
            if duplicate_id not in cross_chunk_originals or similarity > cross_chunk_originals[duplicate_id][1]:
                cross_chunk_originals[duplicate_id] = (original_id, similarity)
        # NEW: Also handle case where original is in chunk but duplicate is outside
        # In this case, we keep the original in chunk (no action needed)
    
    # DEBUG: Print statistics
    print(f"   🔍 DEBUG: Cross-chunk pairs analysis:")
    print(f"      - Total cross-chunk pairs: {len(cross_chunk_pairs_list)}")
    print(f"      - Pairs with duplicate in chunk: {pairs_with_duplicate_in_chunk}")
    print(f"      - Pairs with original in chunk: {pairs_with_original_in_chunk}")
    print(f"      - Unique duplicates marked: {len(cross_chunk_duplicates)}")
    
    if cross_chunk_duplicates:
        print(f"   🔗 Marked {len(cross_chunk_duplicates)} videos as duplicates (original in other chunk)")
        # Show sample cross-chunk duplicates
        sample_dups = list(cross_chunk_duplicates)[:5]
        for dup_id in sample_dups:
            orig_id, sim = cross_chunk_originals[dup_id]
            print(f"      Example: {dup_id} -> original {orig_id} (sim={sim:.4f})")
    
    # ============================================================
    # Build graph for within-chunk clustering
    # ============================================================
    print(f"   🔧 Building graph from {len(chunk_duplicate_pairs)} pairs...")
    # Build graph: job_id -> set of connected job_ids
    graph: Dict[str, Set[str]] = {}
    
    # Initialize graph (exclude cross-chunk duplicates from clustering)
    for job_id in all_job_ids:
        if job_id not in cross_chunk_duplicates:
            graph[job_id] = set()
    
    # Add edges from duplicate pairs (only within chunk, exclude cross-chunk duplicates)
    edges_added = 0
    for job_id1, job_id2, similarity in chunk_duplicate_pairs:
        if job_id1 in graph and job_id2 in graph:
            graph[job_id1].add(job_id2)
            graph[job_id2].add(job_id1)
            edges_added += 1
    print(f"   ✅ Built graph with {len(graph)} nodes and ~{edges_added} edges")
    
    # Find connected components (clusters) using iterative DFS (avoid recursion limit)
    visited: Set[str] = set()
    clusters: List[Set[str]] = []
    
    def dfs_iterative(start_node: str) -> Set[str]:
        """Iterative DFS to find connected component (avoid recursion limit)"""
        if start_node in visited:
            return set()
        
        cluster = set()
        stack = [start_node]
        
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            
            visited.add(node)
            cluster.add(node)
            
            # Add all neighbors to stack
            for neighbor in graph.get(node, set()):
                if neighbor not in visited:
                    stack.append(neighbor)
        
        return cluster
    
    # Find all clusters
    print(f"   🔍 Finding connected components (clusters)...")
    for job_id in all_job_ids:
        if job_id not in visited and job_id not in cross_chunk_duplicates:
            cluster = dfs_iterative(job_id)
            if cluster:
                clusters.append(cluster)
                if len(clusters) % 10 == 0:
                    print(f"      Found {len(clusters)} clusters so far...")
    
    print(f"   ✅ Found {len(clusters)} clusters (excluding cross-chunk duplicates)")
    
    # ============================================================
    # IMPROVEMENT 2: Select original based on smallest job_id in entire collection
    # ============================================================
    originals: Set[str] = set()
    duplicates: List[Dict] = []
    unique_videos: List[Dict] = []
    
    # Track all videos that are in clusters
    videos_in_clusters: Set[str] = set()
    for cluster in clusters:
        videos_in_clusters.update(cluster)
    
    # OPTIMIZATION: Build similarity lookup dictionary for O(1) lookup
    # Instead of searching through 8M pairs, use a dict: (job_id1, job_id2) -> similarity
    print(f"   🔧 Building similarity lookup dictionary...")
    similarity_lookup: Dict[Tuple[str, str], float] = {}
    for job_id1, job_id2, sim in chunk_duplicate_pairs:
        # Normalize: always smaller job_id first for consistent lookup
        key = tuple(sorted([job_id1, job_id2]))
        # Keep highest similarity if multiple pairs exist
        if key not in similarity_lookup or sim > similarity_lookup[key]:
            similarity_lookup[key] = sim
    print(f"   ✅ Built lookup dict with {len(similarity_lookup)} entries")
    
    # Process clusters with progress tracking
    print(f"   🔄 Processing {len(clusters)} clusters...")
    for cluster_idx, cluster in enumerate(clusters):
        if (cluster_idx + 1) % 10 == 0 or cluster_idx == 0:
            print(f"      Processing cluster {cluster_idx + 1}/{len(clusters)} (size: {len(cluster)})")
        
        # CRITICAL FIX: Exclude cross-chunk duplicates from cluster processing
        # If a video is already marked as cross-chunk duplicate, skip it
        cluster_filtered = [jid for jid in cluster if jid not in cross_chunk_duplicates]
        
        if not cluster_filtered:
            # All videos in cluster are cross-chunk duplicates, skip
            continue
        
        # IMPROVEMENT: Sort by job_id to get smallest (will be consistent across chunks)
        # Sort by numeric job_id (not string) to find true original
        sorted_cluster = sorted(cluster_filtered, key=lambda jid: extract_job_id_number(jid))
        original_job_id = sorted_cluster[0]  # Smallest numeric job_id = original
        
        # CRITICAL: Only add to originals if not already a cross-chunk duplicate
        if original_job_id not in cross_chunk_duplicates:
            originals.add(original_job_id)
            
            # Add original to unique videos
            unique_videos.append({
                "url": video_info[original_job_id]["url"],
                "job_id": original_job_id
            })
        
        # Add all others as duplicates (both within-cluster and cross-chunk duplicates in cluster)
        for duplicate_job_id in sorted_cluster[1:]:
            # Skip if already marked as cross-chunk duplicate (will be added separately)
            if duplicate_job_id in cross_chunk_duplicates:
                continue
                
            # OPTIMIZATION: Use lookup dict instead of searching through all pairs
            key = tuple(sorted([duplicate_job_id, original_job_id]))
            max_sim = similarity_lookup.get(key, 0.0)
            
            duplicates.append({
                "duplicate_url": video_info[duplicate_job_id]["url"],
                "duplicate_job_id": duplicate_job_id,
                "original_job_id": original_job_id,
                "original_url": video_info[original_job_id]["url"],
                "similarity": f"{max_sim:.6f}" if max_sim > 0 else "0.000000"
            })
    
    # BUG FIX: Add videos that have no duplicates (not in any cluster and not cross-chunk duplicate)
    # CRITICAL: Only add standalone videos that are NOT cross-chunk duplicates
    standalone_videos = all_job_ids - videos_in_clusters - cross_chunk_duplicates
    if standalone_videos:
        print(f"   ℹ️  Found {len(standalone_videos)} standalone videos (no duplicates)")
        for job_id in sorted(standalone_videos, key=lambda jid: extract_job_id_number(jid)):
            # Double-check: not a cross-chunk duplicate
            if job_id not in cross_chunk_duplicates:
                unique_videos.append({
                    "url": video_info[job_id]["url"],
                    "job_id": job_id
                })
                originals.add(job_id)
    else:
        print(f"   ℹ️  No standalone videos (all {len(all_job_ids)} videos have duplicates)")
    
    # Add cross-chunk duplicates to duplicates list
    # Case 1: Duplicate in chunk, original outside chunk
    for duplicate_job_id in cross_chunk_duplicates:
        original_job_id, similarity = cross_chunk_originals[duplicate_job_id]
        duplicates.append({
            "duplicate_url": video_info[duplicate_job_id]["url"],
            "duplicate_job_id": duplicate_job_id,
            "original_job_id": original_job_id,
            "original_url": f"[CROSS-CHUNK: {original_job_id}]",  # Mark as cross-chunk
            "similarity": f"{similarity:.6f}"
        })
    
    # Case 2: Original in chunk, duplicate outside chunk (for reporting)
    # Track these for reporting purposes (original stays in chunk, but we report the duplicate outside)
    cross_chunk_originals_in_chunk: Dict[str, List[Tuple[str, float]]] = {}  # original_job_id -> [(duplicate_job_id, similarity), ...]
    
    for job_id1, job_id2, similarity in cross_chunk_pairs_list:
        num1 = extract_job_id_number(job_id1)
        num2 = extract_job_id_number(job_id2)
        
        if num1 > 0 and num2 > 0:
            if num1 < num2:
                original_id, duplicate_id = job_id1, job_id2
            else:
                original_id, duplicate_id = job_id2, job_id1
        else:
            if job_id1 < job_id2:
                original_id, duplicate_id = job_id1, job_id2
            else:
                original_id, duplicate_id = job_id2, job_id1
        
        # If original is in chunk and duplicate is outside, track for reporting
        if original_id in all_job_ids and duplicate_id not in all_job_ids:
            if original_id not in cross_chunk_originals_in_chunk:
                cross_chunk_originals_in_chunk[original_id] = []
            cross_chunk_originals_in_chunk[original_id].append((duplicate_id, similarity))
    
    # Add to duplicates list for reporting (but original stays in FINAL_RESULT)
    for original_id, dup_list in cross_chunk_originals_in_chunk.items():
        # Sort by similarity (highest first) and take top duplicates to report
        dup_list_sorted = sorted(dup_list, key=lambda x: x[1], reverse=True)
        # Report top 5 duplicates for each original (to avoid too many entries)
        for duplicate_id, similarity in dup_list_sorted[:5]:
            duplicates.append({
                "duplicate_url": f"[CROSS-CHUNK: {duplicate_id}]",  # Mark as outside chunk
                "duplicate_job_id": duplicate_id,
                "original_job_id": original_id,
                "original_url": video_info[original_id]["url"],
                "similarity": f"{similarity:.6f}"
            })
    
    within_chunk_duplicates = len(duplicates) - len(cross_chunk_duplicates)
    print(f"   ✅ Selected {len(originals)} originals, {within_chunk_duplicates} within-chunk duplicates, {len(cross_chunk_duplicates)} cross-chunk duplicates")
    
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
    file_exists = os.path.exists(unique_csv)
    write_mode = "a" if (append_mode and file_exists) else "w"
    
    with open(unique_csv, write_mode, encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # Only write header if file is new or not in append mode
        if not (append_mode and file_exists):
            writer.writerow(["decoded_url"])
        for video in final_unique_videos:
            writer.writerow([video["url"]])
    
    if append_mode and file_exists:
        print(f"   ✅ Appended {len(final_unique_videos)} unique videos to {unique_csv}")
    else:
        print(f"   ✅ Wrote {len(final_unique_videos)} unique videos to {unique_csv}")
    
    # Duplicate report
    os.makedirs(os.path.dirname(report_csv) or ".", exist_ok=True)
    file_exists = os.path.exists(report_csv)
    write_mode = "a" if (append_mode and file_exists) else "w"
    
    with open(report_csv, write_mode, encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # Only write header if file is new or not in append mode
        if not (append_mode and file_exists):
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
    
    if append_mode and file_exists:
        print(f"   ✅ Appended {len(duplicates)} duplicates to {report_csv}")
    else:
        print(f"   ✅ Wrote {len(duplicates)} duplicates to {report_csv}")
    
    # Write invalid URLs report (if auto-clean enabled)
    if auto_clean and invalid_urls:
        if invalid_csv is None:
            invalid_csv = "invalid_urls.csv"
        
        os.makedirs(os.path.dirname(invalid_csv) or ".", exist_ok=True)
        file_exists = os.path.exists(invalid_csv)
        write_mode = "a" if (append_mode and file_exists) else "w"
        
        with open(invalid_csv, write_mode, encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            # Only write header if file is new or not in append mode
            if not (append_mode and file_exists):
                writer.writerow(['invalid_url', 'job_id', 'reason'])
            for item in invalid_urls:
                writer.writerow([item['url'], item['job_id'], item['reason']])
        
        if append_mode and file_exists:
            print(f"   📄 Appended {len(invalid_urls)} invalid URLs to {invalid_csv}")
        else:
            print(f"   📄 Wrote {len(invalid_urls)} invalid URLs to {invalid_csv}")
    
    print(f"✅ Results written!")
    print(f"   Unique videos: {len(final_unique_videos)}")
    print(f"   Duplicates: {len(duplicates)}")
    
    # DEBUG: Show breakdown
    if chunk_start is not None and chunk_end is not None:
        print(f"\n📊 CHUNK MODE SUMMARY:")
        print(f"   Input chunk size: {chunk_end - chunk_start} videos")
        print(f"   Videos actually loaded: {len(all_data)} videos")
        print(f"   Unique videos (originals): {len(final_unique_videos)}")
        print(f"   Duplicates found: {len(duplicates)}")
        print(f"   - Within-chunk duplicates: {within_chunk_duplicates}")
        print(f"   - Cross-chunk duplicates: {len(cross_chunk_duplicates)}")
        print(f"   - Videos in clusters: {len(videos_in_clusters)}")
        print(f"   - Standalone videos: {len(standalone_videos) if 'standalone_videos' in locals() else 0}")
        if auto_clean:
            print(f"   - Invalid URLs removed: {len(invalid_urls)}")
        
        # Calculate missing videos
        accounted_for = len(final_unique_videos) + len(duplicates)
        if auto_clean:
            accounted_for += len(invalid_urls)
        missing = len(all_data) - accounted_for
        
        print(f"\n   📊 ACCOUNTING:")
        print(f"   - Total loaded: {len(all_data)}")
        print(f"   - Unique (kept): {len(final_unique_videos)}")
        print(f"   - Duplicates (reported): {len(duplicates)}")
        if auto_clean:
            print(f"   - Invalid (removed): {len(invalid_urls)}")
        print(f"   - Accounted for: {accounted_for}")
        if missing > 0:
            print(f"   - ⚠️  MISSING/UNACCOUNTED: {missing} videos ({missing/len(all_data)*100:.1f}%)")
            print(f"   💡 Possible reasons:")
            print(f"      1. Videos detected as duplicates but not written to file")
            print(f"      2. Videos with no duplicates found (threshold too high or TOP_K too small)")
            print(f"      3. Logic error in cluster/duplicate detection")
        elif missing < 0:
            print(f"   - ⚠️  OVER-COUNTED: {abs(missing)} videos (should not happen!)")
        
        if chunk_end > chunk_start:
            dedup_rate = (1 - len(final_unique_videos) / len(all_data)) * 100 if len(all_data) > 0 else 0
            print(f"   - Deduplication rate: {dedup_rate:.1f}% (based on {len(all_data)} loaded videos)")
        # Note: cross_chunk_duplicates is defined in the cross-chunk processing section above
    
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
        "--batch_size",
        type=int,
        default=10,
        help="Batch size for parallel search (default: 10, max: 10 per Zilliz limit). Higher = faster but more memory"
    )
    parser.add_argument(
        "--num_threads",
        type=int,
        default=4,
        help="Number of parallel threads (default: 4). Higher = faster but more CPU"
    )
    parser.add_argument(
        "--fast_mode",
        action="store_true",
        help="Fast mode: Use optimized search params (lower nprobe/ef) for 2-4x speedup. Slightly less accurate."
    )
    parser.add_argument(
        "--chunk_start",
        type=int,
        default=None,
        help="Start index for chunk processing (only process videos from this index). Use with --chunk_end"
    )
    parser.add_argument(
        "--chunk_end",
        type=int,
        default=None,
        help="End index for chunk processing (only process videos up to this index). Use with --chunk_start"
    )
    parser.add_argument(
        "--append_mode",
        action="store_true",
        help="Append mode: Append results to existing CSV files instead of overwriting. Useful when processing multiple chunks sequentially."
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
    
    if args.batch_size < 1 or args.batch_size > 10:
        print(f"❌ ERROR: Batch size must be 1-10 (Zilliz limit, got {args.batch_size})", file=sys.stderr)
        sys.exit(1)
    
    if args.num_threads < 1:
        print(f"❌ ERROR: Number of threads must be >= 1 (got {args.num_threads})", file=sys.stderr)
        sys.exit(1)
    
    # Validate chunk parameters
    if (args.chunk_start is not None and args.chunk_end is not None):
        if args.chunk_start < 0:
            print(f"❌ ERROR: chunk_start must be >= 0 (got {args.chunk_start})", file=sys.stderr)
            sys.exit(1)
        if args.chunk_end <= args.chunk_start:
            print(f"❌ ERROR: chunk_end must be > chunk_start (got {args.chunk_start}-{args.chunk_end})", file=sys.stderr)
            sys.exit(1)
    elif args.chunk_start is not None or args.chunk_end is not None:
        print(f"⚠️  WARNING: Both --chunk_start and --chunk_end must be specified. Ignoring chunk mode.", file=sys.stderr)
        args.chunk_start = None
        args.chunk_end = None
    
    # Update output filenames if chunk mode
    unique_csv = args.unique_csv
    report_csv = args.report_csv
    invalid_csv = args.invalid_csv
    
    # If append mode, don't add chunk suffix (append to same file)
    if args.chunk_start is not None and args.chunk_end is not None and not args.append_mode:
        # Add chunk suffix to avoid overwriting (only if not in append mode)
        base_unique = os.path.splitext(args.unique_csv)[0]
        base_report = os.path.splitext(args.report_csv)[0]
        base_invalid = os.path.splitext(args.invalid_csv)[0]
        ext_unique = os.path.splitext(args.unique_csv)[1] or ".csv"
        ext_report = os.path.splitext(args.report_csv)[1] or ".csv"
        ext_invalid = os.path.splitext(args.invalid_csv)[1] or ".csv"
        
        unique_csv = f"{base_unique}_chunk_{args.chunk_start}_{args.chunk_end}{ext_unique}"
        report_csv = f"{base_report}_chunk_{args.chunk_start}_{args.chunk_end}{ext_report}"
        invalid_csv = f"{base_invalid}_chunk_{args.chunk_start}_{args.chunk_end}{ext_invalid}"
    
    # Print config
    print_config()
    print(f"\n🎯 AGGREGATED MODE:")
    print(f"   → 1 vector per video (instead of 3)")
    print(f"   → 3× faster search")
    print(f"   → Simpler query logic")
    print(f"\n⚡ OPTIMIZATION:")
    print(f"   → Batch size: {args.batch_size} videos/batch")
    print(f"   → Parallel threads: {args.num_threads}")
    print(f"   → Expected speedup: {args.batch_size * args.num_threads}×")
    
    if args.chunk_start is not None and args.chunk_end is not None:
        print(f"\n📦 CHUNK MODE:")
        print(f"   → Processing videos {args.chunk_start} to {args.chunk_end-1}")
        print(f"   → Output files will have suffix: _chunk_{args.chunk_start}_{args.chunk_end}")
    
    if args.config_only:
        return
    
    try:
        unique_count, dup_count = search_duplicates_aggregated(
            args.collection,
            args.cosine_thresh,
            unique_csv,
            report_csv,
            args.top_k,
            args.auto_clean,
            invalid_csv,
            enable_tracking=True,  # Always enable tracking
            batch_size=args.batch_size,
            num_threads=args.num_threads,
            chunk_start=args.chunk_start,
            chunk_end=args.chunk_end,
            fast_mode=args.fast_mode,
            append_mode=args.append_mode
        )
        
        print(f"\n🎉 Search complete!")
        print(f"   ✅ Unique videos: {unique_count}")
        print(f"   ❌ Duplicates found: {dup_count}")
        print(f"   → Unique URLs: {unique_csv}")
        print(f"   → Duplicates report: {report_csv}")
        
        if args.auto_clean:
            print(f"\n🧼 Auto-clean was enabled:")
            print(f"   → Invalid URLs removed (PNG, images, broken URLs)")
            print(f"   → Check {invalid_csv} for details")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

