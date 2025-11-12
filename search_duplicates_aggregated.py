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


def normalize_job_id_format(job_id: str) -> str:
    """
    Normalize job_id format to ensure consistency
    Handles: url_0000, url_10000, url_010000, etc.
    Returns: url_XXXXX (normalized format with leading zeros if needed)
    """
    if not job_id:
        return ""
    
    # Extract numeric part
    parts = job_id.split('_')
    if len(parts) < 2:
        return job_id  # Return as-is if no underscore
    
    prefix = '_'.join(parts[:-1])  # Everything except last part
    numeric_part = parts[-1]
    
    try:
        # Parse number and reformat with 4 digits (matching upload script)
        num = int(numeric_part)
        return f"{prefix}_{num:04d}"
    except ValueError:
        # Not a number, return as-is
        return job_id


def extract_job_id_number(job_id: str) -> int:
    """
    Extract numeric part from job_id like 'url_14814' -> 14814
    CRITICAL FIX: Normalize format first to handle inconsistencies
    """
    try:
        # Normalize format first
        normalized = normalize_job_id_format(job_id)
        
        # Extract number after last underscore
        parts = normalized.split('_')
        if len(parts) > 1:
            return int(parts[-1])
        # If no underscore, try to extract number from end
        match = re.search(r'\d+$', normalized)
        if match:
            return int(match.group())
        # Fallback: use string comparison
        return 0
    except Exception as e:
        # Log error for debugging but don't crash
        # Fallback: use string comparison
        return 0


def extract_itag_from_url(url: str) -> int:
    """
    Extract itag from Google CDN URL
    Returns itag number or 0 if not found
    
    Higher itag generally means higher resolution:
    - 37, 38, 39: 1080p/720p
    - 22: 720p
    - 18: 360p
    - 347, 348: 4K/2160p
    - 342, 343: 1080p
    - 346: 720p
    """
    if not url:
        return 0
    
    url = url.strip().strip('"').strip("'")
    
    # Match pattern: /itag/XXX/ in videoplayback URL
    match = re.search(r'/itag/(\d+)/', url, re.IGNORECASE)
    if match:
        try:
            return int(match.group(1))
        except:
            return 0
    
    return 0


def extract_resolution_from_url(url: str) -> tuple[int, int]:
    """
    Extract video resolution (width, height) from URL
    Returns (width, height) or (0, 0) if not found
    
    Tries multiple methods:
    1. Extract from itag (Google CDN)
    2. Extract from URL pattern (e.g., 1920x1080, 1080p, play_1080p.mp4)
    3. Extract from filename patterns
    """
    if not url:
        return (0, 0)
    
    url = url.strip().strip('"').strip("'")
    url_lower = url.lower()
    
    # Method 1: Extract from itag (Google CDN)
    itag = extract_itag_from_url(url)
    if itag > 0:
        # Map itag to resolution (common YouTube/Google CDN itags)
        itag_to_resolution = {
            # 4K
            348: (3840, 2160),  # 4K/2160p
            347: (3840, 2160),  # 4K/2160p
            # 1080p
            37: (1920, 1080),   # 1080p
            38: (1920, 1080),   # 1080p
            343: (1920, 1080),  # 1080p
            342: (1920, 1080),  # 1080p
            # 720p
            22: (1280, 720),    # 720p
            39: (1280, 720),    # 720p
            346: (1280, 720),   # 720p
            # 480p
            35: (854, 480),     # 480p
            # 360p
            18: (640, 360),     # 360p
        }
        if itag in itag_to_resolution:
            return itag_to_resolution[itag]
    
    # Method 2: Extract from URL pattern (e.g., 1920x1080, 1920_1080)
    match = re.search(r'(\d+)[x_](\d+)', url, re.IGNORECASE)
    if match:
        try:
            width = int(match.group(1))
            height = int(match.group(2))
            # Sanity check: reasonable video dimensions
            if 100 <= width <= 7680 and 100 <= height <= 4320:
                return (width, height)
        except:
            pass
    
    # Method 3: Extract from resolution indicators (1080p, 720p, 4k, etc.)
    # Check for patterns like "1080p", "720p", "4k", "2160p"
    resolution_patterns = {
        r'2160p|4k|3840x2160': (3840, 2160),
        r'1440p|2560x1440': (2560, 1440),
        r'1080p|1920x1080': (1920, 1080),
        r'720p|1280x720': (1280, 720),
        r'480p|854x480': (854, 480),
        r'360p|640x360': (640, 360),
        r'240p|426x240': (426, 240),
    }
    
    for pattern, (w, h) in resolution_patterns.items():
        if re.search(pattern, url_lower):
            return (w, h)
    
    # Method 4: Check filename patterns (e.g., play_1080p.mp4)
    filename_patterns = {
        r'play_1080p|1080p\.mp4': (1920, 1080),
        r'play_720p|720p\.mp4': (1280, 720),
        r'play_480p|480p\.mp4': (854, 480),
        r'play_360p|360p\.mp4': (640, 360),
    }
    
    for pattern, (w, h) in filename_patterns.items():
        if re.search(pattern, url_lower):
            return (w, h)
    
    return (0, 0)


def get_resolution_score(url: str) -> int:
    """
    Get a numeric score representing video resolution quality
    Higher score = better quality
    Returns: score (0 = unknown, higher = better)
    """
    width, height = extract_resolution_from_url(url)
    if width == 0 or height == 0:
        # Fallback to itag if available
        itag = extract_itag_from_url(url)
        return itag  # Use itag as score (higher itag usually = better quality)
    
    # Calculate score: width * height (pixel count)
    # This gives higher score to higher resolution
    return width * height


def extract_video_id_from_url(url: str) -> str:
    """
    Extract video ID from URL (e.g., YouTube, Google CDN, etc.)
    Returns normalized video ID or empty string if not found
    
    NOTE: Only extract for URLs where we're CERTAIN the ID represents the same video content.
    For Google CDN videoplayback, the same video ID with different signatures/expires/itags = same video.
    """
    if not url:
        return ""
    
    # Remove quotes
    url = url.strip().strip('"').strip("'")
    
    # Pattern 1: Google CDN videoplayback (id/XXXXX) - HIGH CONFIDENCE
    # Same video ID = same video content (different signatures/expires/itags are just auth tokens/quality)
    # Example: .../videoplayback/id/131cd7f56e4efe05/...
    # Match pattern: /videoplayback/.../id/HEX_ID/... (must be in videoplayback path)
    match = re.search(r'/videoplayback[^/]*/id/([a-f0-9]{15,})/', url, re.IGNORECASE)
    if match:
        # Only match if it's in videoplayback path (not generic /id/ pattern)
        return f"gcdn_id_{match.group(1).lower()}"
    
    # Pattern 2: YouTube (v=XXXXX) - HIGH CONFIDENCE
    # Same video ID = same video
    match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})', url)
    if match:
        return f"youtube_{match.group(1)}"
    
    # Pattern 3: YouTube short URL (youtu.be/XXXXX) - HIGH CONFIDENCE
    match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
    if match:
        return f"youtube_{match.group(1)}"
    
    # NOTE: We DON'T use generic /video/ pattern because different videos can have similar paths
    # Only use patterns where we're CERTAIN the ID uniquely identifies the video content
    
    return ""


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
    skip_url_dedup: bool = False,
    skip_cross_chunk: bool = False,
    cross_chunk_threshold: float = 0.98,
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
        
        # Initialize variables for chunk mode
        all_data = []  # Will be populated by job_id query or position-based query
        use_job_id_query = False  # Will be set to True if job_id query succeeds
        
        print(f"📦 CHUNK MODE: Will only load videos {start_idx} to {end_idx-1} ({end_idx - start_idx} videos)")
        print(f"   💡 Using job_id range query (more accurate than position-based query)")
        
        # CRITICAL FIX: Query by job_id range instead of position (offset/limit)
        # When uploading with direct_upload_to_zilliz.py --start 24500, job_ids use format :04d
        # Format: url_0000 to url_9999 (4 digits), url_10000 to url_99999 (5 digits, no leading zero)
        # 
        # IMPORTANT: String comparison issue when range spans < 10000 and >= 10000
        # Example: "url_9999" < "url_10000" → False (wrong! because "9" > "1" in string comparison)
        # Solution: If range includes < 10000, we need to handle it specially
        # But if range is all >= 10000, string comparison works correctly
        
        # Use format matching direct_upload_to_zilliz.py (:04d)
        start_job_id_4d = f"url_{start_idx:04d}"  # Format: url_24500 (matches upload script)
        end_job_id_4d = f"url_{end_idx:04d}"       # Format: url_25000
        
        print(f"   📋 Querying job_id range: '{start_job_id_4d}' to '{end_job_id_4d}' (exclusive)")
        print(f"   💡 Note: Using format :04d to match direct_upload_to_zilliz.py")
        
        # Check if range spans the 10000 boundary (could cause string comparison issues)
        if start_idx < 10000 and end_idx > 10000:
            print(f"   ⚠️  WARNING: Range spans 10000 boundary - string comparison may have issues")
            print(f"   💡 Will query and filter results to ensure accuracy")
        
        # Query by job_id range - BUT split into smaller batches to avoid gRPC message size limit
        # gRPC limit: ~4MB per message
        # Each video: 512 dims × 4 bytes = 2KB embedding + URL (can be very long, e.g., Google CDN URLs 500+ chars) + job_id ≈ 5-10KB
        # Observed: 1000 videos = ~26MB, 200 videos = ~5.2MB, 100 videos = ~2.6MB (safer)
        # Using 100 videos/batch - if errors occur, will automatically retry with smaller batches (50, then 25)
        all_data = []
        BATCH_SIZE_VIDEOS = 100  # Query 100 videos at a time to avoid message size limit
        
        try:
            # Split range into smaller batches
            total_videos = end_idx - start_idx
            num_batches = (total_videos + BATCH_SIZE_VIDEOS - 1) // BATCH_SIZE_VIDEOS
            
            print(f"   📦 Splitting query into {num_batches} batches ({BATCH_SIZE_VIDEOS} videos/batch) to avoid message size limit...")
            
            for batch_idx in range(num_batches):
                batch_start_idx = start_idx + batch_idx * BATCH_SIZE_VIDEOS
                batch_end_idx = min(start_idx + (batch_idx + 1) * BATCH_SIZE_VIDEOS, end_idx)
                
                batch_start_job_id = f"url_{batch_start_idx:04d}"
                batch_end_job_id = f"url_{batch_end_idx:04d}"
                
                print(f"   📦 Batch {batch_idx + 1}/{num_batches}: Querying [{batch_start_job_id}, {batch_end_job_id})...", end=" ", flush=True)
                
                try:
                    # Query batch
                    batch_result = collection.query(
                        expr=f'job_id >= "{batch_start_job_id}" and job_id < "{batch_end_job_id}"',
                        output_fields=["job_id", "url", "embedding"]
                    )
                    
                    if batch_result:
                        # Filter by numeric comparison to ensure accuracy
                        def extract_job_num(job_id_str):
                            try:
                                return int(job_id_str.split('_')[1])
                            except:
                                return -1
                        
                        # Filter to ensure all job_ids are in numeric range
                        filtered_batch = [
                            item for item in batch_result
                            if batch_start_idx <= extract_job_num(item["job_id"]) < batch_end_idx
                        ]
                        
                        all_data.extend(filtered_batch)
                        print(f"✅ Got {len(filtered_batch)} videos (Total: {len(all_data)})")
                    else:
                        print(f"⚠️  No videos found in this batch")
                
                except Exception as batch_error:
                     error_str = str(batch_error)
                     if "message larger than max" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                         # Message size too large - retry with progressively smaller batches
                         print(f"⚠️  Message too large, retrying with smaller batches...")
                         
                         # CRITICAL FIX: Retry with exponential backoff and better error handling
                         retry_sizes = [50, 25, 10]
                         batch_success = False
                         max_retries = 3
                         retry_count = 0
                         
                         for retry_size in retry_sizes:
                             if batch_success:
                                 break
                             
                             retry_count += 1
                             print(f"   🔄 Retry {retry_count}/{len(retry_sizes)}: Trying sub-batches of {retry_size} videos...", end=" ", flush=True)
                             sub_batches = []
                             for sub_start in range(batch_start_idx, batch_end_idx, retry_size):
                                 sub_end = min(sub_start + retry_size, batch_end_idx)
                                 sub_batches.append((sub_start, sub_end))
                             
                             sub_success_count = 0
                             sub_errors = []
                             
                             for sub_start, sub_end in sub_batches:
                                 sub_start_job_id = f"url_{sub_start:04d}"
                                 sub_end_job_id = f"url_{sub_end:04d}"
                                 
                                 try:
                                     sub_result = collection.query(
                                         expr=f'job_id >= "{sub_start_job_id}" and job_id < "{sub_end_job_id}"',
                                         output_fields=["job_id", "url", "embedding"]
                                     )
                                     
                                     if sub_result:
                                         def extract_job_num(job_id_str):
                                             try:
                                                 return int(job_id_str.split('_')[1])
                                             except:
                                                 return -1
                                         
                                         filtered_sub = [
                                             item for item in sub_result
                                             if sub_start <= extract_job_num(item["job_id"]) < sub_end
                                         ]
                                         all_data.extend(filtered_sub)
                                         sub_success_count += 1
                                 except Exception as sub_error:
                                     # Collect errors for reporting
                                     sub_errors.append(f"Sub-batch [{sub_start}, {sub_end}): {str(sub_error)[:100]}")
                                     # Continue with next sub-batch
                                     continue
                             
                             if sub_success_count == len(sub_batches):
                                 batch_success = True
                                 print(f"✅ ({sub_success_count}/{len(sub_batches)} sub-batches)")
                             elif sub_success_count > 0:
                                 print(f"⚠️  ({sub_success_count}/{len(sub_batches)} sub-batches succeeded)")
                                 if sub_errors:
                                     print(f"      Errors: {len(sub_errors)} sub-batches failed")
                                     # Log first few errors
                                     for err in sub_errors[:3]:
                                         print(f"         - {err}")
                             else:
                                 print(f"❌ All sub-batches failed")
                                 if sub_errors:
                                     print(f"      Sample errors:")
                                     for err in sub_errors[:3]:
                                         print(f"         - {err}")
                         
                         if batch_success:
                             print(f"   ✅ Retried batch {batch_idx + 1} successfully (Total: {len(all_data)})")
                         elif sub_success_count > 0:
                             print(f"   ⚠️  Batch {batch_idx + 1} partially succeeded ({sub_success_count}/{len(sub_batches)} sub-batches, Total: {len(all_data)})")
                         else:
                             print(f"   ❌ Batch {batch_idx + 1} failed completely after all retries")
                             # Don't continue silently - this is a critical error
                             print(f"   ⚠️  WARNING: Missing data from batch [{batch_start_idx}, {batch_end_idx})")
                     else:
                         # Other errors (not message size related)
                         print(f"❌ Error: {batch_error}")
                         print(f"   ⚠️  Batch [{batch_start_idx}, {batch_end_idx}) failed")
                         # Log full error for debugging
                         import traceback
                         print(f"   Traceback: {traceback.format_exc()[:500]}")
                         # Continue with next batch instead of failing completely
                         continue
            
            # all_data already contains all filtered videos from batches
            if all_data:
                print(f"   ✅ Found {len(all_data)} videos with job_id in range [{start_job_id_4d}, {end_job_id_4d})")
                
                # Verify job_ids are in expected range
                def extract_job_num(job_id_str):
                    try:
                        return int(job_id_str.split('_')[1])
                    except:
                        return -1
                
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
                print(f"   ❌ ERROR: No videos found with job_id in range [{start_job_id_4d}, {end_job_id_4d})")
                print(f"   💡 TIP: Check if you uploaded with the correct --start parameter")
                print(f"   💡 TIP: Verify job_id format in Zilliz matches 'url_XXXX' (4 digits for < 10000, no leading zero for >= 10000)")
                sys.exit(1)
        except Exception as e:
            print(f"   ❌ ERROR querying by job_id range: {e}")
            print(f"   💡 Falling back to position-based query (may be inaccurate)...")
            # Fallback to old method (but warn user)
            all_data = []  # Will be populated by old method
            all_ids = []
            target_count = end_idx - start_idx
            # Continue with old position-based query method below
            use_job_id_query = False
        else:
            # Successfully queried by job_id, skip position-based query
            use_job_id_query = True
            print(f"   ✅ Successfully loaded {len(all_data)} videos using job_id range query")
        
        # Only use position-based query if job_id query failed
        if not use_job_id_query:
            target_count = end_idx - start_idx  # Define target_count for fallback method
            all_ids = []  # Initialize all_ids for fallback method
            
            # If start_idx < 16384, we can use offset/limit directly
            # But if end_idx > 16384, we need to switch to ID range query after hitting the limit
            if start_idx < 16384:
                id_offset = start_idx
                id_batch_size = 10000  # Safe: offset + limit = 10000 < 16384
                
                while id_offset < end_idx and len(all_ids) < target_count:
                    remaining = end_idx - id_offset
                    current_limit = min(id_batch_size, remaining)
                    
                    # Ensure offset + limit <= 16384
                    if id_offset + current_limit > 16384:
                        current_limit = 16384 - id_offset
                        if current_limit <= 0:
                            # We've hit the 16384 limit, switch to ID range query
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
                
                # If we haven't fetched all IDs yet and hit the 16384 limit, switch to ID range query
                if id_offset < end_idx and len(all_ids) < target_count:
                    print(f"   ⚠️  Hit 16384 offset limit at index {id_offset}")
                    print(f"   ❌ ERROR: Cannot fetch beyond 16384 with offset/limit!")
                    print(f"   💡 SOLUTION: Split chunk into smaller ranges:")
                    print(f"      - Range 1: {start_idx}-16384")
                    print(f"      - Range 2: 16384-{end_idx}")
                    print(f"   ⚠️  Fetching all IDs from 0 to {end_idx-1} to ensure accuracy...")
                    
                    # CRITICAL FIX: Must fetch ALL from 0 to end_idx, then slice
                    # ID range query doesn't maintain insertion order!
                    temp_all_ids = []
                    fetch_offset = 0
                    
                    # CRITICAL: Can only fetch up to index 16384 with offset/limit
                    # If end_idx > 16384, we cannot fetch accurately!
                    # Solution: Adjust end_idx to 16384 and warn user
                    if end_idx > 16384:
                        print(f"   ❌ ERROR: end_idx ({end_idx}) > 16384 - cannot fetch beyond 16384 with offset/limit!")
                        print(f"   💡 SOLUTION: Split into smaller chunks:")
                        print(f"      - Chunk 1: {start_idx}-16384")
                        print(f"      - Chunk 2: 16384-{end_idx}")
                        print(f"   ⚠️  Will only process range [{start_idx}, 16384) for now")
                        end_idx = 16384
                        target_count = end_idx - start_idx
                    
                    max_fetchable = min(end_idx, 16384)
                    
                    while fetch_offset < max_fetchable:
                        # Safe limit: offset + limit <= 16384
                        remaining = max_fetchable - fetch_offset
                        current_limit = min(16384 - fetch_offset, remaining)
                        
                        if current_limit <= 0:
                            break
                        
                        id_batch = collection.query(
                            expr="id >= 0",
                            output_fields=["id"],
                            limit=current_limit,
                            offset=fetch_offset
                        )
                        if not id_batch:
                            break
                        
                        batch_ids = [item["id"] for item in id_batch]
                        temp_all_ids.extend(batch_ids)
                        fetch_offset += len(batch_ids)
                        
                        if len(id_batch) < current_limit:
                            break
                    
                    # Slice to get target range [start_idx, end_idx)
                    # But we can only fetch up to max_fetchable (16384)
                    actual_end = min(end_idx, len(temp_all_ids))
                    
                    if len(temp_all_ids) > start_idx:
                        all_ids = temp_all_ids[start_idx:actual_end]
                        if actual_end < end_idx:
                            print(f"   ⚠️  WARNING: Could only fetch up to index {len(temp_all_ids)-1} (requested {end_idx-1})")
                            print(f"   ⚠️  Extracted {len(all_ids):,} IDs in range [{start_idx}, {actual_end}) instead of [{start_idx}, {end_idx})")
                        else:
                            print(f"   ✅ Fetched {len(temp_all_ids):,} IDs, extracted {len(all_ids):,} in range [{start_idx}, {end_idx})")
                    else:
                        all_ids = []
                        print(f"   ❌ ERROR: Could not fetch enough IDs (got {len(temp_all_ids)}, need at least {start_idx})")
                    
                    if 'temp_all_ids' in locals():
                        del temp_all_ids
            else:
                # start_idx >= 16384: Cannot use offset/limit, must use ID range query
                # SOLUTION: Fetch ALL IDs from 0 to end_idx, then slice to get [start_idx, end_idx)
                # This ensures accuracy regardless of ID ordering
                print(f"   ⚠️  start_idx ({start_idx}) >= 16384, using ID range query...")
                print(f"   📊 Fetching all IDs from 0 to {end_idx-1}, will extract range [{start_idx}, {end_idx})")
                print(f"   ⚠️  NOTE: Must fetch from beginning to maintain index order accuracy")
                
                # Strategy: Fetch all IDs sequentially from beginning to end_idx
                # Then slice to get only [start_idx, end_idx) range
                # This is the ONLY reliable way when IDs may not be sequential
                temp_all_ids = []  # Store all IDs from 0 to end_idx
                max_id_so_far = -1
                id_query_batch_size = 16384  # Max limit per query (renamed to avoid conflict with function parameter)
                ids_fetched = 0  # Count of IDs fetched (by position)
                
                # Fetch IDs until we have enough to cover end_idx
                while ids_fetched < end_idx:
                    # Query next batch of IDs (ordered by ID value, not insertion order)
                    remaining_batch = collection.query(
                        expr=f"id > {max_id_so_far}",
                        output_fields=["id"],
                        limit=id_query_batch_size
                    )
                    if not remaining_batch:
                        break
                    
                    batch_ids = [item["id"] for item in remaining_batch]
                    batch_size_actual = len(batch_ids)
                    
                    # Add all IDs to temp list (we'll slice later)
                    temp_all_ids.extend(batch_ids)
                    ids_fetched += batch_size_actual
                    max_id_so_far = max(batch_ids)
                    
                    # Early stop: if we've fetched enough to cover end_idx
                    if ids_fetched >= end_idx:
                        break
                    
                    # If batch is smaller than limit, we've reached the end
                    if batch_size_actual < id_query_batch_size:
                        break
                    
                    # Progress update for large fetches
                    if ids_fetched % 50000 == 0:
                        print(f"      Fetched {ids_fetched:,} IDs so far...")
                
                # Now slice to get only the IDs in our target range [start_idx, end_idx)
                if len(temp_all_ids) >= end_idx:
                    all_ids = temp_all_ids[start_idx:end_idx]
                    print(f"   ✅ Fetched {len(temp_all_ids):,} total IDs, extracted {len(all_ids):,} in range [{start_idx}, {end_idx})")
                elif len(temp_all_ids) > start_idx:
                    # We have some IDs but not enough
                    all_ids = temp_all_ids[start_idx:]
                    print(f"   ⚠️  WARNING: Only fetched {len(temp_all_ids):,} IDs (expected {end_idx}), extracted {len(all_ids):,} from index {start_idx}")
                else:
                    # We don't have enough IDs even to reach start_idx
                    all_ids = []
                    print(f"   ❌ ERROR: Only fetched {len(temp_all_ids):,} IDs, but need at least {start_idx}")
                
                # Clear temp list to free memory
                del temp_all_ids
            
            print(f"   ✅ Found {len(all_ids)} IDs in chunk range (expected: {target_count})")
            if len(all_ids) != target_count:
                print(f"   ⚠️  WARNING: Expected {target_count} IDs but got {len(all_ids)}")
            
            # Now fetch embeddings for position-based query
            print(f"   Step 2: Fetching embeddings in batches...")
            print(f"   📊 Will fetch embeddings for {len(all_ids)} videos in batches of {MAX_QUERY_LIMIT}...")
            
            # Query embeddings by ID ranges
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
            
            print(f"✅ Loaded {len(all_data)} videos total (position-based query)")
    else:
        # Load all IDs (full mode)
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
    
    # Check if all_data already has data (from job_id query in chunk mode)
    # If not, fetch embeddings using all_ids
    # Use try-except to safely check if all_data exists and has data
    try:
        has_all_data = len(all_data) > 0
    except NameError:
        has_all_data = False
    
    if not has_all_data:
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
    else:
        # all_data already populated from job_id query, skip fetching embeddings
        print(f"   ✅ Using {len(all_data)} videos from job_id query (embeddings already loaded)")
    
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
    initial_video_count = len(all_data)
    print(f"📊 Initial video count: {initial_video_count}")
    
    # CRITICAL FIX: Memory optimization - build video_info efficiently
    # Estimate memory usage: each video ~5-10KB (embedding 2KB + URL 3-8KB)
    estimated_memory_mb = (initial_video_count * 7) / 1024  # Average 7KB per video
    print(f"   💾 Estimated memory usage: ~{estimated_memory_mb:.1f} MB for video data")
    
    if estimated_memory_mb > 1000:  # > 1GB
        print(f"   ⚠️  WARNING: High memory usage expected ({estimated_memory_mb:.1f} MB)")
        print(f"   💡 Consider using smaller chunks or --skip_url_dedup to reduce memory")
    
    for video in all_data:
        video_info[video["job_id"]] = {
            "url": video["url"],
            "embedding": video["embedding"]
        }
    
    # CRITICAL FIX: Clear all_data after building video_info to free memory
    # We only need video_info going forward, not the full all_data list
    # Note: Keep all_data for now as it's used in search_batch, but we can optimize later
    # For now, just add a comment that this could be optimized
    # TODO: Refactor to avoid keeping all_data in memory after building video_info
    
    # ============================================================
    # CRITICAL FIX: Pre-filter by video ID to remove URL duplicates
    # ============================================================
    if skip_url_dedup:
        print(f"\n⏭️  Skipping URL-based pre-filtering (--skip_url_dedup enabled)")
        print(f"   📊 All {len(video_info)} videos will be processed (no pre-filtering)")
    else:
        print(f"\n🔍 Pre-filtering: Grouping videos by video ID...")
        print(f"   📊 Videos before pre-filtering: {len(video_info)}")
    
    video_id_groups: Dict[str, List[str]] = {}  # video_id -> [job_ids]
    video_id_to_keep: Dict[str, str] = {}  # video_id -> job_id to keep
    videos_without_id = 0  # Count videos that don't match any video ID pattern
    
    # Only do pre-filtering if not skipped
    if not skip_url_dedup:
        for job_id, info in video_info.items():
            video_id = extract_video_id_from_url(info["url"])
            if video_id:
                if video_id not in video_id_groups:
                    video_id_groups[video_id] = []
                video_id_groups[video_id].append(job_id)
            else:
                videos_without_id += 1
        
        # DEBUG: Show statistics
        print(f"   📊 Statistics:")
        print(f"      - Videos with video ID: {len(video_info) - videos_without_id}")
        print(f"      - Videos without video ID: {videos_without_id}")
        print(f"      - Unique video IDs found: {len(video_id_groups)}")
        
        # Show top video ID groups (largest groups)
        if video_id_groups:
            sorted_groups = sorted(video_id_groups.items(), key=lambda x: len(x[1]), reverse=True)
            print(f"   📋 Top 5 largest video ID groups:")
            for video_id, job_ids in sorted_groups[:5]:
                print(f"      - {video_id}: {len(job_ids)} videos (sample job_ids: {job_ids[:3]}...)")
        
        # For each video ID group, keep only the job_id with smallest numeric value
        # BUT: If video ID already exists in other chunks (with smaller job_id), skip entire group
        url_duplicates_removed = 0
        video_ids_to_skip = set()  # Video IDs that already exist in other chunks
        
        # If in chunk mode, check if video IDs already exist in other chunks
        # CRITICAL FIX: Use batch querying and caching to improve performance
        if chunk_start is not None or chunk_end is not None:
            print(f"   🔍 Checking if video IDs already exist in other chunks...")
            start_idx = chunk_start if chunk_start is not None else 0
            end_idx = chunk_end if chunk_end is not None else total_entities
            
            # CRITICAL FIX: Only check video IDs that exist in current chunk (not all videos)
            # This reduces query size significantly
            video_ids_to_check = set(video_id_groups.keys())
            if not video_ids_to_check:
                print(f"   ℹ️  No video IDs to check (all videos without extractable ID)")
            else:
                print(f"   📊 Checking {len(video_ids_to_check)} unique video IDs against other chunks...")
                
                # Query all videos outside current chunk to find existing video IDs
                # OPTIMIZATION: Query in batches to avoid memory issues and improve performance
                try:
                    CROSS_CHUNK_BATCH_SIZE = 5000  # Query 5k videos at a time
                    existing_video_id_map: Dict[str, int] = {}  # video_id -> smallest job_id_num
                    
                    # Query videos before current chunk in batches
                    if start_idx > 0:
                        print(f"   📦 Querying videos before chunk (0 to {start_idx-1})...")
                        query_offset = 0
                        batch_count = 0
                        
                        while query_offset < start_idx:
                            batch_limit = min(CROSS_CHUNK_BATCH_SIZE, start_idx - query_offset)
                            
                            try:
                                before_result = collection.query(
                                    expr=f'job_id >= "url_{query_offset:04d}" and job_id < "url_{start_idx:04d}"',
                                    output_fields=["job_id", "url"],
                                    limit=batch_limit,
                                    offset=0  # Use job_id range instead of offset
                                )
                                
                                if not before_result:
                                    break
                                
                                for item in before_result:
                                    existing_video_id = extract_video_id_from_url(item["url"])
                                    if existing_video_id and existing_video_id in video_ids_to_check:
                                        existing_num = extract_job_id_number(item["job_id"])
                                        # Keep smallest job_id for each video_id
                                        if existing_video_id not in existing_video_id_map or existing_num < existing_video_id_map[existing_video_id]:
                                            existing_video_id_map[existing_video_id] = existing_num
                                
                                batch_count += 1
                                query_offset += len(before_result)
                                
                                if len(before_result) < batch_limit:
                                    break
                                    
                            except Exception as batch_error:
                                error_str = str(batch_error)
                                if "message larger than max" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                                    # Reduce batch size and retry
                                    CROSS_CHUNK_BATCH_SIZE = max(1000, CROSS_CHUNK_BATCH_SIZE // 2)
                                    print(f"   ⚠️  Batch too large, reducing to {CROSS_CHUNK_BATCH_SIZE}...")
                                    continue
                                else:
                                    print(f"   ⚠️  Error querying batch: {batch_error}")
                                    break
                        
                        print(f"   ✅ Processed {batch_count} batches before chunk")
                    
                    # Query videos after current chunk in batches
                    if end_idx < total_entities:
                        print(f"   📦 Querying videos after chunk ({end_idx} to {total_entities-1})...")
                        query_start = end_idx
                        batch_count = 0
                        
                        while query_start < total_entities:
                            batch_limit = min(CROSS_CHUNK_BATCH_SIZE, total_entities - query_start)
                            
                            try:
                                after_result = collection.query(
                                    expr=f'job_id >= "url_{query_start:04d}" and job_id < "url_{min(query_start + batch_limit, total_entities):04d}"',
                                    output_fields=["job_id", "url"],
                                    limit=batch_limit
                                )
                                
                                if not after_result:
                                    break
                                
                                for item in after_result:
                                    existing_video_id = extract_video_id_from_url(item["url"])
                                    if existing_video_id and existing_video_id in video_ids_to_check:
                                        existing_num = extract_job_id_number(item["job_id"])
                                        # Keep smallest job_id for each video_id
                                        if existing_video_id not in existing_video_id_map or existing_num < existing_video_id_map[existing_video_id]:
                                            existing_video_id_map[existing_video_id] = existing_num
                                
                                batch_count += 1
                                query_start += len(after_result)
                                
                                if len(after_result) < batch_limit:
                                    break
                                    
                            except Exception as batch_error:
                                error_str = str(batch_error)
                                if "message larger than max" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                                    # Reduce batch size and retry
                                    CROSS_CHUNK_BATCH_SIZE = max(1000, CROSS_CHUNK_BATCH_SIZE // 2)
                                    print(f"   ⚠️  Batch too large, reducing to {CROSS_CHUNK_BATCH_SIZE}...")
                                    continue
                                else:
                                    print(f"   ⚠️  Error querying batch: {batch_error}")
                                    break
                        
                        print(f"   ✅ Processed {batch_count} batches after chunk")
                    
                    # Now check which video IDs should be skipped
                    for video_id, smallest_existing_num in existing_video_id_map.items():
                        if video_id in video_id_groups:
                            current_job_ids = video_id_groups[video_id]
                            current_nums = [extract_job_id_number(jid) for jid in current_job_ids]
                            if current_nums and smallest_existing_num < min(current_nums):
                                video_ids_to_skip.add(video_id)
                    
                    if video_ids_to_skip:
                        total_videos_to_skip = sum(len(video_id_groups[vid]) for vid in video_ids_to_skip)
                        print(f"   ⚠️  Found {len(video_ids_to_skip)} video IDs already exist in other chunks")
                        print(f"   ⚠️  CRITICAL: Will skip {total_videos_to_skip} videos from current chunk")
                        print(f"      → This removes {total_videos_to_skip} videos ({total_videos_to_skip/initial_video_count*100:.1f}% of total)")
                    else:
                        print(f"   ✅ No video IDs found in other chunks (all are unique)")
                        
                except Exception as e:
                    print(f"   ⚠️  Warning: Could not check cross-chunk video IDs: {e}")
                    print(f"   → Continuing with pre-filtering within chunk only...")
                    import traceback
                    traceback.print_exc()
        
        for video_id, job_ids in video_id_groups.items():
            # Skip if video ID already exists in other chunks with smaller job_id
            if video_id in video_ids_to_skip:
                # Remove all videos with this video ID from current chunk
                url_duplicates_removed += len(job_ids)
                continue
            
            if len(job_ids) > 1:
                # NEW: Prefer video with highest itag (highest resolution) instead of smallest job_id
                # This ensures we keep the best quality version when multiple resolutions exist
                job_id_with_itag = []
                for jid in job_ids:
                    url = video_info[jid]["url"]
                    itag = extract_itag_from_url(url)
                    job_id_num = extract_job_id_number(jid)
                    # Store: (itag, job_id_num, job_id)
                    # Sort by itag DESC (highest first), then job_id_num ASC (smallest) as tiebreaker
                    job_id_with_itag.append((itag, job_id_num, jid))
                
                # Sort: first by itag (descending - highest resolution first), then by job_id (ascending - smallest as tiebreaker)
                job_id_with_itag.sort(key=lambda x: (-x[0], x[1]))  # Negative itag for descending
                
                # Keep the one with highest itag (or smallest job_id if no itag)
                best_job_id = job_id_with_itag[0][2]
                best_itag = job_id_with_itag[0][0]
                
                video_id_to_keep[video_id] = best_job_id
                url_duplicates_removed += len(job_ids) - 1
                
                # DEBUG: Log which video was kept and why
                if best_itag > 0:
                    print(f"      → Kept {best_job_id} (itag={best_itag}, highest resolution) from {len(job_ids)} videos")
                else:
                    print(f"      → Kept {best_job_id} (no itag, smallest job_id) from {len(job_ids)} videos")
        
        if url_duplicates_removed > 0:
            print(f"   ✅ Found {len(video_id_groups)} unique video IDs")
            print(f"   🗑️  Pre-filtered {url_duplicates_removed} URL duplicates (same video ID, different signatures/itags)")
            
            # Remove duplicate job_ids from video_info (keep only the one with smallest job_id)
            # Also remove all videos with video IDs that already exist in other chunks
            job_ids_to_remove = set()
            for video_id, job_ids in video_id_groups.items():
                # If video ID already exists in other chunks, remove all videos with this ID
                if video_id in video_ids_to_skip:
                    for job_id in job_ids:
                        job_ids_to_remove.add(job_id)
                elif len(job_ids) > 1:
                    keep_job_id = video_id_to_keep[video_id]
                    for job_id in job_ids:
                        if job_id != keep_job_id:
                            job_ids_to_remove.add(job_id)
            
            # Remove from video_info and all_data
            for job_id in job_ids_to_remove:
                if job_id in video_info:
                    del video_info[job_id]
            
            # Filter all_data
            all_data = [v for v in all_data if v["job_id"] in video_info]
            removed_count = initial_video_count - len(video_info)
            print(f"   ✅ Filtered to {len(video_info)} videos (removed {len(job_ids_to_remove)} URL duplicates)")
            print(f"      - Videos with video ID kept: {len(video_id_groups)}")
            print(f"      - Videos without video ID kept: {videos_without_id}")
            print(f"   ⚠️  CRITICAL: Removed {removed_count} videos out of {initial_video_count} ({removed_count/initial_video_count*100:.1f}%)")
            print(f"      → This might be too aggressive if videos have similar URLs but different content!")
        else:
            print(f"   ℹ️  No URL duplicates found (all videos have unique video IDs)")
            print(f"   📊 Videos after pre-filtering: {len(video_info)} (no change)")
    
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
                
                # CRITICAL FIX: Memory cleanup - clear batch data after processing
                # This helps free memory for large collections
                del batch
                
                # Update progress and monitor memory
                if progress_bar:
                    progress_bar.update(1)
                    if processed_batches % 10 == 0:
                        if tracker:
                            tracker.update_stats()
                            # Check memory usage periodically
                            current_ram_mb = tracker.process.memory_info().rss / 1024 / 1024
                            if current_ram_mb > tracker.peak_ram_mb * 1.1:  # 10% increase
                                print(f"\n   ⚠️  Memory usage increased to {current_ram_mb:.1f} MB")
                elif processed_batches % 50 == 0:
                    print(f"   📊 Processed {processed_batches}/{total_batches} batches...")
                    if tracker:
                        current_ram_mb = tracker.process.memory_info().rss / 1024 / 1024
                        print(f"      Memory: {current_ram_mb:.1f} MB")
            
            except Exception as e:
                print(f"⚠️  Error processing batch: {e}")
                import traceback
                print(f"   Traceback: {traceback.format_exc()[:300]}")
        
        if progress_bar:
            progress_bar.close()
    
    # Remove duplicate pairs (same pair with different order)
    duplicate_pairs = list(set(duplicate_pairs))
    print(f"   ✅ Found {len(duplicate_pairs)} duplicate pairs (including cross-chunk)")
    
    # CRITICAL FIX: Save count before clearing all_data for memory cleanup
    # We need this count for performance report later
    total_videos_processed = len(all_data)
    
    # CRITICAL FIX: Clear all_data after search to free memory
    # We no longer need the full embeddings list, only video_info (which has URLs)
    # Note: all_data is still referenced in search_batch closures, but after search completes,
    # we can safely clear it as we only need video_info going forward
    print(f"   🧹 Cleaning up memory after search...")
    # Create a copy of video_info keys for reference, then clear all_data
    all_data = None  # Mark for garbage collection
    import gc
    gc.collect()  # Force garbage collection
    print(f"   ✅ Memory cleanup complete")
    
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
    # Note: extract_job_id_number is defined at global scope (line 174)
    
    # Track videos that should be marked as duplicates due to cross-chunk pairs
    # CRITICAL FIX: If a video in current chunk has a duplicate with ANY video outside chunk,
    # we need to check if the outside video has a smaller job_id. If yes, mark current video as duplicate.
    # If the outside video has a larger job_id, we still mark current video as duplicate (because original is outside).
    cross_chunk_duplicates: Set[str] = set()
    cross_chunk_originals: Dict[str, Tuple[str, float]] = {}  # job_id -> (original_job_id, similarity)
    
    # Skip cross-chunk duplicate removal if disabled
    if skip_cross_chunk:
        print(f"   ⏭️  Skipping cross-chunk duplicate removal (--skip_cross_chunk enabled)")
        print(f"   📊 All {len(all_job_ids)} videos in chunk will be processed independently")
    else:
        print(f"   🔗 Processing cross-chunk duplicates (threshold: {cross_chunk_threshold})")
        print(f"      → Only videos with similarity >= {cross_chunk_threshold} will be marked as cross-chunk duplicates")
        
        # DEBUG: Track statistics
        pairs_with_duplicate_in_chunk = 0
        pairs_with_original_in_chunk = 0
        
        for job_id1, job_id2, similarity in cross_chunk_pairs_list:
            # CRITICAL: Only mark as cross-chunk duplicate if similarity >= cross_chunk_threshold
            # This prevents false positives (videos that are similar but not identical)
            if similarity < cross_chunk_threshold:
                continue  # Skip this pair - similarity too low for cross-chunk removal
            
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
        print(f"      - Pairs with similarity >= {cross_chunk_threshold}: {pairs_with_duplicate_in_chunk + pairs_with_original_in_chunk}")
        print(f"      - Pairs with duplicate in chunk: {pairs_with_duplicate_in_chunk}")
        print(f"      - Pairs with original in chunk: {pairs_with_original_in_chunk}")
        print(f"      - Unique duplicates marked: {len(cross_chunk_duplicates)}")
        
        if cross_chunk_duplicates:
            print(f"   🔗 Marked {len(cross_chunk_duplicates)} videos as duplicates (original in other chunk)")
            print(f"      → These videos will be excluded from unique results")
            # Show sample cross-chunk duplicates
            sample_dups = list(cross_chunk_duplicates)[:5]
            for dup_id in sample_dups:
                orig_id, sim = cross_chunk_originals[dup_id]
                print(f"      Example: {dup_id} -> original {orig_id} (sim={sim:.4f})")
        else:
            print(f"   ✅ No cross-chunk duplicates found (all videos in chunk are unique)")
    
    # ============================================================
    # Build graph for within-chunk clustering
    # ============================================================
    print(f"   🔧 Building graph from {len(chunk_duplicate_pairs)} pairs...")
    # OPTIMIZATION: Build similarity lookup dictionary FIRST (needed for path validation)
    # Instead of searching through pairs, use a dict: (job_id1, job_id2) -> similarity
    print(f"   🔧 Building similarity lookup dictionary...")
    similarity_lookup: Dict[Tuple[str, str], float] = {}
    for job_id1, job_id2, sim in chunk_duplicate_pairs:
        # Normalize: always smaller job_id first for consistent lookup
        key = tuple(sorted([job_id1, job_id2]))
        # Keep highest similarity if multiple pairs exist
        if key not in similarity_lookup or sim > similarity_lookup[key]:
            similarity_lookup[key] = sim
    print(f"   ✅ Built lookup dict with {len(similarity_lookup)} entries")
    
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
    # CRITICAL FIX: Use path-based similarity validation to prevent transitive closure issues
    visited: Set[str] = set()
    clusters: List[Set[str]] = []
    
    def dfs_iterative_with_validation(start_node: str, max_path_length: int = 3) -> Set[str]:
        """
        Iterative DFS with path-based similarity validation
        Prevents transitive closure: A-B-C-D where A and D are not similar
        Only connects nodes if path similarity is maintained above threshold
        """
        if start_node in visited:
            return set()
        
        cluster = set()
        stack = [(start_node, [start_node], 1.0)]  # (node, path, min_similarity_along_path)
        
        while stack:
            node, path, min_sim = stack.pop()
            if node in visited:
                continue
            
            # CRITICAL: Only add if path similarity is still above threshold
            # If path is too long or similarity dropped too much, don't add
            if len(path) > max_path_length:
                continue
            
            # For paths longer than 1, validate similarity along the path
            # This prevents transitive closure where A-B-C-D but A and D are not similar
            if len(path) > 1:
                prev_node = path[-2]
                # Get similarity for the edge we're traversing (prev_node -> node)
                edge_key = tuple(sorted([prev_node, node]))
                edge_sim = similarity_lookup.get(edge_key, 0.0)
                
                # Update minimum similarity along path
                min_sim = min(min_sim, edge_sim)
                
                # CRITICAL: If path similarity dropped below threshold, stop this path
                # This prevents connecting nodes that are too dissimilar through a chain
                # Use 90% of threshold to allow some tolerance for path decay
                if min_sim < similarity_threshold * 0.9:
                    continue
            
            visited.add(node)
            cluster.add(node)
            
            # Add neighbors to stack with updated path
            for neighbor in graph.get(node, set()):
                if neighbor not in visited and neighbor not in path:
                    new_path = path + [neighbor]
                    stack.append((neighbor, new_path, min_sim))
        
        return cluster
    
    def dfs_iterative(start_node: str) -> Set[str]:
        """Fallback: Simple iterative DFS (used if similarity validation disabled)"""
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
    # CRITICAL FIX: Use validated DFS to prevent transitive closure issues
    print(f"   🔍 Finding connected components (clusters with path validation)...")
    use_validated_dfs = True  # Enable path-based validation by default
    
    for job_id in all_job_ids:
        if job_id not in visited and job_id not in cross_chunk_duplicates:
            if use_validated_dfs:
                cluster = dfs_iterative_with_validation(job_id, max_path_length=3)
            else:
                cluster = dfs_iterative(job_id)
            if cluster:
                clusters.append(cluster)
                if len(clusters) % 10 == 0:
                    print(f"      Found {len(clusters)} clusters so far...")
    
    print(f"   ✅ Found {len(clusters)} clusters (excluding cross-chunk duplicates)")
    
    # DEBUG: Show cluster size distribution
    if clusters:
        cluster_sizes = [len(c) for c in clusters]
        cluster_sizes.sort(reverse=True)
        print(f"   📊 Cluster size distribution:")
        print(f"      - Largest cluster: {cluster_sizes[0]} videos")
        print(f"      - Top 5 largest: {cluster_sizes[:5]}")
        print(f"      - Total videos in clusters: {sum(cluster_sizes)}")
        if len(cluster_sizes) > 10:
            print(f"      - Smallest 5 clusters: {cluster_sizes[-5:]}")
    
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
    
    # Note: similarity_lookup already built above (before graph building)
    
    # Process clusters with progress tracking
    print(f"   🔄 Processing {len(clusters)} clusters...")
    
    # CRITICAL: Detect suspiciously large clusters (likely transitive closure issue)
    MAX_CLUSTER_SIZE_WARNING = 1000  # Warn if cluster > 1000 videos
    large_clusters = []
    
    for cluster_idx, cluster in enumerate(clusters):
        if (cluster_idx + 1) % 10 == 0 or cluster_idx == 0:
            print(f"      Processing cluster {cluster_idx + 1}/{len(clusters)} (size: {len(cluster)})")
        
        # Detect large clusters
        if len(cluster) > MAX_CLUSTER_SIZE_WARNING:
            large_clusters.append((cluster_idx, len(cluster), list(cluster)[:5]))  # Store first 5 job_ids as sample
        
        # CRITICAL FIX: Exclude cross-chunk duplicates from cluster processing
        # If a video is already marked as cross-chunk duplicate, skip it
        cluster_filtered = [jid for jid in cluster if jid not in cross_chunk_duplicates]
        
        if not cluster_filtered:
            # All videos in cluster are cross-chunk duplicates, skip
            continue
        
        # CRITICAL FIX: Sort by resolution quality (highest first), then by job_id (smallest as tiebreaker)
        # This ensures we keep the best quality version (e.g., 1920x1080) when multiple resolutions exist
        def get_video_quality_score(job_id: str) -> tuple:
            """Get quality score for sorting: (resolution_score, job_id_num)"""
            url = video_info[job_id]["url"]
            resolution_score = get_resolution_score(url)
            job_id_num = extract_job_id_number(job_id)
            # Return tuple: (-resolution_score for descending, job_id_num for ascending)
            # Negative resolution_score so higher resolution comes first
            return (-resolution_score, job_id_num)
        
        sorted_cluster = sorted(cluster_filtered, key=get_video_quality_score)
        original_job_id = sorted_cluster[0]  # Highest resolution (or smallest job_id if same resolution)
        
        # DEBUG: Log which video was selected and why
        original_url = video_info[original_job_id]["url"]
        original_resolution = extract_resolution_from_url(original_url)
        original_itag = extract_itag_from_url(original_url)
        if original_resolution[0] > 0:
            print(f"      → Selected {original_job_id} as original (resolution: {original_resolution[0]}x{original_resolution[1]}, itag: {original_itag}) from cluster of {len(cluster_filtered)} videos")
        elif original_itag > 0:
            print(f"      → Selected {original_job_id} as original (itag: {original_itag}) from cluster of {len(cluster_filtered)} videos")
        else:
            print(f"      → Selected {original_job_id} as original (no resolution info, smallest job_id) from cluster of {len(cluster_filtered)} videos")
        
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
    for duplicate_job_id in cross_chunk_duplicates:
        original_job_id, similarity = cross_chunk_originals[duplicate_job_id]
        duplicates.append({
            "duplicate_url": video_info[duplicate_job_id]["url"],
            "duplicate_job_id": duplicate_job_id,
            "original_job_id": original_job_id,
            "original_url": f"[CROSS-CHUNK: {original_job_id}]",  # Mark as cross-chunk
            "similarity": f"{similarity:.6f}"
        })
    
    within_chunk_duplicates = len(duplicates) - len(cross_chunk_duplicates)
    print(f"   ✅ Selected {len(originals)} originals, {within_chunk_duplicates} within-chunk duplicates, {len(cross_chunk_duplicates)} cross-chunk duplicates")
    
    # Warn about large clusters
    if large_clusters:
        print(f"\n   ⚠️  WARNING: Found {len(large_clusters)} suspiciously large clusters (> {MAX_CLUSTER_SIZE_WARNING} videos):")
        for cluster_idx, size, sample_ids in large_clusters:
            print(f"      - Cluster {cluster_idx + 1}: {size} videos (sample: {sample_ids})")
        print(f"   💡 This might be caused by:")
        print(f"      1. Transitive closure: Videos connected through chain (A-B, B-C, C-D → all in one cluster)")
        print(f"      2. Same video ID: Videos with same Google CDN/YouTube ID but different itag/signature")
        print(f"      3. Threshold too low: Similarity threshold {similarity_threshold} might be too low")
        print(f"   💡 SOLUTION: Remove --skip_url_dedup flag to enable pre-filtering by video ID")
    
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
    
    # DEBUG: Show breakdown
    if chunk_start is not None and chunk_end is not None:
        print(f"\n📊 CHUNK MODE SUMMARY:")
        print(f"   Input chunk size: {chunk_end - chunk_start} videos")
        print(f"   Unique videos in chunk: {len(final_unique_videos)}")
        print(f"   Duplicates found: {len(duplicates)}")
        if chunk_end > chunk_start:
            dedup_rate = (1 - len(final_unique_videos) / (chunk_end - chunk_start)) * 100
            print(f"   Deduplication rate: {dedup_rate:.1f}%")
        # Note: cross_chunk_duplicates is defined in the cross-chunk processing section above
    
    if tracker:
        tracker.end_phase()
        # Print performance report
        # Use total_videos_processed instead of len(all_data) since all_data was cleared for memory
        tracker.print_report(total_videos_processed, len(final_unique_videos), len(duplicates))
    
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
        "--config-only",
        action="store_true",
        help="Only print configuration"
    )
    parser.add_argument(
        "--skip_url_dedup",
        action="store_true",
        help="Skip URL-based pre-filtering (disable aggressive video ID deduplication). Use this if videos with similar URLs but different content are being incorrectly removed."
    )
    parser.add_argument(
        "--skip_cross_chunk",
        action="store_true",
        help="Skip cross-chunk duplicate removal. Use this to process each chunk independently without removing videos that have duplicates in other chunks."
    )
    parser.add_argument(
        "--cross_chunk_threshold",
        type=float,
        default=0.98,
        help="Threshold for cross-chunk duplicate detection (default: 0.98). Only videos with similarity >= this threshold will be marked as cross-chunk duplicates. Use 1.0 for exact matches only."
    )
    args = parser.parse_args()
    
    # Validate
    if not 0.0 <= args.cosine_thresh <= 1.0:
        print(f"❌ ERROR: Threshold must be 0.0-1.0 (got {args.cosine_thresh})", file=sys.stderr)
        sys.exit(1)
    
    if not 0.0 <= args.cross_chunk_threshold <= 1.0:
        print(f"❌ ERROR: Cross-chunk threshold must be 0.0-1.0 (got {args.cross_chunk_threshold})", file=sys.stderr)
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
    
    if args.chunk_start is not None and args.chunk_end is not None:
        # Add chunk suffix to avoid overwriting
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
            skip_url_dedup=args.skip_url_dedup,
            skip_cross_chunk=args.skip_cross_chunk,
            cross_chunk_threshold=args.cross_chunk_threshold
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

