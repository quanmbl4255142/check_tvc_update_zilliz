"""
Kafka Consumer xử lý video: Redis cache → check_tvc → Milvus
Luồng: Kafka → Redis (cache check) → Embedding & Search Milvus → Lưu vào Milvus → Update Redis
"""

import os
import json
import sys
import time
import hashlib
from typing import Optional, Dict, Tuple
from datetime import datetime

from kafka import KafkaConsumer
from kafka.errors import KafkaError
import redis
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from video_service import VideoService
from milvus_config import print_config

console = Console()

# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "video_processing")
# Group ID cố định - nếu muốn reset offset, set RESET_OFFSET=true hoặc xóa consumer group
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "video_processor_group")
RESET_OFFSET_ON_START = os.getenv("RESET_OFFSET", "false").lower() == "true"

# Retry configuration - Import từ timeout_config
try:
    from timeout_config import (
        REDIS_SOCKET_CONNECT_TIMEOUT,
        REDIS_SOCKET_TIMEOUT,
        REDIS_HEALTH_CHECK_INTERVAL,
        REDIS_CONNECTION_RETRIES,
        REDIS_RETRY_DELAY,
        KAFKA_CONSUMER_TIMEOUT_MS,
        KAFKA_MAX_POLL_RECORDS,
        KAFKA_MAX_POLL_INTERVAL_MS,
        KAFKA_SESSION_TIMEOUT_MS,
        KAFKA_HEARTBEAT_INTERVAL_MS,
        CONSUMER_ERROR_RETRY_DELAY,
        CONSUMER_MAX_ERROR_WAIT,
    )
except ImportError:
    # Fallback
    REDIS_SOCKET_CONNECT_TIMEOUT = int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
    REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
    REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
    REDIS_CONNECTION_RETRIES = int(os.getenv("REDIS_CONNECTION_RETRIES", "5"))
    REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "2"))
    KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "2000"))
    KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "10"))
    KAFKA_MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))
    KAFKA_SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))
    KAFKA_HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))
    CONSUMER_ERROR_RETRY_DELAY = int(os.getenv("CONSUMER_ERROR_RETRY_DELAY", "5"))
    CONSUMER_MAX_ERROR_WAIT = int(os.getenv("CONSUMER_MAX_ERROR_WAIT", "60"))

# Retry configuration (legacy - giữ để backward compatibility)
MAX_RETRY_ATTEMPTS = int(os.getenv("MAX_RETRY_ATTEMPTS", "3"))
RETRY_DELAY_SECONDS = int(os.getenv("RETRY_DELAY_SECONDS", "5"))

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

# Redis key prefixes
REDIS_KEY_PREFIX_VIDEO = "video:"
REDIS_KEY_PREFIX_UNIQUE_ID = "unique_id:"
REDIS_KEY_PREFIX_STATS = "stats:"

# Cache TTL (seconds) - 7 days
CACHE_TTL = int(os.getenv("CACHE_TTL", "604800"))

# Result TTL (seconds) - 30 phút (tăng từ 5 phút để đảm bảo UI có đủ thời gian lấy kết quả)
RESULT_TTL = int(os.getenv("RESULT_TTL", "1800"))  # 30 phút

# Processing status TTL (seconds) - 1 giờ (cho status "processing")
PROCESSING_STATUS_TTL = int(os.getenv("PROCESSING_STATUS_TTL", "3600"))  # 1 giờ


def get_video_hash(video_url: str) -> str:
    """Tạo hash từ video URL để dùng làm key trong Redis"""
    return hashlib.md5(video_url.encode('utf-8')).hexdigest()


def connect_redis() -> redis.Redis:
    """Kết nối Redis với retry mechanism và connection pooling"""
    max_retries = REDIS_CONNECTION_RETRIES
    retry_delay = REDIS_RETRY_DELAY
    
    for attempt in range(max_retries):
        try:
            r = redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                password=REDIS_PASSWORD,
                decode_responses=True,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,  # Keep connection alive
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                retry_on_timeout=True,  # Retry on timeout
                retry_on_error=[redis.ConnectionError, redis.TimeoutError]
            )
            # Test connection
            r.ping()
            console.print(f"[green]✅ Redis connected: {REDIS_HOST}:{REDIS_PORT}[/green]")
            return r
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt < max_retries - 1:
                console.print(f"[yellow]⚠️  Redis connection attempt {attempt + 1}/{max_retries} failed: {e}[/yellow]")
                console.print(f"[dim]   Retrying in {retry_delay} seconds...[/dim]")
                time.sleep(retry_delay)
                retry_delay *= 1.5  # Exponential backoff
            else:
                console.print(f"[red]❌ Failed to connect to Redis after {max_retries} attempts: {e}[/red]")
                raise
        except Exception as e:
            console.print(f"[red]❌ Unexpected error connecting to Redis: {e}[/red]")
            raise


def connect_kafka() -> KafkaConsumer:
    """Kết nối Kafka Consumer"""
    try:
        # Nếu muốn reset offset, dùng group_id mới (tạo group mới sẽ đọc từ latest)
        group_id = KAFKA_GROUP_ID
        if RESET_OFFSET_ON_START:
            group_id = f"{KAFKA_GROUP_ID}_reset_{int(time.time())}"
            console.print(f"[yellow]⚠️  RESET_OFFSET=true: Dùng group_id mới: {group_id}[/yellow]")
            console.print(f"[yellow]   Consumer sẽ đọc từ latest offset (chỉ message mới)[/yellow]")
        
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=group_id,  # Group ID
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',  # Chỉ đọc message mới (không đọc message cũ)
            enable_auto_commit=False,  # Manual commit - chỉ commit sau khi xử lý xong
            consumer_timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS,
            max_poll_records=KAFKA_MAX_POLL_RECORDS,
            max_poll_interval_ms=KAFKA_MAX_POLL_INTERVAL_MS,
            session_timeout_ms=KAFKA_SESSION_TIMEOUT_MS,
            heartbeat_interval_ms=KAFKA_HEARTBEAT_INTERVAL_MS,
            api_version=(0, 10, 1)  # Chỉ định API version để nhất quán với producer
        )
        
        # Subscribe topic
        consumer.subscribe([KAFKA_TOPIC])
        console.print(f"[yellow]Subscribing to topic: {KAFKA_TOPIC}...[/yellow]")
        
        # Đợi assignment (topic có thể chưa tồn tại, sẽ được tạo khi có message đầu tiên)
        import time as time_module
        console.print(f"[dim]Waiting for partition assignment (topic may be auto-created on first message)...[/dim]")
        timeout = time_module.time() + 10
        assignment_received = False
        poll_attempts = 0
        while time_module.time() < timeout and poll_attempts < 50:
            consumer.poll(timeout_ms=200)
            poll_attempts += 1
            if consumer.assignment():
                assignment_received = True
                break
        
        if assignment_received:
            partitions = [p.partition for p in consumer.assignment()]
            console.print(f"[green]✅ Assigned to partitions: {partitions}[/green]")
        else:
            console.print(f"[yellow]⚠️  No partitions assigned yet (topic may not exist, will wait for messages)[/yellow]")
        
        console.print(f"[green]✅ Kafka consumer connected: {KAFKA_BOOTSTRAP_SERVERS}[/green]")
        console.print(f"[cyan]📨 Topic: {KAFKA_TOPIC}, Group: {group_id}[/cyan]")
        console.print(f"[dim]   Auto offset reset: LATEST (will only read NEW messages)[/dim]")
        if not RESET_OFFSET_ON_START:
            console.print(f"[dim]   💡 Tip: Để reset offset, set RESET_OFFSET=true hoặc chạy:[/dim]")
            console.print(f"[dim]      docker exec -it kafka kafka-consumer-groups --bootstrap-server localhost:9092 --group {KAFKA_GROUP_ID} --topic {KAFKA_TOPIC} --reset-offsets --to-latest --execute[/dim]")
        return consumer
    except Exception as e:
        console.print(f"[red]❌ Failed to connect to Kafka: {e}[/red]")
        raise


def get_stats_from_redis(redis_client: redis.Redis) -> Dict:
    """Lấy thống kê từ Redis"""
    try:
        total_before = redis_client.get(f"{REDIS_KEY_PREFIX_STATS}total_before") or "0"
        total_after = redis_client.get(f"{REDIS_KEY_PREFIX_STATS}total_after") or "0"
        total_added = redis_client.get(f"{REDIS_KEY_PREFIX_STATS}total_added") or "0"
        total_duplicates = redis_client.get(f"{REDIS_KEY_PREFIX_STATS}total_duplicates") or "0"
        
        return {
            "total_before": int(total_before),
            "total_after": int(total_after),
            "total_added": int(total_added),
            "total_duplicates": int(total_duplicates)
        }
    except Exception:
        return {
            "total_before": 0,
            "total_after": 0,
            "total_added": 0,
            "total_duplicates": 0
        }


def update_stats_in_redis(redis_client: redis.Redis, stats: Dict):
    """Cập nhật thống kê vào Redis - sử dụng Redis atomic operations để tránh race condition"""
    try:
        # Sử dụng Redis WATCH + MULTI + EXEC để đảm bảo atomicity và tránh race condition
        # Hoặc sử dụng Lua script để đảm bảo atomic operations
        pipe = redis_client.pipeline()
        
        # Set stats (overwrite)
        pipe.set(f"{REDIS_KEY_PREFIX_STATS}total_before", stats.get("total_before", 0))
        pipe.set(f"{REDIS_KEY_PREFIX_STATS}total_after", stats.get("total_after", 0))
        
        # Chỉ increment nếu giá trị > 0 để tránh tăng không cần thiết
        # Sử dụng INCRBY thay vì INCR để có thể increment nhiều hơn 1
        added = stats.get("added", 0)
        duplicates = stats.get("duplicates", 0)
        if added > 0:
            pipe.incrby(f"{REDIS_KEY_PREFIX_STATS}total_added", added)
        if duplicates > 0:
            pipe.incrby(f"{REDIS_KEY_PREFIX_STATS}total_duplicates", duplicates)
        
        # Execute tất cả operations trong một transaction (atomic)
        pipe.execute()
    except Exception as e:
        console.print(f"[yellow]⚠️  Failed to update stats: {e}[/yellow]")
        # Fallback: thử lại với retry
        try:
            time.sleep(0.1)
            pipe = redis_client.pipeline()
            pipe.set(f"{REDIS_KEY_PREFIX_STATS}total_before", stats.get("total_before", 0))
            pipe.set(f"{REDIS_KEY_PREFIX_STATS}total_after", stats.get("total_after", 0))
            if added > 0:
                pipe.incrby(f"{REDIS_KEY_PREFIX_STATS}total_added", added)
            if duplicates > 0:
                pipe.incrby(f"{REDIS_KEY_PREFIX_STATS}total_duplicates", duplicates)
            pipe.execute()
        except Exception as retry_error:
            console.print(f"[red]❌ Failed to update stats after retry: {retry_error}[/red]")


def process_video_message(
    message: Dict,
    redis_client: redis.Redis,
    video_service: VideoService
) -> Dict:
    """
    Xử lý một video message từ Kafka
    
    Luồng:
    1. Lưu initial status "processing" vào Redis (tránh race condition)
    2. Kiểm tra Redis cache
    3. Nếu cache miss → Embedding & Search Milvus
    4. Nếu TVC mới → Lưu vào Milvus & Lấy unique_id mới
    5. Nếu TVC cũ → Lấy unique_id cũ
    6. Lưu vào Redis cache
    7. Trả về kết quả
    """
    request_id = message.get("request_id", "unknown")
    video_url = message.get("video_url", "")
    timestamp = message.get("timestamp", datetime.now().isoformat())
    
    # Bước 0: Lưu initial status "processing" vào Redis ngay lập tức (tránh race condition)
    result_key = f"request_id:{request_id}"
    try:
        initial_status = {
            "status": "processing",
            "request_id": request_id,
            "video_url": video_url,
            "message": "Video đang được xử lý...",
            "timestamp": timestamp
        }
        redis_client.setex(
            result_key,
            PROCESSING_STATUS_TTL,  # TTL 1 giờ cho processing status
            json.dumps(initial_status)
        )
        console.print(f"[dim]✅ Đã lưu initial status 'processing' vào Redis với request_id: {request_id}[/dim]")
    except Exception as redis_error:
        console.print(f"[yellow]⚠️  Không thể lưu initial status vào Redis: {redis_error}[/yellow]")
    
    if not video_url:
        # Lưu lỗi vào Redis với request_id
        try:
            result_key = f"request_id:{request_id}"
            error_data = {
                "status": "error",
                "request_id": request_id,
                "video_url": "",
                "message": "Thiếu video_url trong message",
                "error": "Missing video_url"
            }
            redis_client.setex(
                result_key,
                RESULT_TTL,  # TTL 30 phút
                json.dumps(error_data)
            )
            console.print(f"[yellow]⚠️  Đã lưu lỗi 'Thiếu video_url' vào Redis với request_id: {request_id}[/yellow]")
        except Exception as redis_error:
            console.print(f"[red]❌ Không thể lưu lỗi vào Redis: {redis_error}[/red]")
        
        return {
            "status": "error",
            "message": "Thiếu video_url",
            "request_id": request_id
        }
    
    video_hash = get_video_hash(video_url)
    cache_key = f"{REDIS_KEY_PREFIX_VIDEO}{video_hash}"
    
    console.print(f"\n[bold cyan]📹 Processing video: {video_url[:80]}...[/bold cyan]")
    console.print(f"[dim]Request ID: {request_id}[/dim]")
    
    # Bước 1: Kiểm tra Redis cache
    console.print("[yellow]🔍 Bước 1: Kiểm tra Redis cache...[/yellow]")
    cached_data = redis_client.get(cache_key)
    
    if cached_data:
        # Cache Hit
        console.print("[green]✅ Cache Hit![/green]")
        try:
            cached_info = json.loads(cached_data)
            unique_id = cached_info.get("unique_id")
            is_new = cached_info.get("is_new", False)
            added_at = cached_info.get("added_at", timestamp)
            
            console.print(f"[cyan]📋 Unique ID từ cache: {unique_id}[/cyan]")
            console.print(f"[cyan]📊 Video {'MỚI' if is_new else 'CŨ'}: {video_url[:60]}...[/cyan]")
            
            # Lấy stats hiện tại
            stats = get_stats_from_redis(redis_client)
            
            # Lưu kết quả với request_id để API có thể query
            result_key = f"request_id:{request_id}"
            similarity = cached_info.get("similarity", 0.0)
            result_data = {
                "status": "completed",
                "request_id": request_id,
                "video_url": video_url,
                "unique_id": unique_id,
                "is_new": is_new,
                "similarity": similarity,
                "added_at": added_at,
                "message": "Video đã được thêm mới vào Zilliz" if is_new else f"Video đã có trên dữ liệu (similarity: {similarity:.4f}) nên sẽ không thêm vào Zilliz",
                "cache_hit": True
            }
            redis_client.setex(
                result_key,
                RESULT_TTL,  # TTL 30 phút
                json.dumps(result_data)
            )
            
            return {
                "status": "success",
                "request_id": request_id,
                "video_url": video_url,
                "cache_hit": True,
                "unique_id": unique_id,
                "is_new": is_new,
                "added_at": added_at,
                "message": "Video đã tồn tại trong cache",
                "stats": stats
            }
        except Exception as e:
            console.print(f"[yellow]⚠️  Error parsing cache data: {e}[/yellow]")
            # Fall through to cache miss processing
    
    # Cache Miss - Cần xử lý
    console.print("[yellow]❌ Cache Miss - Cần xử lý video[/yellow]")
    
    # Bước 2: Embedding & Search Milvus
    console.print("[yellow]🔍 Bước 2: Embedding & Search Milvus...[/yellow]")
    
    try:
        # Lấy số lượng videos hiện tại trong Milvus (trước khi thêm)
        stats_before = video_service.get_collection_count()
        
        # Kiểm tra duplicate và thêm vào Milvus nếu chưa có
        result = video_service.check_and_add_video(video_url)
        
        if result["status"] == "error":
            # Lưu lỗi vào Redis với request_id để API có thể query
            try:
                result_key = f"request_id:{request_id}"
                error_data = {
                    "status": "error",
                    "request_id": request_id,
                    "video_url": video_url,
                    "message": result.get("message", "Lỗi khi xử lý video"),
                    "error": result.get("error", "Unknown error")
                }
                redis_client.setex(
                    result_key,
                    300,  # TTL 5 phút
                    json.dumps(error_data)
                )
                console.print(f"[yellow]⚠️  Đã lưu lỗi vào Redis với request_id: {request_id}[/yellow]")
            except Exception as redis_error:
                console.print(f"[red]❌ Không thể lưu lỗi vào Redis: {redis_error}[/red]")
            
            return {
                "status": "error",
                "request_id": request_id,
                "video_url": video_url,
                "message": result.get("message", "Lỗi khi xử lý video"),
                "error": result.get("error")
            }
        
        is_new = result["is_new"]
        unique_id = result["unique_id"]
        similarity = result.get("similarity", 0.0)
        
        # Lấy số lượng videos sau khi thêm
        stats_after = video_service.get_collection_count()
        
        # Bước 3a hoặc 3b: Lưu unique_id
        if is_new:
            console.print(f"[green]✅ TVC MỚI - Đã thêm vào Milvus với unique_id: {unique_id}[/green]")
        else:
            console.print(f"[yellow]⚠️  TVC CŨ - Đã tồn tại với unique_id: {unique_id} (similarity: {similarity:.4f})[/yellow]")
        
        # Bước 4: Lưu vào Redis cache
        console.print("[yellow]💾 Bước 4: Lưu vào Redis cache...[/yellow]")
        cache_data = {
            "unique_id": unique_id,
            "is_new": is_new,
            "video_url": video_url,
            "added_at": timestamp,
            "similarity": similarity
        }
        redis_client.setex(
            cache_key,
            CACHE_TTL,
            json.dumps(cache_data)
        )
        console.print("[green]✅ Đã lưu vào Redis cache[/green]")
        
        # Lưu kết quả với request_id để API có thể query
        result_key = f"request_id:{request_id}"
        result_data = {
            "status": "completed",
            "request_id": request_id,
            "video_url": video_url,
            "unique_id": unique_id,
            "is_new": is_new,
            "similarity": similarity,
            "added_at": timestamp,
            "message": "Video đã được thêm mới vào Zilliz" if is_new else f"Video đã có trên dữ liệu (similarity: {similarity:.4f}) nên sẽ không thêm vào Zilliz",
            "stats_before": stats_before,
            "stats_after": stats_after
        }
        redis_client.setex(
            result_key,
            RESULT_TTL,  # TTL 30 phút
            json.dumps(result_data)
        )
        console.print(f"[green]✅ Đã lưu kết quả với request_id: {request_id}[/green]")
        
        # Cập nhật stats
        stats = {
            "total_before": stats_before,
            "total_after": stats_after,
            "added": 1 if is_new else 0,
            "duplicates": 0 if is_new else 1
        }
        update_stats_in_redis(redis_client, stats)
        
        # Lấy stats tổng hợp
        final_stats = get_stats_from_redis(redis_client)
        
        return {
            "status": "success",
            "request_id": request_id,
            "video_url": video_url,
            "cache_hit": False,
            "unique_id": unique_id,
            "is_new": is_new,
            "similarity": similarity,
            "added_at": timestamp,
            "message": f"Video {'đã được thêm mới' if is_new else 'đã tồn tại'}",
            "stats_before": stats_before,
            "stats_after": stats_after,
            "stats": final_stats
        }
        
    except Exception as e:
        console.print(f"[red]❌ Error processing video: {e}[/red]")
        import traceback
        traceback.print_exc()
        
        # Lưu lỗi vào Redis với request_id
        try:
            result_key = f"request_id:{request_id}"
            error_data = {
                "status": "error",
                "request_id": request_id,
                "video_url": video_url,
                "message": f"Lỗi khi xử lý video: {str(e)}",
                "error": str(e)
            }
            redis_client.setex(
                result_key,
                RESULT_TTL,  # TTL 30 phút
                json.dumps(error_data)
            )
        except:
            pass  # Ignore Redis errors
        return {
            "status": "error",
            "request_id": request_id,
            "video_url": video_url,
            "message": f"Lỗi khi xử lý video: {str(e)}",
            "error": str(e)
        }


def main():
    """Main consumer loop"""
    console.print("[bold cyan]🚀 Starting Video Consumer[/bold cyan]")
    console.print("=" * 60)
    
    # Print configuration
    print_config()
    console.print(f"\n[cyan]📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}[/cyan]")
    console.print(f"[cyan]📨 Topic: {KAFKA_TOPIC}, Group: {KAFKA_GROUP_ID}[/cyan]")
    console.print(f"[cyan]🔴 Redis: {REDIS_HOST}:{REDIS_PORT}[/cyan]")
    console.print("=" * 60)
    
    # Connect to services
    try:
        redis_client = connect_redis()
        kafka_consumer = connect_kafka()
        video_service = VideoService()
    except Exception as e:
        console.print(f"[red]❌ Failed to initialize services: {e}[/red]")
        sys.exit(1)
    
    console.print("\n[green]✅ All services connected! Waiting for messages...[/green]\n")
    console.print(f"[dim]Consumer will read from LATEST offset (only new messages)[/dim]")
    console.print(f"[yellow]💡 Tip: Nếu muốn đọc lại message cũ, reset offset: kafka-consumer-groups --bootstrap-server localhost:9092 --group {KAFKA_GROUP_ID} --topic {KAFKA_TOPIC} --reset-offsets --to-latest --execute[/yellow]\n")
    
    # Consumer loop
    try:
        processed_count = 0
        poll_count = 0
        while True:
            try:
                # Poll for messages (timeout từ config)
                message_pack = kafka_consumer.poll(timeout_ms=KAFKA_CONSUMER_TIMEOUT_MS)
                poll_count += 1
                
                if not message_pack:
                    # Log mỗi 10 lần poll để biết đang hoạt động
                    if poll_count % 10 == 0:
                        console.print(f"[dim]Polling... (polled {poll_count} times, waiting for messages)[/dim]")
                    continue
                
                # Có messages!
                total_messages = sum(len(msgs) for msgs in message_pack.values())
                console.print(f"\n[bold green]📨 Received {total_messages} message(s) from {len(message_pack)} partition(s)[/bold green]")
                
                # Process each partition
                for topic_partition, messages in message_pack.items():
                    console.print(f"[cyan]Processing partition {topic_partition.partition}...[/cyan]")
                    for message in messages:
                        request_id = message.value.get('request_id', 'unknown')
                        video_url = message.value.get('video_url', '')[:60]
                        message_offset = message.offset
                        message_partition = topic_partition.partition
                        
                        # Idempotency check - kiểm tra xem message đã được xử lý chưa
                        idempotency_key = f"processed:{request_id}:{message_partition}:{message_offset}"
                        try:
                            if redis_client.get(idempotency_key):
                                console.print(f"[yellow]⚠️  Message đã được xử lý trước đó (idempotency check): {request_id}[/yellow]")
                                # Commit ngay vì đã xử lý rồi
                                kafka_consumer.commit()
                                continue
                        except Exception as idem_error:
                            console.print(f"[yellow]⚠️  Idempotency check failed: {idem_error}[/yellow]")
                            # Continue processing anyway
                        
                        try:
                            console.print(f"[bold yellow]🔄 Processing message:[/bold yellow]")
                            console.print(f"[yellow]   Request ID: {request_id}[/yellow]")
                            console.print(f"[yellow]   Video URL: {video_url}...[/yellow]")
                            console.print(f"[yellow]   Partition: {message_partition}, Offset: {message_offset}[/yellow]")
                            
                            # Process message
                            result = process_video_message(
                                message.value,
                                redis_client,
                                video_service
                            )
                            
                            # Kiểm tra xem kết quả đã được lưu vào Redis chưa
                            result_key = f"request_id:{request_id}"
                            check_result = redis_client.get(result_key)
                            if check_result:
                                console.print(f"[green]✅ Đã xác nhận: Kết quả đã được lưu vào Redis với key: {result_key}[/green]")
                            else:
                                console.print(f"[red]❌ CẢNH BÁO: Kết quả CHƯA được lưu vào Redis với key: {result_key}[/red]")
                                console.print(f"[yellow]   Status: {result.get('status')}, Message: {result.get('message', 'N/A')[:100]}[/yellow]")
                            
                            # CHỈ commit sau khi xử lý thành công (manual commit)
                            # Nếu có lỗi, message sẽ được retry
                            if result.get("status") in ["success", "error"]:
                                # Đánh dấu message đã được xử lý (idempotency)
                                try:
                                    redis_client.setex(idempotency_key, 86400, "1")  # TTL 24h
                                except Exception:
                                    pass  # Ignore idempotency save error
                                
                                kafka_consumer.commit()
                                processed_count += 1
                                console.print(f"[green]✅ Committed offset for request_id: {request_id}[/green]")
                            else:
                                console.print(f"[yellow]⚠️  Không commit offset vì status không rõ ràng: {result.get('status')}[/yellow]")
                            
                            # Print result summary
                            console.print("\n[bold]" + "=" * 60 + "[/bold]")
                            console.print(f"[bold green]✅ Video processed #{processed_count}[/bold green]")
                            console.print(f"[cyan]Status: {result['status']}[/cyan]")
                            console.print(f"[cyan]Video URL: {result.get('video_url', '')[:80]}...[/cyan]")
                            
                            if result['status'] == 'success':
                                console.print(f"[green]Unique ID: {result.get('unique_id')}[/green]")
                                console.print(f"[green]Is New: {'CÓ' if result.get('is_new') else 'KHÔNG'}[/green]")
                                
                                if 'stats_before' in result:
                                    console.print(f"[yellow]📊 Stats Before: {result['stats_before']} videos[/yellow]")
                                    console.print(f"[yellow]📊 Stats After: {result['stats_after']} videos[/yellow]")
                                
                                if 'stats' in result:
                                    stats = result['stats']
                                    console.print(f"[cyan]📈 Total Added: {stats.get('total_added', 0)}[/cyan]")
                                    console.print(f"[cyan]📈 Total Duplicates: {stats.get('total_duplicates', 0)}[/cyan]")
                            
                            console.print("[bold]" + "=" * 60 + "[/bold]\n")
                            
                        except Exception as e:
                            console.print(f"[red]❌ Error processing message: {e}[/red]")
                            import traceback
                            traceback.print_exc()
                            
                            request_id = message.value.get("request_id", "unknown")
                            
                            # Lưu lỗi vào Redis với request_id để API có thể query
                            try:
                                result_key = f"request_id:{request_id}"
                                error_data = {
                                    "status": "error",
                                    "request_id": request_id,
                                    "video_url": message.value.get("video_url", ""),
                                    "message": f"Lỗi khi xử lý message: {str(e)}",
                                    "error": str(e),
                                    "retry_available": True  # Đánh dấu có thể retry
                                }
                                redis_client.setex(
                                    result_key,
                                    RESULT_TTL,  # TTL 30 phút
                                    json.dumps(error_data)
                                )
                                console.print(f"[yellow]⚠️  Đã lưu lỗi vào Redis với request_id: {request_id}[/yellow]")
                            except Exception as redis_error:
                                console.print(f"[red]❌ Không thể lưu lỗi vào Redis: {redis_error}[/red]")
                            
                            # KHÔNG commit message lỗi - để có thể retry sau
                            # Với manual commit, message sẽ được retry tự động khi consumer restart
                            # Hoặc có thể implement retry mechanism phức tạp hơn với dead letter queue
                            console.print(f"[yellow]⚠️  Không commit offset cho message lỗi - sẽ retry khi consumer restart[/yellow]")
                            console.print(f"[dim]   Message sẽ được retry tự động vì offset chưa được commit[/dim]")
                            
                            # Đợi một chút trước khi tiếp tục (tránh spam retry)
                            time.sleep(RETRY_DELAY_SECONDS)
                            
                            continue
                
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️  Interrupted by user[/yellow]")
                break
            except Exception as e:
                console.print(f"[red]❌ Error in consumer loop: {e}[/red]")
                import traceback
                traceback.print_exc()
                # Exponential backoff cho error recovery
                error_wait = min(CONSUMER_ERROR_RETRY_DELAY * (1.5 ** min(processed_count // 10, 5)), CONSUMER_MAX_ERROR_WAIT)
                console.print(f"[yellow]⏳ Waiting {error_wait:.1f}s before retrying...[/yellow]")
                time.sleep(error_wait)
                continue
    
    finally:
        console.print("\n[yellow]🛑 Shutting down consumer...[/yellow]")
        kafka_consumer.close()
        console.print("[green]✅ Consumer stopped[/green]")


if __name__ == "__main__":
    main()

