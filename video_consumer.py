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
KAFKA_GROUP_ID = os.getenv("KAFKA_GROUP_ID", "video_processor_group")

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


def get_video_hash(video_url: str) -> str:
    """Tạo hash từ video URL để dùng làm key trong Redis"""
    return hashlib.md5(video_url.encode('utf-8')).hexdigest()


def connect_redis() -> redis.Redis:
    """Kết nối Redis"""
    try:
        r = redis.Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            password=REDIS_PASSWORD,
            decode_responses=True,
            socket_connect_timeout=5,
            socket_timeout=5
        )
        # Test connection
        r.ping()
        console.print(f"[green]✅ Redis connected: {REDIS_HOST}:{REDIS_PORT}[/green]")
        return r
    except Exception as e:
        console.print(f"[red]❌ Failed to connect to Redis: {e}[/red]")
        raise


def connect_kafka() -> KafkaConsumer:
    """Kết nối Kafka Consumer"""
    try:
        # Dùng group_id cố định để đảm bảo đọc được messages mới
        # Nếu muốn đọc lại từ đầu, có thể reset offset: kafka-consumer-groups --reset-offsets
        consumer = KafkaConsumer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            group_id=KAFKA_GROUP_ID,  # Group ID cố định
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='latest',  # Đọc từ message mới nhất (không đọc lại messages cũ)
            enable_auto_commit=True,  # Auto commit để đảm bảo không đọc lại
            consumer_timeout_ms=2000,
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
        console.print(f"[cyan]📨 Topic: {KAFKA_TOPIC}, Group: {KAFKA_GROUP_ID}[/cyan]")
        console.print(f"[dim]   Auto offset reset: latest (will read new messages only)[/dim]")
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
    """Cập nhật thống kê vào Redis"""
    try:
        redis_client.set(f"{REDIS_KEY_PREFIX_STATS}total_before", stats.get("total_before", 0))
        redis_client.set(f"{REDIS_KEY_PREFIX_STATS}total_after", stats.get("total_after", 0))
        redis_client.incr(f"{REDIS_KEY_PREFIX_STATS}total_added", stats.get("added", 0))
        redis_client.incr(f"{REDIS_KEY_PREFIX_STATS}total_duplicates", stats.get("duplicates", 0))
    except Exception as e:
        console.print(f"[yellow]⚠️  Failed to update stats: {e}[/yellow]")


def process_video_message(
    message: Dict,
    redis_client: redis.Redis,
    video_service: VideoService
) -> Dict:
    """
    Xử lý một video message từ Kafka
    
    Luồng:
    1. Kiểm tra Redis cache
    2. Nếu cache miss → Embedding & Search Milvus
    3. Nếu TVC mới → Lưu vào Milvus & Lấy unique_id mới
    4. Nếu TVC cũ → Lấy unique_id cũ
    5. Lưu vào Redis cache
    6. Trả về kết quả
    """
    request_id = message.get("request_id", "unknown")
    video_url = message.get("video_url", "")
    timestamp = message.get("timestamp", datetime.now().isoformat())
    
    if not video_url:
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
    console.print(f"[dim]Consumer will read from earliest offset (all messages)[/dim]\n")
    
    # Consumer loop
    try:
        processed_count = 0
        poll_count = 0
        while True:
            try:
                # Poll for messages (timeout 2 seconds)
                message_pack = kafka_consumer.poll(timeout_ms=2000)
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
                        try:
                            console.print(f"[yellow]Processing message: {message.value.get('request_id', 'unknown')[:8]}...[/yellow]")
                            
                            # Process message
                            result = process_video_message(
                                message.value,
                                redis_client,
                                video_service
                            )
                            
                            # Commit sau khi xử lý thành công
                            kafka_consumer.commit()
                            processed_count += 1
                            
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
                            continue
                
            except KeyboardInterrupt:
                console.print("\n[yellow]⚠️  Interrupted by user[/yellow]")
                break
            except Exception as e:
                console.print(f"[red]❌ Error in consumer loop: {e}[/red]")
                import traceback
                traceback.print_exc()
                time.sleep(5)  # Wait before retrying
                continue
    
    finally:
        console.print("\n[yellow]🛑 Shutting down consumer...[/yellow]")
        kafka_consumer.close()
        console.print("[green]✅ Consumer stopped[/green]")


if __name__ == "__main__":
    main()

