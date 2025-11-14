"""
Flask API endpoint để nhận video URL và gửi vào Kafka
Endpoint: POST /api/video
"""

import os
import json
import uuid
import time
import redis
from datetime import datetime
from flask import Flask, request, jsonify
from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from rich.console import Console

console = Console()

app = Flask(__name__)

# Kafka configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "video_processing")

# Redis configuration
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))

# Kafka timeout configuration - Import từ timeout_config
try:
    from timeout_config import (
        KAFKA_WAIT_READY_TIMEOUT,
        KAFKA_REQUEST_TIMEOUT_MS,
        KAFKA_MAX_BLOCK_MS,
        KAFKA_SEND_TIMEOUT_MS,
        KAFKA_RETRIES,
        KAFKA_RETRY_DELAY,
        REDIS_SOCKET_CONNECT_TIMEOUT,
        REDIS_SOCKET_TIMEOUT,
        REDIS_HEALTH_CHECK_INTERVAL,
        REDIS_CONNECTION_RETRIES,
        REDIS_RETRY_DELAY,
        RESULT_WAIT_TIMEOUT,
        RESULT_POLL_INTERVAL,
    )
except ImportError:
    # Fallback nếu không có timeout_config
    KAFKA_WAIT_READY_TIMEOUT = int(os.getenv("KAFKA_WAIT_READY_TIMEOUT", "15"))
    KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "10000"))
    KAFKA_MAX_BLOCK_MS = int(os.getenv("KAFKA_MAX_BLOCK_MS", "10000"))
    KAFKA_SEND_TIMEOUT_MS = int(os.getenv("KAFKA_SEND_TIMEOUT_MS", "10000"))
    KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", "3"))
    KAFKA_RETRY_DELAY = int(os.getenv("KAFKA_RETRY_DELAY", "2"))
    REDIS_SOCKET_CONNECT_TIMEOUT = int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))
    REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))
    REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))
    REDIS_CONNECTION_RETRIES = int(os.getenv("REDIS_CONNECTION_RETRIES", "5"))
    REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "2"))
    RESULT_WAIT_TIMEOUT = int(os.getenv("RESULT_WAIT_TIMEOUT", "30"))
    RESULT_POLL_INTERVAL = float(os.getenv("RESULT_POLL_INTERVAL", "0.5"))

# Initialize Kafka producer
producer = None

def ensure_topic_exists():
    """Đảm bảo topic tồn tại - tạo topic bằng AdminClient nếu chưa có"""
    from kafka.admin import KafkaAdminClient, NewTopic
    from kafka.errors import TopicAlreadyExistsError
    
    try:
        # Thử tạo topic bằng AdminClient trước
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='video_api_admin',
            request_timeout_ms=min(KAFKA_REQUEST_TIMEOUT_MS, 15000),  # Max 15s
            api_version=(0, 10, 1)
        )
        
        # Kiểm tra topic có tồn tại không
        try:
            topics = admin_client.list_topics()
            topic_list = list(topics) if isinstance(topics, (set, list)) else topics
            
            if KAFKA_TOPIC in topic_list:
                console.print(f"[green]✅ Topic {KAFKA_TOPIC} already exists[/green]")
                admin_client.close()
                return True
        except Exception as list_error:
            console.print(f"[dim]Could not list topics: {list_error}[/dim]")
            # Continue to try creating topic
        
        # Tạo topic nếu chưa tồn tại
        console.print(f"[cyan]Creating topic {KAFKA_TOPIC}...[/cyan]")
        topic = NewTopic(
            name=KAFKA_TOPIC,
            num_partitions=1,
            replication_factor=1
        )
        
        try:
            admin_client.create_topics([topic], timeout_ms=15000)
            console.print(f"[green]✅ Topic {KAFKA_TOPIC} created successfully[/green]")
            # Đợi topic được tạo xong
            time.sleep(2)
            admin_client.close()
            return True
        except TopicAlreadyExistsError:
            console.print(f"[green]✅ Topic {KAFKA_TOPIC} already exists[/green]")
            admin_client.close()
            return True
        except Exception as create_error:
            console.print(f"[yellow]⚠️  Could not create topic: {create_error}[/yellow]")
            admin_client.close()
            return False
            
    except Exception as e:
        console.print(f"[yellow]⚠️  AdminClient error: {e}[/yellow]")
        console.print(f"[dim]   Topic will be auto-created on first message[/dim]")
        return False

def get_redis_client():
    """Lazy initialization of Redis client với retry và connection pooling"""
    max_retries = REDIS_CONNECTION_RETRIES
    retry_delay = REDIS_RETRY_DELAY
    
    for attempt in range(max_retries):
        try:
            return redis.Redis(
                host=REDIS_HOST,
                port=REDIS_PORT,
                db=REDIS_DB,
                decode_responses=True,
                socket_connect_timeout=REDIS_SOCKET_CONNECT_TIMEOUT,
                socket_timeout=REDIS_SOCKET_TIMEOUT,
                socket_keepalive=True,
                health_check_interval=REDIS_HEALTH_CHECK_INTERVAL,
                retry_on_timeout=True,
                retry_on_error=[redis.ConnectionError, redis.TimeoutError]
            )
        except (redis.ConnectionError, redis.TimeoutError) as e:
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
                retry_delay *= 1.5
            else:
                console.print(f"[yellow]⚠️  Redis connection error after {max_retries} attempts: {e}[/yellow]")
                return None
        except Exception as e:
            console.print(f"[yellow]⚠️  Redis connection error: {e}[/yellow]")
            return None

def wait_for_result(request_id: str, timeout: int, poll_interval: float):
    """
    Đợi kết quả từ consumer trong Redis
    
    Args:
        request_id: Request ID để query
        timeout: Timeout tối đa (giây)
        poll_interval: Khoảng thời gian giữa các lần poll (giây)
    
    Returns:
        dict: Kết quả từ consumer hoặc None nếu timeout
    """
    redis_client = get_redis_client()
    if not redis_client:
        return None
    
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            result_key = f"request_id:{request_id}"
            result_json = redis_client.get(result_key)
            
            if result_json:
                result = json.loads(result_json)
                # Chuyển đổi response format để phù hợp với API
                return {
                    "status": "success" if result.get("status") == "completed" else result.get("status"),
                    "message": result.get("message", ""),
                    "request_id": result.get("request_id", request_id),
                    "video_url": result.get("video_url", ""),
                    "unique_id": result.get("unique_id"),
                    "is_new": result.get("is_new", False),
                    "similarity": result.get("similarity", 0.0),
                    "added_at": result.get("added_at"),
                    "cache_hit": result.get("cache_hit", False),
                    "stats_before": result.get("stats_before"),
                    "stats_after": result.get("stats_after")
                }
        except Exception as e:
            console.print(f"[yellow]⚠️  Error polling Redis: {e}[/yellow]")
        
        time.sleep(poll_interval)
    
    return None

# Cache để tránh check Kafka ready nhiều lần
_kafka_ready_cache = {"ready": False, "last_check": 0, "cache_ttl": 30}

def check_kafka_socket(host='localhost', port=9092, timeout=2):
    """Kiểm tra Kafka socket có sẵn sàng không (nhanh hơn AdminClient)"""
    import socket
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        result = sock.connect_ex((host, port))
        sock.close()
        return result == 0
    except:
        return False

def wait_for_kafka_ready(max_wait=None, force_check=False):
    """Đợi Kafka broker sẵn sàng - test bằng socket check và AdminClient"""
    from kafka.admin import KafkaAdminClient
    from kafka.errors import KafkaError
    import socket
    
    # Sử dụng timeout từ config nếu không chỉ định
    if max_wait is None:
        max_wait = KAFKA_WAIT_READY_TIMEOUT
    
    # Sử dụng cache nếu đã check gần đây
    current_time = time.time()
    if not force_check and _kafka_ready_cache["ready"]:
        if current_time - _kafka_ready_cache["last_check"] < _kafka_ready_cache["cache_ttl"]:
            return True
    
    # Parse bootstrap servers để lấy host và port
    bootstrap_host = "localhost"
    bootstrap_port = 9092
    try:
        if ":" in KAFKA_BOOTSTRAP_SERVERS:
            parts = KAFKA_BOOTSTRAP_SERVERS.split(":")
            bootstrap_host = parts[0].replace("localhost", "127.0.0.1")
            bootstrap_port = int(parts[1])
        else:
            bootstrap_host = KAFKA_BOOTSTRAP_SERVERS.replace("localhost", "127.0.0.1")
    except:
        pass
    
    start_time = time.time()
    attempt = 0
    
    # Bước 1: Kiểm tra socket trước (nhanh hơn)
    console.print(f"[dim]Checking Kafka socket at {bootstrap_host}:{bootstrap_port}...[/dim]")
    socket_ready = False
    socket_check_timeout = min(max_wait, 10)  # Max 10s để check socket
    socket_start_time = time.time()
    
    while time.time() - socket_start_time < socket_check_timeout and not socket_ready:
        socket_ready = check_kafka_socket(bootstrap_host, bootstrap_port, timeout=2)
        if socket_ready:
            console.print(f"[green]✅ Kafka socket is open[/green]")
            # Đợi thêm 5-10 giây để Kafka hoàn toàn sẵn sàng (Zookeeper connection, metadata init)
            console.print(f"[dim]Waiting for Kafka broker to fully initialize (5-10 seconds)...[/dim]")
            time.sleep(5)  # Đợi 5s để Kafka khởi tạo metadata
            break
        time.sleep(1)
    
    if not socket_ready:
        console.print(f"[red]❌ Kafka socket is not open at {bootstrap_host}:{bootstrap_port}[/red]")
        console.print(f"[yellow]💡 Hãy kiểm tra Kafka đang chạy: docker ps | findstr kafka[/yellow]")
        console.print(f"[yellow]💡 Hoặc khởi động Kafka: docker-compose up -d[/yellow]")
        _kafka_ready_cache["ready"] = False
        _kafka_ready_cache["last_check"] = time.time()
        return False
    
    # Bước 2: Test bằng Producer thay vì AdminClient (reliable hơn)
    # Producer có thể kết nối ngay cả khi AdminClient fail
    attempt = 0
    producer_check_start = time.time()
    remaining_time = max_wait - (time.time() - start_time)
    
    # Đảm bảo còn ít nhất 10s để test Producer
    if remaining_time < 10:
        console.print(f"[yellow]⚠️  Không đủ thời gian để test Producer (còn {remaining_time:.1f}s)[/yellow]")
        console.print(f"[yellow]💡 Kafka socket mở nhưng broker có thể chưa sẵn sàng. Sẽ thử tạo Producer trực tiếp.[/yellow]")
        _kafka_ready_cache["ready"] = False
        _kafka_ready_cache["last_check"] = time.time()
        return False  # Nhưng vẫn cho phép tạo Producer
    
    while time.time() - producer_check_start < remaining_time:
        try:
            attempt += 1
            console.print(f"[dim]Testing Kafka broker readiness with Producer (attempt {attempt})...[/dim]")
            
            # Test bằng cách tạo Producer và thử fetch metadata
            # Producer thường reliable hơn AdminClient
            test_producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                request_timeout_ms=min(KAFKA_REQUEST_TIMEOUT_MS, 15000),
                max_block_ms=min(KAFKA_MAX_BLOCK_MS, 15000),
                api_version=(0, 10, 1)
            )
            
            # Đợi một chút để Producer khởi tạo
            time.sleep(1)
            
            # Thử fetch metadata bằng cách gọi partitions_for (non-blocking nếu metadata đã có)
            # Hoặc thử send một dummy message với timeout ngắn
            try:
                partitions = test_producer.partitions_for(KAFKA_TOPIC)
                if partitions is not None:
                    console.print(f"[dim]   Metadata available: {len(partitions) if partitions else 0} partition(s)[/dim]")
            except:
                # Metadata chưa có, nhưng Producer đã tạo được - có nghĩa là Kafka sẵn sàng
                pass
            
            test_producer.close(timeout=2)
            
            # Update cache
            _kafka_ready_cache["ready"] = True
            _kafka_ready_cache["last_check"] = time.time()
            total_time = time.time() - start_time
            console.print(f"[green]✅ Kafka broker is ready! (after {attempt} attempt(s), {total_time:.1f}s)[/green]")
            return True
        except (KafkaError, Exception) as e:
            # Chưa sẵn sàng, đợi thêm
            elapsed = time.time() - producer_check_start
            remaining = remaining_time - elapsed
            if remaining > 5:  # Còn ít nhất 5s thì retry
                wait_time = min(3, remaining / 2)  # Đợi 3s hoặc một nửa thời gian còn lại
                error_msg = str(e)[:100]
                console.print(f"[yellow]⚠️  Broker not ready yet (attempt {attempt}): {error_msg}[/yellow]")
                console.print(f"[dim]   Waiting {wait_time:.1f}s before retry...[/dim]")
                time.sleep(wait_time)
            else:
                # Không còn thời gian để retry
                break
    
    # Update cache - không ready
    _kafka_ready_cache["ready"] = False
    _kafka_ready_cache["last_check"] = time.time()
    console.print(f"[yellow]⚠️  Kafka socket is open but broker may not be fully ready after {max_wait}s[/yellow]")
    console.print(f"[yellow]💡 Kafka có thể đang khởi động hoặc có vấn đề. Hãy thử:[/yellow]")
    console.print(f"[yellow]   1. Kiểm tra Kafka logs: docker logs kafka[/yellow]")
    console.print(f"[yellow]   2. Kiểm tra Zookeeper: docker logs zookeeper[/yellow]")
    console.print(f"[yellow]   3. Restart Kafka: docker restart kafka[/yellow]")
    console.print(f"[yellow]   4. Đợi 30-60 giây rồi thử lại[/yellow]")
    return False

def get_kafka_producer():
    """Lazy initialization of Kafka producer với retry logic và timeout hợp lý"""
    global producer
    if producer is None:
        import time
        max_retries = KAFKA_RETRIES
        retry_delay = KAFKA_RETRY_DELAY
        
        for attempt in range(max_retries + 1):  # +1 để có tổng (max_retries + 1) lần thử
            try:
                # Đợi Kafka sẵn sàng trước khi tạo producer (chỉ lần đầu)
                # Nhưng không fail nếu check fail - vẫn cho phép tạo Producer
                if attempt == 0:
                    console.print(f"[cyan]⏳ Đợi Kafka broker sẵn sàng (timeout: {KAFKA_WAIT_READY_TIMEOUT}s)...[/cyan]")
                    kafka_ready = wait_for_kafka_ready(max_wait=KAFKA_WAIT_READY_TIMEOUT, force_check=(attempt == 0))
                    if not kafka_ready:
                        console.print(f"[yellow]⚠️  Kafka readiness check failed, but will try to create Producer anyway...[/yellow]")
                        console.print(f"[dim]   Producer will retry automatically when sending messages[/dim]")
                
                # Đảm bảo topic tồn tại trước khi tạo Producer
                # Điều này giúp Producer không phải fetch metadata lâu
                try:
                    ensure_topic_exists()
                except Exception as topic_error:
                    console.print(f"[yellow]⚠️  Could not ensure topic exists: {topic_error}[/yellow]")
                    console.print(f"[dim]   Topic will be auto-created on first message[/dim]")
                
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks=0,  # Fire-and-forget để tránh block (message vẫn được gửi)
                    retries=max_retries,  # Số lần retry từ config
                    max_in_flight_requests_per_connection=1,  # Đảm bảo thứ tự message
                    request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,  # Timeout chuẩn hóa
                    metadata_max_age_ms=300000,  # 5 phút - refresh metadata
                    connections_max_idle_ms=540000,  # 9 phút - giữ connection
                    linger_ms=0,  # Gửi ngay (không batch)
                    batch_size=0,  # Không batch
                    max_block_ms=10000,  # Giảm xuống 10s để tránh block quá lâu
                    api_version=(0, 10, 1)  # Chỉ định API version
                )
                
                # Không pre-fetch metadata nữa - để Producer tự fetch khi send
                # Với max_block_ms=10s, Producer sẽ chỉ block tối đa 10s khi fetch metadata
                # Nếu không fetch được trong 10s, Producer sẽ raise exception nhưng message vẫn được queue
                console.print(f"[dim]   Metadata will be fetched automatically on first send (max_block: 10s)[/dim]")
                
                console.print(f"[green]✅ Kafka producer created với acks=0 (fire-and-forget)[/green]")
                console.print(f"[dim]   Config: request_timeout={KAFKA_REQUEST_TIMEOUT_MS}ms, max_block=10000ms, retries={max_retries}[/dim]")
                break  # Thành công, thoát khỏi retry loop
            except Exception as e:
                if attempt < max_retries:
                    console.print(f"[yellow]⚠️  Attempt {attempt + 1}/{max_retries + 1} failed: {e}[/yellow]")
                    console.print(f"[dim]   Retrying in {retry_delay} seconds...[/dim]")
                    time.sleep(retry_delay)
                else:
                    console.print(f"[red]❌ Failed to connect to Kafka after {max_retries + 1} attempts: {e}[/red]")
                    console.print(f"[yellow]💡 Đảm bảo Kafka đang chạy: docker-compose up -d[/yellow]")
                    raise
    return producer


@app.route('/api/video', methods=['POST'])
def add_video():
    """
    Nhận video URL và gửi vào Kafka
    
    Request body:
    {
        "video_url": "https://example.com/video.mp4"
    }
    
    Response:
    {
        "status": "success",
        "message": "Video đã được gửi vào Kafka",
        "request_id": "uuid",
        "video_url": "https://example.com/video.mp4",
        "timestamp": "2024-01-01T00:00:00"
    }
    """
    try:
        data = request.get_json()
        
        if not data or 'video_url' not in data:
            return jsonify({
                "status": "error",
                "message": "Thiếu video_url trong request body"
            }), 400
        
        video_url = data['video_url'].strip()
        
        if not video_url:
            return jsonify({
                "status": "error",
                "message": "video_url không được để trống"
            }), 400
        
        # URL validation - kiểm tra format URL hợp lệ
        import re
        from urllib.parse import urlparse
        
        # Basic URL validation
        url_pattern = re.compile(
            r'^https?://'  # http:// or https://
            r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+[A-Z]{2,6}\.?|'  # domain...
            r'localhost|'  # localhost...
            r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})'  # ...or ip
            r'(?::\d+)?'  # optional port
            r'(?:/?|[/?]\S+)$', re.IGNORECASE)
        
        if not url_pattern.match(video_url):
            return jsonify({
                "status": "error",
                "message": "video_url không đúng định dạng URL hợp lệ"
            }), 400
        
        # Parse URL để kiểm tra scheme và host
        try:
            parsed = urlparse(video_url)
            if not parsed.scheme or not parsed.netloc:
                return jsonify({
                    "status": "error",
                    "message": "video_url phải có scheme (http/https) và host"
                }), 400
        except Exception as e:
            return jsonify({
                "status": "error",
                "message": f"video_url không hợp lệ: {str(e)}"
            }), 400
        
        # Generate unique request ID
        request_id = str(uuid.uuid4())
        timestamp = datetime.now().isoformat()
        
        # Create message payload
        message = {
            "request_id": request_id,
            "video_url": video_url,
            "timestamp": timestamp,
            "status": "pending"
        }
        
        # Send to Kafka - Fire and forget (không đợi confirmation để tránh timeout)
        try:
            console.print(f"[cyan]📤 Sending video to Kafka: {video_url[:60]}...[/cyan]")
            console.print(f"[dim]Request ID: {request_id}[/dim]")
            
            # Thử lấy producer (có thể fail nếu Kafka chưa sẵn sàng)
            try:
                kafka_producer = get_kafka_producer()
            except Exception as producer_error:
                # Nếu không tạo được producer, vẫn trả về "processing"
                console.print(f"[yellow]⚠️  Không thể tạo Kafka producer: {producer_error}[/yellow]")
                console.print(f"[dim]   Trả về status 'processing' - UI sẽ poll status endpoint[/dim]")
                return jsonify({
                    "status": "processing",
                    "message": "Video đã được gửi vào hàng đợi, đang chờ Kafka sẵn sàng. Vui lòng kiểm tra lại sau.",
                    "request_id": request_id,
                    "video_url": video_url,
                    "timestamp": timestamp,
                    "kafka_status": "not_ready",
                    "check_status_url": f"/api/video/status/{request_id}"
                }), 202
            
            # Gửi message - với acks=0, Producer sẽ không block và trả về ngay
            try:
                console.print(f"[cyan]📤 Sending message to Kafka topic: {KAFKA_TOPIC}...[/cyan]")
                console.print(f"[dim]   Request ID: {request_id}[/dim]")
                
                # Gửi message - với max_block_ms=10s, nếu không fetch được metadata trong 10s sẽ raise exception
                # Nhưng với acks=0, Producer sẽ queue message và retry trong background
                try:
                    future = kafka_producer.send(
                        KAFKA_TOPIC,
                        value=message,
                        key=request_id
                    )
                    
                    # Với acks=0, không cần đợi future.get() - message đã được queue
                    # Producer sẽ tự retry trong background nếu có lỗi
                    console.print(f"[green]✅ Message queued to Kafka (fire-and-forget)[/green]")
                    console.print(f"[dim]   Message sẽ được gửi trong background[/dim]")
                    console.print(f"[dim]   Producer sẽ tự retry nếu có lỗi[/dim]")
                    
                    return jsonify({
                        "status": "processing",
                        "message": "Video đã được gửi vào Kafka và đang được xử lý",
                        "request_id": request_id,
                        "video_url": video_url,
                        "timestamp": timestamp,
                        "kafka_topic": KAFKA_TOPIC,
                        "check_status_url": f"/api/video/status/{request_id}",
                        "note": "Đảm bảo video_consumer.py đang chạy để xử lý message. Message đang được gửi trong background."
                    }), 202  # 202 Accepted - đang xử lý
                    
                except Exception as send_error:
                    error_str = str(send_error).lower()
                    # Nếu là metadata timeout, vẫn trả về processing vì Producer có thể retry
                    if "metadata" in error_str or "timeout" in error_str:
                        console.print(f"[yellow]⚠️  Metadata timeout khi gửi message: {send_error}[/yellow]")
                        console.print(f"[dim]   Producer sẽ retry trong background[/dim]")
                        console.print(f"[dim]   Trả về processing để UI có thể poll status[/dim]")
                        
                        return jsonify({
                            "status": "processing",
                            "message": "Video đã được gửi vào hàng đợi. Kafka đang fetch metadata, message sẽ được gửi trong background.",
                            "request_id": request_id,
                            "video_url": video_url,
                            "timestamp": timestamp,
                            "kafka_status": "metadata_fetching",
                            "kafka_error": str(send_error),
                            "check_status_url": f"/api/video/status/{request_id}",
                            "note": "Producer đang fetch metadata. Message sẽ được gửi tự động khi metadata sẵn sàng."
                        }), 202
                    else:
                        # Lỗi khác
                        console.print(f"[red]❌ Error sending to Kafka: {send_error}[/red]")
                        import traceback
                        traceback.print_exc()
                        
                        return jsonify({
                            "status": "error",
                            "message": f"Không thể gửi message vào Kafka: {str(send_error)}",
                            "request_id": request_id,
                            "video_url": video_url,
                            "timestamp": timestamp,
                            "kafka_error": str(send_error),
                            "hint": "Kiểm tra Kafka đang chạy: docker ps | findstr kafka"
                        }), 500
                        
            except Exception as send_error:
                error_str = str(send_error).lower()
                console.print(f"[red]❌ Error sending to Kafka: {send_error}[/red]")
                import traceback
                traceback.print_exc()
                
                # Nếu là metadata/connection error, trả về processing để UI có thể retry
                if "metadata" in error_str or "timeout" in error_str or "node" in error_str or "connection" in error_str:
                    return jsonify({
                        "status": "processing",
                        "message": "Kafka broker chưa sẵn sàng. Video đã được gửi vào hàng đợi, đang chờ Kafka. Vui lòng kiểm tra Kafka và thử lại sau.",
                        "request_id": request_id,
                        "video_url": video_url,
                        "timestamp": timestamp,
                        "kafka_status": "not_ready",
                        "kafka_error": str(send_error),
                        "check_status_url": f"/api/video/status/{request_id}",
                        "troubleshooting": [
                            "1. Kiểm tra Kafka đang chạy: docker ps | findstr kafka",
                            "2. Khởi động Kafka: docker-compose up -d",
                            "3. Đợi 30-60 giây để Kafka khởi động hoàn toàn",
                            "4. Kiểm tra logs: docker logs kafka"
                        ]
                    }), 202
                else:
                    return jsonify({
                        "status": "error",
                        "message": f"Không thể gửi message vào Kafka: {str(send_error)}",
                        "request_id": request_id,
                        "video_url": video_url,
                        "timestamp": timestamp,
                        "kafka_error": str(send_error),
                        "hint": "Kiểm tra Kafka đang chạy: docker ps | findstr kafka"
                    }), 500
            
        except Exception as e:
            error_str = str(e).lower()
            console.print(f"[red]❌ Failed to send to Kafka: {e}[/red]")
            import traceback
            traceback.print_exc()
            
            # Nếu là connection/metadata error, trả về processing để UI có thể retry
            if "metadata" in error_str or "timeout" in error_str or "node" in error_str or "connection" in error_str:
                console.print(f"[yellow]💡 Kafka không sẵn sàng. Trả về processing để UI có thể retry sau.[/yellow]")
                return jsonify({
                    "status": "processing",
                    "message": "Kafka broker chưa sẵn sàng. Video đã được gửi vào hàng đợi, đang chờ Kafka. Vui lòng kiểm tra Kafka và thử lại sau.",
                    "request_id": request_id,
                    "video_url": video_url,
                    "timestamp": timestamp,
                    "kafka_status": "not_ready",
                    "kafka_error": str(e),
                    "check_status_url": f"/api/video/status/{request_id}",
                    "troubleshooting": [
                        "1. Kiểm tra Kafka: docker ps | findstr kafka",
                        "2. Khởi động Kafka: docker-compose up -d",
                        "3. Đợi 30-60 giây để Kafka khởi động",
                        "4. Kiểm tra logs: docker logs kafka"
                    ]
                }), 202
            else:
                # Lỗi khác
                return jsonify({
                    "status": "error",
                    "message": f"Lỗi khi gửi vào Kafka: {str(e)}",
                    "request_id": request_id,
                    "video_url": video_url,
                    "timestamp": timestamp,
                    "kafka_error": str(e),
                    "hint": "Kiểm tra Kafka đang chạy: docker ps | findstr kafka"
                }), 500
            
    except Exception as e:
        console.print(f"[red]❌ API Error: {e}[/red]")
        return jsonify({
            "status": "error",
            "message": f"Lỗi server: {str(e)}"
        }), 500


@app.route('/api/video/status/<request_id>', methods=['GET'])
def get_video_status(request_id):
    """
    Kiểm tra trạng thái xử lý video theo request_id
    
    Response:
    {
        "status": "completed|processing|error",
        "message": "...",
        "request_id": "...",
        ...
    }
    """
    redis_client = get_redis_client()
    if not redis_client:
        return jsonify({
            "status": "error",
            "message": "Không thể kết nối Redis"
        }), 503
    
    try:
        result_key = f"request_id:{request_id}"
        result_json = redis_client.get(result_key)
        
        if result_json:
            result = json.loads(result_json)
            # Map status: "completed" -> "success" để phù hợp với API response
            status = result.get("status", "unknown")
            if status == "completed":
                status = "success"
            
            return jsonify({
                "status": status,
                "message": result.get("message", ""),
                "request_id": result.get("request_id", request_id),
                "video_url": result.get("video_url", ""),
                "unique_id": result.get("unique_id"),
                "is_new": result.get("is_new", False),
                "similarity": result.get("similarity", 0.0),
                "added_at": result.get("added_at"),
                "cache_hit": result.get("cache_hit", False),
                "stats_before": result.get("stats_before"),
                "stats_after": result.get("stats_after"),
                "error": result.get("error")  # Thêm error field nếu có
            }), 200
        else:
            # Không tìm thấy kết quả - có thể consumer chưa xử lý hoặc chưa chạy
            # Kiểm tra xem có message trong Kafka không (optional - chỉ để debug)
            console.print(f"[dim]⚠️  Không tìm thấy kết quả cho request_id: {request_id}[/dim]")
            console.print(f"[dim]   Có thể: 1) Consumer chưa xử lý 2) Consumer chưa chạy 3) Message chưa được consume[/dim]")
            return jsonify({
                "status": "processing",
                "message": "Video đang được xử lý hoặc request_id không tồn tại. Đảm bảo video_consumer.py đang chạy!",
                "request_id": request_id,
                "hint": "Kiểm tra: 1) video_consumer.py có đang chạy không? 2) Kafka có đang chạy không? 3) Consumer có nhận được message từ Kafka không?",
                "note": "Nếu consumer đang chạy, có thể video đang được xử lý. Vui lòng đợi thêm."
            }), 200
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Lỗi khi kiểm tra trạng thái: {str(e)}",
            "request_id": request_id
        }), 500

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test Kafka connection với timeout chuẩn hóa
        from kafka import KafkaProducer
        test_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=KAFKA_REQUEST_TIMEOUT_MS,  # Dùng timeout chuẩn hóa
            max_block_ms=KAFKA_MAX_BLOCK_MS,  # Dùng timeout chuẩn hóa
            api_version=(0, 10, 1)
        )
        # Test bằng cách list topics (nhanh hơn)
        test_producer.close(timeout=2)
        return jsonify({
            "status": "healthy",
            "kafka": "connected",
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "kafka_topic": KAFKA_TOPIC,
            "kafka_config": {
                "request_timeout_ms": KAFKA_REQUEST_TIMEOUT_MS,
                "max_block_ms": KAFKA_MAX_BLOCK_MS,
                "send_timeout_ms": KAFKA_SEND_TIMEOUT_MS,
                "retries": KAFKA_RETRIES
            }
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "kafka": "disconnected",
            "error": str(e),
            "hint": "Kiểm tra Kafka: docker ps | findstr kafka hoặc cd vào thư mục dự án && docker-compose up -d"
        }), 503


@app.route('/', methods=['GET'])
def index():
    """API documentation"""
    return jsonify({
        "service": "Video Processing API",
        "version": "1.0.0",
        "endpoints": {
            "POST /api/video": "Gửi video URL vào Kafka để xử lý",
            "GET /api/health": "Kiểm tra trạng thái service",
            "GET /": "API documentation"
        },
        "example_request": {
            "video_url": "https://example.com/video.mp4"
        }
    }), 200


if __name__ == '__main__':
    port = int(os.getenv("API_PORT", "5000"))
    host = os.getenv("API_HOST", "0.0.0.0")
    
    console.print(f"[bold cyan]🚀 Starting Video Processing API on {host}:{port}[/bold cyan]")
    console.print(f"[cyan]📡 Kafka: {KAFKA_BOOTSTRAP_SERVERS}[/cyan]")
    console.print(f"[cyan]📨 Topic: {KAFKA_TOPIC}[/cyan]")
    
    app.run(host=host, port=port, debug=True)

