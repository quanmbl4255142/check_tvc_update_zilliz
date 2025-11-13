"""
Flask API endpoint để nhận video URL và gửi vào Kafka
Endpoint: POST /api/video
"""

import os
import json
import uuid
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

# Initialize Kafka producer
producer = None

def ensure_topic_exists():
    """Đảm bảo topic tồn tại"""
    try:
        admin_client = KafkaAdminClient(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            client_id='video_api_admin',
            request_timeout_ms=5000
        )
        # Kiểm tra topic có tồn tại không
        try:
            topics = admin_client.list_topics(timeout_ms=5000)
            # list_topics() trả về set hoặc list
            if isinstance(topics, set):
                topic_exists = KAFKA_TOPIC in topics
            else:
                topic_exists = KAFKA_TOPIC in list(topics)
        except:
            topic_exists = False
        
        if not topic_exists:
            # Tạo topic mới
            topic = NewTopic(
                name=KAFKA_TOPIC,
                num_partitions=1,
                replication_factor=1
            )
            try:
                admin_client.create_topics([topic], timeout_ms=5000)
                console.print(f"[green]✅ Created topic: {KAFKA_TOPIC}[/green]")
            except TopicAlreadyExistsError:
                console.print(f"[dim]Topic {KAFKA_TOPIC} already exists[/dim]")
        else:
            console.print(f"[dim]Topic {KAFKA_TOPIC} already exists[/dim]")
        admin_client.close()
    except Exception as e:
        # Nếu không tạo được, Kafka sẽ auto-create khi có message đầu tiên
        console.print(f"[yellow]⚠️  Could not ensure topic exists: {e}[/yellow]")
        console.print(f"[dim]   Topic will be auto-created on first message[/dim]")

def get_kafka_producer():
    """Lazy initialization of Kafka producer với retry logic"""
    global producer
    if producer is None:
        import time
        max_retries = 3
        retry_delay = 2  # seconds
        
        for attempt in range(max_retries):
            try:
                # Bỏ ensure_topic_exists() vì có thể gây lỗi
                # Kafka sẽ tự tạo topic với KAFKA_AUTO_CREATE_TOPICS_ENABLE=true
                
                producer = KafkaProducer(
                    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    key_serializer=lambda k: k.encode('utf-8') if k else None,
                    acks=1,  # Đợi leader xác nhận - đảm bảo message được gửi
                    retries=3,  # Retry 3 lần nếu lỗi
                    max_in_flight_requests_per_connection=1,
                    request_timeout_ms=10000,  # Tăng timeout lên 10 giây
                    metadata_max_age_ms=300000,
                    linger_ms=0,  # Gửi ngay
                    batch_size=0,  # Không batch
                    max_block_ms=10000,  # Tăng block time lên 10 giây
                    api_version=(0, 10, 1)  # Chỉ định API version để tránh auto-detect timeout
                )
                console.print(f"[green]✅ Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}[/green]")
                break  # Thành công, thoát khỏi retry loop
            except Exception as e:
                if attempt < max_retries - 1:
                    console.print(f"[yellow]⚠️  Attempt {attempt + 1}/{max_retries} failed: {e}[/yellow]")
                    console.print(f"[dim]   Retrying in {retry_delay} seconds...[/dim]")
                    time.sleep(retry_delay)
                else:
                    console.print(f"[red]❌ Failed to connect to Kafka after {max_retries} attempts: {e}[/red]")
                    console.print(f"[yellow]💡 Đảm bảo Kafka đang chạy: docker-compose up -d trong thư mục kafka[/yellow]")
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
        
        # Send to Kafka
        kafka_producer = get_kafka_producer()
        
        try:
            console.print(f"[cyan]📤 Sending video to Kafka: {video_url[:60]}...[/cyan]")
            console.print(f"[dim]Request ID: {request_id}[/dim]")
            
            # Gửi message và đợi confirmation
            future = kafka_producer.send(
                KAFKA_TOPIC,
                value=message,
                key=request_id  # Use request_id as key for partitioning
            )
            
            # Đợi confirmation từ Kafka (với timeout)
            record_metadata = future.get(timeout=10)  # Đợi tối đa 10 giây
            
            # Flush để đảm bảo message được gửi ngay
            kafka_producer.flush(timeout=5)
            
            console.print(f"[green]✅ Video sent to Kafka successfully![/green]")
            console.print(f"[dim]   Topic: {record_metadata.topic}[/dim]")
            console.print(f"[dim]   Partition: {record_metadata.partition}[/dim]")
            console.print(f"[dim]   Offset: {record_metadata.offset}[/dim]")
            
            return jsonify({
                "status": "success",
                "message": "Video đã được gửi vào Kafka để xử lý",
                "request_id": request_id,
                "video_url": video_url,
                "timestamp": timestamp,
                "kafka_topic": record_metadata.topic,
                "kafka_partition": record_metadata.partition,
                "kafka_offset": record_metadata.offset
            }), 200
            
        except Exception as e:
            console.print(f"[red]❌ Failed to send to Kafka: {e}[/red]")
            import traceback
            traceback.print_exc()
            error_msg = str(e)
            return jsonify({
                "status": "error",
                "message": f"Lỗi khi gửi vào Kafka: {error_msg}",
                "hint": "Kiểm tra Kafka đang chạy: docker ps | findstr kafka"
            }), 500
            
    except Exception as e:
        console.print(f"[red]❌ API Error: {e}[/red]")
        return jsonify({
            "status": "error",
            "message": f"Lỗi server: {str(e)}"
        }), 500


@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint"""
    try:
        # Test Kafka connection với timeout dài hơn
        from kafka import KafkaProducer
        test_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=10000,  # 10 seconds timeout
            max_block_ms=10000,
            api_version=(0, 10, 1)
        )
        # Test bằng cách list topics (nhanh hơn)
        test_producer.close(timeout=2)
        return jsonify({
            "status": "healthy",
            "kafka": "connected",
            "kafka_bootstrap_servers": KAFKA_BOOTSTRAP_SERVERS,
            "kafka_topic": KAFKA_TOPIC
        }), 200
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "kafka": "disconnected",
            "error": str(e),
            "hint": "Kiểm tra Kafka: docker ps | findstr kafka hoặc cd D:\\lặt vặt\\đi làm\\kafka && docker-compose up -d"
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

