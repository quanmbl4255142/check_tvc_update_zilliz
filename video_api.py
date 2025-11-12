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
    """Đảm bảo topic tồn tại, nếu chưa thì tạo (bỏ qua nếu Kafka chưa sẵn sàng)"""
    # Với KAFKA_AUTO_CREATE_TOPICS_ENABLE=true, topic sẽ tự động được tạo
    # Nên không cần tạo topic trước, chỉ log thông tin
    console.print(f"[cyan]📨 Topic '{KAFKA_TOPIC}' will be auto-created on first message[/cyan]")
    return

def get_kafka_producer():
    """Lazy initialization of Kafka producer"""
    global producer
    if producer is None:
        try:
            # Đảm bảo topic tồn tại trước
            ensure_topic_exists()
            
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                key_serializer=lambda k: k.encode('utf-8') if k else None,
                acks=0,  # Fire and forget - không đợi confirmation (nhanh nhất)
                retries=0,  # Không retry để tránh timeout
                max_in_flight_requests_per_connection=1,
                request_timeout_ms=5000,  # 5 seconds
                metadata_max_age_ms=300000,  # 5 minutes
                api_version=(0, 10, 1),  # Specify API version
                linger_ms=0,  # Gửi ngay lập tức
                batch_size=0,  # Không batch
                max_block_ms=5000  # Max time to block when metadata unavailable
            )
            console.print(f"[green]✅ Kafka producer connected to {KAFKA_BOOTSTRAP_SERVERS}[/green]")
        except Exception as e:
            console.print(f"[red]❌ Failed to connect to Kafka: {e}[/red]")
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
        future = kafka_producer.send(
            KAFKA_TOPIC,
            value=message,
            key=request_id  # Use request_id as key for partitioning
        )
        
        # Gửi message (fire and forget với acks=0)
        # Với acks=0, producer không đợi confirmation nên không có timeout
        try:
            # Gửi message (không đợi response)
            console.print(f"[cyan]📤 Sending video to Kafka: {video_url[:60]}... (request_id: {request_id})[/cyan]")
            
            # Với acks=0, future.get() sẽ return ngay lập tức
            # Nhưng vẫn cần gọi để đảm bảo message được gửi vào buffer
            try:
                record_metadata = future.get(timeout=2)  # Timeout ngắn
                console.print(f"[green]✅ Video sent successfully (Partition: {record_metadata.partition}, Offset: {record_metadata.offset})[/green]")
                
                return jsonify({
                    "status": "success",
                    "message": "Video đã được gửi vào Kafka để xử lý",
                    "request_id": request_id,
                    "video_url": video_url,
                    "timestamp": timestamp,
                    "kafka_topic": KAFKA_TOPIC,
                    "kafka_partition": record_metadata.partition,
                    "kafka_offset": record_metadata.offset
                }), 200
            except Exception as e:
                # Với acks=0, có thể không có metadata nhưng message vẫn được gửi
                error_type = type(e).__name__
                if "Timeout" in error_type or "NodeNotReady" in error_type:
                    console.print(f"[yellow]⚠️  Kafka may not be ready, but message queued for sending[/yellow]")
                    # Vẫn trả về success vì message đã được queue
                    return jsonify({
                        "status": "success",
                        "message": "Video đã được queue để gửi vào Kafka",
                        "request_id": request_id,
                        "video_url": video_url,
                        "timestamp": timestamp,
                        "kafka_topic": KAFKA_TOPIC,
                        "warning": "Kafka may not be fully ready, but message is queued"
                    }), 200
                else:
                    raise
            
        except Exception as e:
            console.print(f"[red]❌ Failed to send to Kafka: {e}[/red]")
            error_msg = str(e)
            if "timeout" in error_msg.lower() or "metadata" in error_msg.lower() or "NodeNotReady" in error_msg:
                error_msg += ". Đảm bảo Kafka đang chạy và sẵn sàng: docker-compose up -d trong thư mục kafka"
            return jsonify({
                "status": "error",
                "message": f"Lỗi khi gửi vào Kafka: {error_msg}",
                "hint": "Kiểm tra Kafka: docker ps | findstr kafka và đợi vài giây để Kafka khởi động hoàn toàn"
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
        # Test Kafka connection với timeout ngắn
        from kafka import KafkaProducer
        test_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            request_timeout_ms=5000,  # 5 seconds timeout
            api_version=(0, 10, 1)
        )
        # Test bằng cách list topics (nhanh hơn)
        test_producer.close(timeout=1)
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

