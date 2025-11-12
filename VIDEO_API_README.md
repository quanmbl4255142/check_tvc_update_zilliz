# 🎬 Video Processing API - Kafka + Redis + Milvus

Hệ thống xử lý video tự động: **Video URL → Kafka → Redis Cache → Check TVC → Milvus**

## 📋 Tổng quan

Luồng xử lý:
1. **API Endpoint** nhận video URL
2. Gửi vào **Kafka** (topic: `video_processing`)
3. **Kafka Consumer** xử lý:
   - Kiểm tra **Redis cache**
   - Nếu cache miss → Embedding & Search **Milvus**
   - Nếu TVC mới → Thêm vào Milvus
   - Nếu TVC cũ → Lấy unique_id từ Milvus
   - Lưu vào **Redis cache**
4. Trả về kết quả: video có tồn tại chưa, đã thêm hay chưa, stats tổng hợp

## 🚀 Cài đặt

### 1. Cài đặt dependencies

```bash
pip install -r requirements.txt
```

### 2. Khởi động Kafka và Redis

```bash
cd D:\lặt vặt\đi làm\kafka
docker-compose up -d
```

Kiểm tra services:
- **Kafka**: `localhost:9092`
- **Kafka UI**: `http://localhost:8080`
- **Redis**: `localhost:6379`

### 3. Cấu hình môi trường

Tạo file `.env` (tùy chọn) hoặc set environment variables:

```bash
# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
KAFKA_TOPIC=video_processing
KAFKA_GROUP_ID=video_processor_group

# Redis
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=0
REDIS_PASSWORD=

# Milvus (xem milvus_config.py)
MILVUS_MODE=zilliz  # hoặc milvus, local
MILVUS_URI=...
MILVUS_TOKEN=...
MILVUS_COLLECTION=video_dedup_aggregated

# API
API_HOST=0.0.0.0
API_PORT=5000
```

## 🎯 Sử dụng

### 1. Khởi động tất cả services (Khuyến nghị)

Sử dụng script tự động:

**Windows:**
```bash
start_services.bat
```

**PowerShell:**
```powershell
.\start_services.ps1
```

Script sẽ tự động khởi động:
- Kafka & Redis (Docker)
- API Server (port 5000)
- Kafka Consumer
- Web UI (port 5001)

### 2. Hoặc khởi động thủ công

#### 2.1. Khởi động Kafka và Redis

```bash
cd D:\lặt vặt\đi làm\kafka
docker-compose up -d
```

#### 2.2. Khởi động API Server

```bash
python video_api.py
```

API sẽ chạy tại: `http://localhost:5000`

#### 2.3. Khởi động Kafka Consumer

Mở terminal mới:

```bash
python video_consumer.py
```

Consumer sẽ tự động xử lý messages từ Kafka.

#### 2.4. Khởi động Web UI

Mở terminal mới:

```bash
python video_web_ui.py
```

Web UI sẽ chạy tại: `http://localhost:5001`

### 3. Truy cập Web UI

Mở trình duyệt và truy cập: **http://localhost:5001**

Web UI cung cấp:
- ✅ Form nhập video URL
- ✅ Hiển thị kết quả real-time
- ✅ Thống kê tổng hợp
- ✅ Lịch sử video đã xử lý
- ✅ Links đến Redis UI và Kafka UI

### 4. Gửi video URL qua Web UI

1. Mở http://localhost:5001
2. Nhập video URL vào form
3. Click "📤 Gửi Video"
4. Xem kết quả và stats

### 5. Hoặc gửi video URL qua API

#### Sử dụng curl:

```bash
curl -X POST http://localhost:5000/api/video \
  -H "Content-Type: application/json" \
  -d '{"video_url": "https://example.com/video.mp4"}'
```

#### Sử dụng Python:

```python
import requests

response = requests.post(
    "http://localhost:5000/api/video",
    json={"video_url": "https://example.com/video.mp4"}
)

print(response.json())
```

#### Response mẫu:

**Cache Hit (video đã xử lý trước đó):**
```json
{
  "status": "success",
  "request_id": "uuid-here",
  "video_url": "https://example.com/video.mp4",
  "cache_hit": true,
  "unique_id": "url_0123",
  "is_new": false,
  "added_at": "2024-01-01T00:00:00",
  "message": "Video đã tồn tại trong cache",
  "stats": {
    "total_before": 100,
    "total_after": 100,
    "total_added": 50,
    "total_duplicates": 50
  }
}
```

**Cache Miss - TVC MỚI (đã thêm vào Milvus):**
```json
{
  "status": "success",
  "request_id": "uuid-here",
  "video_url": "https://example.com/video.mp4",
  "cache_hit": false,
  "unique_id": "url_0124",
  "is_new": true,
  "similarity": 0.0,
  "added_at": "2024-01-01T00:00:00",
  "message": "Video mới đã được thêm vào Milvus",
  "stats_before": 100,
  "stats_after": 101,
  "stats": {
    "total_before": 100,
    "total_after": 101,
    "total_added": 51,
    "total_duplicates": 50
  }
}
```

**Cache Miss - TVC CŨ (đã tồn tại trong Milvus):**
```json
{
  "status": "success",
  "request_id": "uuid-here",
  "video_url": "https://example.com/video.mp4",
  "cache_hit": false,
  "unique_id": "url_0100",
  "is_new": false,
  "similarity": 0.9985,
  "added_at": "2024-01-01T00:00:00",
  "message": "Video đã tồn tại (similarity: 0.9985)",
  "duplicate_url": "https://example.com/duplicate-video.mp4",
  "stats_before": 100,
  "stats_after": 100,
  "stats": {
    "total_before": 100,
    "total_after": 100,
    "total_added": 50,
    "total_duplicates": 51
  }
}
```

## 📊 API Endpoints

### `POST /api/video`
Gửi video URL vào Kafka để xử lý

**Request:**
```json
{
  "video_url": "https://example.com/video.mp4"
}
```

**Response:**
- `200`: Success - Video đã được gửi vào Kafka
- `400`: Bad Request - Thiếu video_url
- `500`: Server Error

### `GET /api/health`
Kiểm tra trạng thái service

**Response:**
```json
{
  "status": "healthy",
  "kafka": "connected",
  "kafka_bootstrap_servers": "localhost:9092",
  "kafka_topic": "video_processing"
}
```

### `GET /`
API documentation

## 🔍 Kiểm tra kết quả

### 1. Web UI (Khuyến nghị)
Truy cập: **http://localhost:5001**
- Xem kết quả real-time
- Thống kê tổng hợp
- Lịch sử video đã xử lý

### 2. Xem logs trong Consumer
Consumer sẽ in ra:
- Video URL đang xử lý
- Cache hit/miss
- Unique ID
- Stats trước và sau khi thêm
- Tổng số videos đã thêm/duplicates

### 3. Redis UI
Truy cập: **http://localhost:8081**
- Username: `admin`
- Password: `admin`
- Xem tất cả keys trong Redis
- Xem cache data cho từng video

### 4. Kiểm tra Redis bằng CLI
```bash
redis-cli
> KEYS video:*
> GET video:<hash>
> KEYS stats:*
```

### 5. Kafka UI
Truy cập: **http://localhost:8080**
- Xem messages trong topic `video_processing`
- Xem consumer group `video_processor_group`
- Monitor Kafka performance

### 6. Kiểm tra Milvus
Sử dụng `list_collections.py` hoặc `test_milvus_connection.py` để xem số lượng videos trong collection.

## ⚙️ Cấu hình nâng cao

### Thay đổi similarity threshold
Trong `video_service.py`:
```python
result = service.check_and_add_video(
    video_url,
    similarity_threshold=0.995  # Default: 0.995
)
```

### Thay đổi cache TTL
Trong `video_consumer.py`:
```python
CACHE_TTL = 604800  # 7 days (seconds)
```

### Thay đổi collection name
Set environment variable:
```bash
MILVUS_COLLECTION=video_dedup_aggregated
```

## 🐛 Troubleshooting

### Kafka connection failed
- Kiểm tra Kafka đang chạy: `docker ps`
- Kiểm tra port 9092: `netstat -an | findstr 9092`
- Xem logs: `docker logs kafka`

### Redis connection failed
- Kiểm tra Redis đang chạy: `docker ps`
- Kiểm tra port 6379: `netstat -an | findstr 6379`
- Xem logs: `docker logs redis`

### Milvus connection failed
- Kiểm tra `milvus_config.py`
- Test connection: `python test_milvus_connection.py`

### Video embedding failed
- Kiểm tra video URL có hợp lệ không
- Kiểm tra video có thể stream được không
- Xem logs trong consumer để biết lỗi cụ thể

## 📝 Lưu ý

1. **Video phải unique**: Hệ thống đảm bảo không có video trùng lặp trong Milvus
2. **Redis cache**: Lưu thông tin video đã xử lý để tránh xử lý lại
3. **Stats**: Thống kê được lưu trong Redis, reset khi restart Redis
4. **Collection**: Đảm bảo collection `video_dedup_aggregated` đã tồn tại trong Milvus
5. **Redis UI**: Truy cập http://localhost:8081 với username/password: `admin/admin`
6. **Web UI**: Chạy trên port 5001, không conflict với API server (port 5000)

## 🎉 Hoàn tất!

Hệ thống đã sẵn sàng xử lý video. Gửi video URL qua API và xem kết quả trong consumer logs!

