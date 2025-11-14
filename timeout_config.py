"""
Timeout Configuration - Chuẩn hóa tất cả timeout trong hệ thống
"""
import os

# ============================================
# KAFKA TIMEOUTS
# ============================================
KAFKA_WAIT_READY_TIMEOUT = int(os.getenv("KAFKA_WAIT_READY_TIMEOUT", "60"))  # Đợi Kafka sẵn sàng: 60s (tăng từ 30s để đảm bảo Kafka có đủ thời gian khởi động)
KAFKA_REQUEST_TIMEOUT_MS = int(os.getenv("KAFKA_REQUEST_TIMEOUT_MS", "30000"))  # Request timeout: 30s (tăng từ 10s)
KAFKA_MAX_BLOCK_MS = int(os.getenv("KAFKA_MAX_BLOCK_MS", "60000"))  # Max block khi fetch metadata: 60s (tăng từ 30s để đảm bảo có đủ thời gian, đặc biệt khi auto-create topic)
KAFKA_SEND_TIMEOUT_MS = int(os.getenv("KAFKA_SEND_TIMEOUT_MS", "30000"))  # Timeout khi gửi message: 30s (tăng từ 10s để match với metadata timeout)
KAFKA_RETRIES = int(os.getenv("KAFKA_RETRIES", "3"))  # Số lần retry: 3
KAFKA_RETRY_DELAY = int(os.getenv("KAFKA_RETRY_DELAY", "2"))  # Delay giữa các retry: 2s

# Kafka Consumer timeouts
KAFKA_CONSUMER_TIMEOUT_MS = int(os.getenv("KAFKA_CONSUMER_TIMEOUT_MS", "2000"))  # Consumer poll timeout: 2s
KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "10"))  # Max messages per poll: 10
KAFKA_MAX_POLL_INTERVAL_MS = int(os.getenv("KAFKA_MAX_POLL_INTERVAL_MS", "300000"))  # Max poll interval: 5 phút
KAFKA_SESSION_TIMEOUT_MS = int(os.getenv("KAFKA_SESSION_TIMEOUT_MS", "30000"))  # Session timeout: 30s
KAFKA_HEARTBEAT_INTERVAL_MS = int(os.getenv("KAFKA_HEARTBEAT_INTERVAL_MS", "10000"))  # Heartbeat: 10s

# ============================================
# REDIS TIMEOUTS
# ============================================
REDIS_SOCKET_CONNECT_TIMEOUT = int(os.getenv("REDIS_SOCKET_CONNECT_TIMEOUT", "5"))  # Connection timeout: 5s
REDIS_SOCKET_TIMEOUT = int(os.getenv("REDIS_SOCKET_TIMEOUT", "5"))  # Socket timeout: 5s
REDIS_HEALTH_CHECK_INTERVAL = int(os.getenv("REDIS_HEALTH_CHECK_INTERVAL", "30"))  # Health check: 30s
REDIS_CONNECTION_RETRIES = int(os.getenv("REDIS_CONNECTION_RETRIES", "5"))  # Max retries: 5
REDIS_RETRY_DELAY = float(os.getenv("REDIS_RETRY_DELAY", "2"))  # Initial retry delay: 2s

# ============================================
# VIDEO PROCESSING TIMEOUTS
# ============================================
VIDEO_EXTRACT_FRAME_TIMEOUT = int(os.getenv("VIDEO_EXTRACT_FRAME_TIMEOUT", "30"))  # Video frame extraction: 30s
VIDEO_EXTRACT_FRAME_RETRIES = int(os.getenv("VIDEO_EXTRACT_FRAME_RETRIES", "2"))  # Max retries: 2
VIDEO_PROCESSING_TOTAL_TIMEOUT = int(os.getenv("VIDEO_PROCESSING_TOTAL_TIMEOUT", "300"))  # Total processing: 5 phút

# ============================================
# API TIMEOUTS
# ============================================
RESULT_WAIT_TIMEOUT = int(os.getenv("RESULT_WAIT_TIMEOUT", "30"))  # Đợi kết quả từ consumer: 30s
RESULT_POLL_INTERVAL = float(os.getenv("RESULT_POLL_INTERVAL", "0.5"))  # Poll interval: 0.5s
API_REQUEST_TIMEOUT = int(os.getenv("API_REQUEST_TIMEOUT", "15"))  # API request timeout: 15s

# ============================================
# MILVUS TIMEOUTS
# ============================================
MILVUS_INSERT_RETRIES = int(os.getenv("MILVUS_INSERT_RETRIES", "3"))  # Insert retries: 3
MILVUS_INSERT_RETRY_DELAY = int(os.getenv("MILVUS_INSERT_RETRY_DELAY", "1"))  # Retry delay: 1s

# ============================================
# CONSUMER TIMEOUTS
# ============================================
CONSUMER_ERROR_RETRY_DELAY = int(os.getenv("CONSUMER_ERROR_RETRY_DELAY", "5"))  # Error retry delay: 5s
CONSUMER_MAX_ERROR_WAIT = int(os.getenv("CONSUMER_MAX_ERROR_WAIT", "60"))  # Max error wait: 60s

