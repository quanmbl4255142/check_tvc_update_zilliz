"""
Milvus Configuration
Supports both local development and cloud production (Zilliz Cloud)
"""

import os
from typing import Optional

# ============================================
# ENVIRONMENT DETECTION
# ============================================
# Set USE_CLOUD=False in environment to use local Docker
# Default: True (Zilliz Cloud - production ready)
USE_CLOUD = os.getenv("USE_CLOUD", "True").lower() in ("true", "1", "yes")

# ============================================
# CONNECTION SETTINGS
# ============================================
if USE_CLOUD:
    # === PRODUCTION: Zilliz Cloud ===
    # Your Zilliz Cloud cluster endpoint (serverless format)
    MILVUS_URI = os.getenv("MILVUS_URI", "https://in03-70f34bcdf458805.serverless.aws-eu-central-1.cloud.zilliz.com")
    MILVUS_TOKEN = os.getenv("MILVUS_TOKEN", "72105e0290198f6770a148d6c89073da064d96c026e3c8cc6ce0864b8e82332fecdcaaeca1d65f666998784b70b63635fb74eb90")
    MILVUS_SECURE = True
    
    # Extract host from URI for backward compatibility
    from urllib.parse import urlparse
    parsed = urlparse(MILVUS_URI)
    MILVUS_HOST = parsed.hostname
    MILVUS_PORT = parsed.port or 443
else:
    # === DEVELOPMENT: Local Docker ===
    MILVUS_HOST = os.getenv("MILVUS_HOST", "localhost")
    MILVUS_PORT = int(os.getenv("MILVUS_PORT", "19530"))
    MILVUS_TOKEN = None
    MILVUS_SECURE = False
    MILVUS_URI = None

# ============================================
# COLLECTION SETTINGS
# ============================================
COLLECTION_NAME = os.getenv("MILVUS_COLLECTION", "video_dedup")

# CLIP embedding dimension (openai/clip-vit-base-patch32)
EMBEDDING_DIM = 512

# Maximum length for text fields
MAX_URL_LENGTH = 1000
MAX_JOB_ID_LENGTH = 100
MAX_FRAME_TYPE_LENGTH = 20

# ============================================
# INDEX SETTINGS
# ============================================
# Index type: IVF_FLAT (balanced), HNSW (best accuracy), IVF_PQ (fastest)
INDEX_TYPE = os.getenv("MILVUS_INDEX_TYPE", "IVF_FLAT")

# Metric type: IP (Inner Product for normalized vectors), L2, COSINE
METRIC_TYPE = os.getenv("MILVUS_METRIC_TYPE", "IP")

# Index parameters based on index type
if INDEX_TYPE == "IVF_FLAT":
    INDEX_PARAMS = {
        "metric_type": METRIC_TYPE,
        "index_type": INDEX_TYPE,
        "params": {
            "nlist": 1024  # Number of cluster units (1024 good for 10k-100k vectors)
        }
    }
    SEARCH_PARAMS = {
        "metric_type": METRIC_TYPE,
        "params": {
            "nprobe": 16  # Number of clusters to search (higher = more accurate but slower)
        }
    }
elif INDEX_TYPE == "HNSW":
    INDEX_PARAMS = {
        "metric_type": METRIC_TYPE,
        "index_type": INDEX_TYPE,
        "params": {
            "M": 16,  # Max connections per layer
            "efConstruction": 256  # Build time/quality tradeoff
        }
    }
    SEARCH_PARAMS = {
        "metric_type": METRIC_TYPE,
        "params": {
            "ef": 64  # Search quality (higher = more accurate but slower)
        }
    }
elif INDEX_TYPE == "IVF_PQ":
    INDEX_PARAMS = {
        "metric_type": METRIC_TYPE,
        "index_type": INDEX_TYPE,
        "params": {
            "nlist": 1024,
            "m": 8  # PQ segments
        }
    }
    SEARCH_PARAMS = {
        "metric_type": METRIC_TYPE,
        "params": {
            "nprobe": 16
        }
    }
else:
    raise ValueError(f"Unsupported index type: {INDEX_TYPE}")

# ============================================
# SEARCH SETTINGS
# ============================================
# Default similarity threshold for detecting duplicates
DEFAULT_SIMILARITY_THRESHOLD = float(os.getenv("SIMILARITY_THRESHOLD", "0.995"))

# Top K results to retrieve per query
DEFAULT_TOP_K = int(os.getenv("TOP_K", "10"))

# Batch size for insert/search operations
# 500 = Cân bằng tốt - Upload mỗi 500 videos (khuyến nghị khi chạy song song 10 CMD)
# 1000 = Nhanh hơn nhưng rủi ro mất nhiều data nếu crash
# 100 = Rất an toàn nhưng chậm (nhiều requests)
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "500"))

# ============================================
# CONSISTENCY SETTINGS
# ============================================
# Consistency level: Strong, Bounded, Session, Eventually
CONSISTENCY_LEVEL = os.getenv("CONSISTENCY_LEVEL", "Strong")

# ============================================
# HELPER FUNCTIONS
# ============================================
def get_connection_params() -> dict:
    """Get connection parameters for pymilvus"""
    if USE_CLOUD:
        # For Zilliz Cloud serverless, use URI format
        return {
            "uri": MILVUS_URI,
            "token": MILVUS_TOKEN,
        }
    else:
        return {
            "host": MILVUS_HOST,
            "port": str(MILVUS_PORT),
        }


def print_config():
    """Print current configuration (for debugging)"""
    print("=" * 60)
    print("MILVUS CONFIGURATION")
    print("=" * 60)
    print(f"Mode: {'☁️  CLOUD (Zilliz)' if USE_CLOUD else '💻 LOCAL (Docker)'}")
    if USE_CLOUD:
        print(f"URI: {MILVUS_URI}")
        token_preview = MILVUS_TOKEN[:20] + "..." if MILVUS_TOKEN else "NOT SET"
        print(f"Token: {token_preview}")
    else:
        print(f"Host: {MILVUS_HOST}")
        print(f"Port: {MILVUS_PORT}")
    print(f"Collection: {COLLECTION_NAME}")
    print(f"Embedding Dim: {EMBEDDING_DIM}")
    print(f"Index Type: {INDEX_TYPE}")
    print(f"Metric Type: {METRIC_TYPE}")
    print(f"Similarity Threshold: {DEFAULT_SIMILARITY_THRESHOLD}")
    print(f"Batch Size: {BATCH_SIZE}")
    print("=" * 60)


if __name__ == "__main__":
    # Test configuration
    print_config()
    
    # Validate
    if USE_CLOUD and (not MILVUS_URI or not MILVUS_TOKEN):
        print("\n⚠️  WARNING: USE_CLOUD=True but missing credentials!")
        print("Set MILVUS_URI and MILVUS_TOKEN in environment or .env file")
    else:
        print("\n✅ Configuration looks good!")

