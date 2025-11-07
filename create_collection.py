"""
Tạo collection mới trong Milvus/Zilliz
Có thể tạo collection rỗng hoặc tự động tạo khi upload dữ liệu
"""

import argparse
import sys
from milvus_config import (
    get_connection_params,
    EMBEDDING_DIM,
    MAX_URL_LENGTH,
    MAX_JOB_ID_LENGTH,
    INDEX_PARAMS,
    CONSISTENCY_LEVEL,
    print_config,
)

try:
    from pymilvus import (
        connections,
        utility,
        Collection,
        CollectionSchema,
        FieldSchema,
        DataType,
    )
except ImportError:
    print("❌ ERROR: pymilvus not installed!")
    print("Install it with: pip install pymilvus")
    sys.exit(1)


def create_collection(collection_name: str, schema_type: str = "video_dedup"):
    """
    Tạo collection mới
    
    Args:
        collection_name: Tên collection
        schema_type: Loại schema
            - "video_dedup": Schema cho video deduplication (1 vector per video)
            - "video_frames": Schema cho multiple frames per video
            - "aggregated": Schema cho aggregated vectors (3 frames → 1 vector)
    """
    print("=" * 70)
    print("🆕 CREATING NEW COLLECTION")
    print("=" * 70)
    print()
    
    # Print config
    print_config()
    print()
    
    # Connect
    print("🔌 Connecting...")
    try:
        params = get_connection_params()
        connections.connect("default", **params)
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False
    
    print()
    
    # Check if collection exists
    if utility.has_collection(collection_name):
        print(f"⚠️  Collection '{collection_name}' already exists!")
        response = input("Do you want to drop and recreate it? (yes/no): ").strip().lower()
        if response == "yes":
            print(f"🗑️  Dropping existing collection...")
            utility.drop_collection(collection_name)
        else:
            print("❌ Cancelled. Collection already exists.")
            return False
    
    print(f"🆕 Creating collection '{collection_name}' with schema type: {schema_type}...")
    
    # Define schema based on type
    if schema_type == "video_dedup":
        # Schema: 1 vector per video (direct upload)
        fields = [
            FieldSchema(
                name="id",
                dtype=DataType.INT64,
                is_primary=True,
                auto_id=True,
                description="Auto-generated primary key"
            ),
            FieldSchema(
                name="url",
                dtype=DataType.VARCHAR,
                max_length=MAX_URL_LENGTH,
                description="Source video URL"
            ),
            FieldSchema(
                name="job_id",
                dtype=DataType.VARCHAR,
                max_length=MAX_JOB_ID_LENGTH,
                description="Job ID (url_XXXX)"
            ),
            FieldSchema(
                name="embedding",
                dtype=DataType.FLOAT_VECTOR,
                dim=EMBEDDING_DIM,
                description="CLIP embedding from first frame"
            ),
        ]
        description = "Video embeddings (1 vector per video, direct upload)"
        
    elif schema_type == "video_frames":
        # Schema: Multiple frames per video
        fields = [
            FieldSchema(
                name="id",
                dtype=DataType.INT64,
                is_primary=True,
                auto_id=True,
                description="Auto-generated primary key"
            ),
            FieldSchema(
                name="url",
                dtype=DataType.VARCHAR,
                max_length=MAX_URL_LENGTH,
                description="Source video URL"
            ),
            FieldSchema(
                name="job_id",
                dtype=DataType.VARCHAR,
                max_length=MAX_JOB_ID_LENGTH,
                description="Job folder ID (e.g., url_0000)"
            ),
            FieldSchema(
                name="frame_type",
                dtype=DataType.VARCHAR,
                max_length=20,
                description="Frame type: first, middle, or last"
            ),
            FieldSchema(
                name="embedding",
                dtype=DataType.FLOAT_VECTOR,
                dim=EMBEDDING_DIM,
                description="CLIP embedding vector (L2-normalized)"
            ),
        ]
        description = "Video frame embeddings for deduplication"
        
    elif schema_type == "aggregated":
        # Schema: Aggregated vectors (3 frames → 1 vector)
        fields = [
            FieldSchema(
                name="id",
                dtype=DataType.INT64,
                is_primary=True,
                auto_id=True,
                description="Auto-generated primary key"
            ),
            FieldSchema(
                name="url",
                dtype=DataType.VARCHAR,
                max_length=MAX_URL_LENGTH,
                description="Source video URL"
            ),
            FieldSchema(
                name="job_id",
                dtype=DataType.VARCHAR,
                max_length=MAX_JOB_ID_LENGTH,
                description="Job folder ID (e.g., url_0000)"
            ),
            FieldSchema(
                name="embedding",
                dtype=DataType.FLOAT_VECTOR,
                dim=EMBEDDING_DIM,
                description="Aggregated CLIP embedding from multiple frames"
            ),
        ]
        description = "Aggregated video embeddings (1 vector per video)"
        
    else:
        print(f"❌ Unknown schema type: {schema_type}")
        print("Available types: video_dedup, video_frames, aggregated")
        return False
    
    # Create schema
    schema = CollectionSchema(
        fields,
        description=description
    )
    
    # Create collection
    try:
        collection = Collection(
            name=collection_name,
            schema=schema,
            consistency_level=CONSISTENCY_LEVEL
        )
        print(f"✅ Collection created!")
        
        # Create index
        print(f"🔨 Building index ({INDEX_PARAMS['index_type']})...")
        collection.create_index(
            field_name="embedding",
            index_params=INDEX_PARAMS
        )
        print(f"✅ Index created!")
        
        # Load collection
        collection.load()
        print(f"✅ Collection loaded and ready!")
        
        print()
        print("=" * 70)
        print(f"🎉 Collection '{collection_name}' created successfully!")
        print("=" * 70)
        print()
        print("📋 Schema:")
        for field in schema.fields:
            field_type = field.dtype.name
            if hasattr(field, 'params') and 'dim' in field.params:
                field_type += f" (dim={field.params['dim']})"
            print(f"   - {field.name}: {field_type}")
        print()
        print("💡 Next steps:")
        print(f"   1. Upload data: python direct_upload_to_zilliz.py --collection {collection_name}")
        print(f"   2. List collections: python list_collections.py")
        
        return True
        
    except Exception as e:
        print(f"❌ Failed to create collection: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Create a new Milvus collection"
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Collection name (required)"
    )
    parser.add_argument(
        "--schema",
        choices=["video_dedup", "video_frames", "aggregated"],
        default="video_dedup",
        help="Schema type (default: video_dedup)"
    )
    parser.add_argument(
        "--config-only",
        action="store_true",
        help="Only print configuration"
    )
    
    args = parser.parse_args()
    
    if args.config_only:
        print_config()
        return
    
    success = create_collection(args.collection, args.schema)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

