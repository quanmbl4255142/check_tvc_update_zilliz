"""
Tạo collection product_embeddings với schema đúng cho video processing
Schema: id, product_name, image_url, embedding (dim=512 cho CLIP)
"""

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
    print("ERROR: pymilvus not installed!")
    print("Please activate venv and install: pip install pymilvus")
    sys.exit(1)


def create_product_embeddings_collection():
    """Tạo collection product_embeddings với schema đúng"""
    collection_name = "product_embeddings"
    
    print("=" * 70)
    print("CREATING product_embeddings COLLECTION")
    print("=" * 70)
    print()
    
    # Skip print_config() to avoid Unicode issues on Windows
    # print_config()
    print()
    
    # Connect
    print("Connecting...")
    try:
        params = get_connection_params()
        connections.connect("default", **params)
        print("Connected!")
    except Exception as e:
        print(f"Connection failed: {e}")
        return False
    
    print()
    
    # Check if collection exists
    if utility.has_collection(collection_name):
        print(f"WARNING: Collection '{collection_name}' already exists!")
        response = input("Do you want to drop and recreate it? (yes/no): ").strip().lower()
        if response == "yes":
            print(f"Dropping existing collection...")
            utility.drop_collection(collection_name)
        else:
            print("Cancelled.")
            return False
    
    print(f"Creating collection '{collection_name}'...")
    
    # Schema cho video processing
    fields = [
        FieldSchema(
            name="id",
            dtype=DataType.INT64,
            is_primary=True,
            auto_id=True,
            description="Auto-generated primary key"
        ),
        FieldSchema(
            name="product_name",
            dtype=DataType.VARCHAR,
            max_length=MAX_JOB_ID_LENGTH,
            description="Product/Video identifier (e.g., url_0000)"
        ),
        FieldSchema(
            name="image_url",
            dtype=DataType.VARCHAR,
            max_length=MAX_URL_LENGTH,
            description="Video URL"
        ),
        FieldSchema(
            name="embedding",
            dtype=DataType.FLOAT_VECTOR,
            dim=EMBEDDING_DIM,  # 512 cho CLIP
            description="CLIP embedding from video frame (dim=512)"
        ),
    ]
    
    schema = CollectionSchema(
        fields,
        description="Video embeddings for product_embeddings collection (CLIP dim=512)"
    )
    
    # Create collection
    try:
        collection = Collection(
            name=collection_name,
            schema=schema,
            consistency_level=CONSISTENCY_LEVEL
        )
        print(f"Collection created!")
        
        # Create index
        print(f"Building index ({INDEX_PARAMS['index_type']})...")
        collection.create_index(
            field_name="embedding",
            index_params=INDEX_PARAMS
        )
        print(f"Index created!")
        
        # Load collection
        collection.load()
        print(f"Collection loaded and ready!")
        
        print()
        print("=" * 70)
        print(f"Collection '{collection_name}' created successfully!")
        print("=" * 70)
        print()
        print("Schema:")
        for field in schema.fields:
            field_type = field.dtype.name
            if hasattr(field, 'params') and 'dim' in field.params:
                field_type += f" (dim={field.params['dim']})"
            print(f"   - {field.name}: {field_type}")
        print()
        print("Collection is ready for video processing!")
        
        return True
        
    except Exception as e:
        print(f"Failed to create collection: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = create_product_embeddings_collection()
    sys.exit(0 if success else 1)

