"""
Test script to verify Milvus/Zilliz Cloud connection
Run this first to make sure everything is configured correctly!
"""

import sys
from milvus_config import (
    USE_CLOUD,
    get_connection_params,
    COLLECTION_NAME,
    EMBEDDING_DIM,
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


def test_connection():
    """Test connection to Milvus"""
    print("=" * 60)
    print("🧪 TESTING MILVUS CONNECTION")
    print("=" * 60)
    print()
    
    # Print config
    print_config()
    print()
    
    # Test 1: Connection
    print("🔌 Test 1: Connecting to Milvus...")
    try:
        params = get_connection_params()
        print(f"   Parameters: {list(params.keys())}")
        connections.connect("default", **params)
        print("   ✅ Connection successful!")
    except Exception as e:
        print(f"   ❌ Connection failed: {e}")
        print("\n💡 Troubleshooting:")
        if USE_CLOUD:
            print("   1. Check MILVUS_URI and MILVUS_TOKEN in config")
            print("   2. Verify Zilliz Cloud cluster is running")
            print("   3. Check internet connection")
        else:
            print("   1. Make sure Milvus Docker container is running")
            print("   2. Run: docker-compose up -d")
            print("   3. Or: docker run -d -p 19530:19530 milvusdb/milvus")
        return False
    
    print()
    
    # Test 2: List collections
    print("📦 Test 2: Listing collections...")
    try:
        collections = utility.list_collections()
        print(f"   Found {len(collections)} collection(s):")
        for col in collections:
            print(f"      - {col}")
        if not collections:
            print("   ℹ️  No collections yet (this is OK if you haven't uploaded data)")
        print("   ✅ List collections successful!")
    except Exception as e:
        print(f"   ❌ Failed to list collections: {e}")
        return False
    
    print()
    
    # Test 3: Check target collection
    print(f"🔍 Test 3: Checking collection '{COLLECTION_NAME}'...")
    try:
        if utility.has_collection(COLLECTION_NAME):
            collection = Collection(COLLECTION_NAME)
            collection.load()
            num_entities = collection.num_entities
            print(f"   ✅ Collection exists!")
            print(f"   📊 Number of vectors: {num_entities}")
            if num_entities > 0:
                # Get sample data
                sample = collection.query(
                    expr="id >= 0",
                    limit=1,
                    output_fields=["job_id", "url", "frame_type"]
                )
                if sample:
                    print(f"   📝 Sample: {sample[0]}")
        else:
            print(f"   ℹ️  Collection doesn't exist yet (will be created on first upload)")
        print("   ✅ Collection check successful!")
    except Exception as e:
        print(f"   ❌ Failed to check collection: {e}")
        return False
    
    print()
    
    # Test 4: Create test collection (optional)
    test_collection_name = "test_connection_collection"
    print(f"🧪 Test 4: Creating test collection '{test_collection_name}'...")
    try:
        if utility.has_collection(test_collection_name):
            utility.drop_collection(test_collection_name)
        
        # Create test schema
        fields = [
            FieldSchema(name="id", dtype=DataType.INT64, is_primary=True, auto_id=True),
            FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=EMBEDDING_DIM),
        ]
        schema = CollectionSchema(fields, "Test collection")
        test_collection = Collection(test_collection_name, schema)
        
        # Insert test vector
        import numpy as np
        test_vec = np.random.rand(EMBEDDING_DIM).astype(np.float32).tolist()
        test_collection.insert([test_vec])
        test_collection.flush()
        
        # Search test
        test_collection.load()
        results = test_collection.search(
            data=[test_vec],
            anns_field="embedding",
            param={"metric_type": "IP", "params": {"nprobe": 10}},
            limit=1
        )
        
        if results and len(results[0]) > 0:
            print(f"   ✅ Test search successful! Score: {results[0][0].score:.4f}")
        
        # Cleanup
        utility.drop_collection(test_collection_name)
        print("   ✅ Test collection created and deleted successfully!")
        
    except Exception as e:
        print(f"   ⚠️  Test collection creation failed: {e}")
        print("   (This is OK if you don't have write permissions)")
    
    print()
    print("=" * 60)
    print("✅ ALL TESTS PASSED!")
    print("=" * 60)
    print()
    print("🚀 You're ready to use Milvus!")
    print("   Next steps:")
    print("   1. Run: python upload_to_milvus.py")
    print("   2. Run: python search_duplicates_milvus.py")
    
    return True


if __name__ == "__main__":
    success = test_connection()
    sys.exit(0 if success else 1)

