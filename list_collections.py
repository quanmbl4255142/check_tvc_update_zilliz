"""
List all collections in Milvus/Zilliz
"""

import sys
from pymilvus import connections, utility
from milvus_config import get_connection_params, print_config

def list_all_collections():
    """List all collections in Milvus"""
    print_config()
    print()
    
    print("🔌 Connecting to Milvus...")
    try:
        params = get_connection_params()
        connections.connect("default", **params)
        print("✅ Connected!")
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        sys.exit(1)
    
    print("\n📋 Listing all collections...")
    try:
        collections = utility.list_collections()
        
        if not collections:
            print("   ⚠️  No collections found!")
            print("   💡 You need to upload data first using:")
            print("      - direct_upload_to_zilliz.py")
            print("      - upload_aggregated_to_milvus.py")
            print("      - upload_to_milvus.py")
        else:
            print(f"   ✅ Found {len(collections)} collection(s):\n")
            
            for collection_name in collections:
                try:
                    from pymilvus import Collection
                    collection = Collection(collection_name)
                    collection.load()
                    num_entities = collection.num_entities
                    
                    # Get schema info
                    schema = collection.schema
                    fields = [field.name for field in schema.fields]
                    
                    print(f"   📦 Collection: {collection_name}")
                    print(f"      - Entities: {num_entities:,} vectors")
                    print(f"      - Fields: {', '.join(fields)}")
                    print()
                except Exception as e:
                    print(f"   ⚠️  Error loading collection '{collection_name}': {e}")
                    print()
            
            print("💡 To use a collection, specify it with --collection flag:")
            print(f"   python search_duplicates_aggregated.py --collection {collections[0]}")
            
    except Exception as e:
        print(f"❌ Error listing collections: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    list_all_collections()
