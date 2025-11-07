"""
List và xem chi tiết các collections trong Milvus/Zilliz
"""

import sys
from milvus_config import (
    CONNECTION_MODE,
    get_connection_params,
    print_config,
)

try:
    from pymilvus import (
        connections,
        utility,
        Collection,
    )
except ImportError:
    print("❌ ERROR: pymilvus not installed!")
    print("Install it with: pip install pymilvus")
    sys.exit(1)


def list_collections_detail():
    """List tất cả collections và thông tin chi tiết"""
    print("=" * 70)
    print("📦 LISTING ALL COLLECTIONS")
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
        return
    
    print()
    
    # List collections
    try:
        collections = utility.list_collections()
        print(f"📊 Found {len(collections)} collection(s):\n")
        
        if not collections:
            print("   ℹ️  No collections found")
            return
        
        # Show details for each collection
        for idx, col_name in enumerate(collections, 1):
            print("=" * 70)
            print(f"📦 Collection {idx}: {col_name}")
            print("=" * 70)
            
            try:
                collection = Collection(col_name)
                collection.load()
                
                # Basic info
                num_entities = collection.num_entities
                print(f"   📊 Number of vectors: {num_entities:,}")
                
                # Schema info
                schema = collection.schema
                print(f"\n   📋 Schema:")
                print(f"      Fields: {len(schema.fields)}")
                for field in schema.fields:
                    field_type = field.dtype.name
                    if hasattr(field, 'params') and 'dim' in field.params:
                        field_type += f" (dim={field.params['dim']})"
                    print(f"         - {field.name}: {field_type}")
                
                # Index info
                indexes = collection.indexes
                if indexes:
                    print(f"\n   🔍 Indexes:")
                    for idx_info in indexes:
                        idx_type = idx_info.params.get('index_type', 'Unknown')
                        metric = idx_info.params.get('metric_type', 'Unknown')
                        print(f"         - Type: {idx_type}, Metric: {metric}")
                
                # Sample data (if has data)
                if num_entities > 0:
                    print(f"\n   📝 Sample data (first 3 records):")
                    try:
                        # Try to get sample with common fields
                        sample_fields = ["id"]
                        # Check available fields
                        available_fields = [f.name for f in schema.fields]
                        
                        # Add common fields if available
                        for field_name in ["job_id", "url", "frame_type"]:
                            if field_name in available_fields:
                                sample_fields.append(field_name)
                        
                        # Limit to max 5 fields for readability
                        sample_fields = sample_fields[:5]
                        
                        sample = collection.query(
                            expr="id >= 0",
                            limit=3,
                            output_fields=sample_fields
                        )
                        
                        for i, record in enumerate(sample, 1):
                            print(f"         {i}. {record}")
                    except Exception as e:
                        print(f"         ⚠️  Could not fetch sample: {e}")
                
                print()
                
            except Exception as e:
                print(f"   ❌ Error loading collection: {e}")
                print()
        
        print("=" * 70)
        print("✅ Done!")
        print("=" * 70)
        
    except Exception as e:
        print(f"❌ Error listing collections: {e}")


if __name__ == "__main__":
    list_collections_detail()

