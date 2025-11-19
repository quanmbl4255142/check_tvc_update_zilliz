"""
Script để xóa collection trong Milvus/Zilliz
CẢNH BÁO: Hành động này không thể hoàn tác!
"""

import argparse
import sys
from milvus_config import (
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


def delete_collection(collection_name: str, force: bool = False):
    """
    Xóa collection
    
    Args:
        collection_name: Tên collection cần xóa
        force: Nếu True, xóa ngay không hỏi
    """
    print("=" * 70)
    print("🗑️  DELETE COLLECTION")
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
    if not utility.has_collection(collection_name):
        print(f"⚠️  Collection '{collection_name}' does not exist!")
        return False
    
    # Show collection info
    try:
        collection = Collection(collection_name)
        collection.load()
        num_entities = collection.num_entities
        print(f"📦 Collection: {collection_name}")
        print(f"📊 Number of vectors: {num_entities:,}")
        print()
    except Exception as e:
        print(f"⚠️  Could not load collection info: {e}")
        print()
    
    # Confirm deletion
    if not force:
        print("⚠️  WARNING: This action cannot be undone!")
        print("⚠️  All data in this collection will be permanently deleted!")
        print()
        response = input(f"Are you sure you want to delete '{collection_name}'? (yes/no): ").strip().lower()
        if response != "yes":
            print("❌ Deletion cancelled.")
            return False
    
    # Delete collection
    try:
        print(f"🗑️  Deleting collection '{collection_name}'...")
        utility.drop_collection(collection_name)
        print(f"✅ Collection '{collection_name}' deleted successfully!")
        return True
    except Exception as e:
        print(f"❌ Failed to delete collection: {e}")
        return False


def main():
    parser = argparse.ArgumentParser(
        description="Delete a collection from Milvus/Zilliz (WARNING: Cannot be undone!)"
    )
    parser.add_argument(
        "--collection",
        required=True,
        help="Name of collection to delete"
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Delete without confirmation (dangerous!)"
    )
    
    args = parser.parse_args()
    
    delete_collection(args.collection, args.force)


if __name__ == "__main__":
    main()

