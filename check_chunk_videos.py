"""
Kiểm tra số lượng videos thực tế trong chunk range từ collection
"""

import sys
from pymilvus import connections, Collection, utility
from milvus_config import get_connection_params, print_config

def check_chunk_videos(collection_name: str, chunk_start: int, chunk_end: int):
    """Kiểm tra số videos thực tế trong chunk range"""
    
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
    
    if not utility.has_collection(collection_name):
        print(f"❌ Collection '{collection_name}' not found!")
        sys.exit(1)
    
    collection = Collection(collection_name)
    collection.load()
    
    total_entities = collection.num_entities
    print(f"📊 Total entities in collection: {total_entities:,}")
    print()
    
    # Try different job_id formats
    formats_to_try = [
        (f"url_{chunk_start:04d}", f"url_{chunk_end:04d}", ":04d format"),
        (f"url_{chunk_start}", f"url_{chunk_end}", "no leading zeros"),
        (f"url_{chunk_start:05d}", f"url_{chunk_end:05d}", ":05d format"),
    ]
    
    print(f"🔍 Checking chunk range [{chunk_start}, {chunk_end})...")
    print()
    
    for start_job_id, end_job_id, format_name in formats_to_try:
        try:
            print(f"   Trying format: {format_name}")
            print(f"   Query: job_id >= '{start_job_id}' and job_id < '{end_job_id}'")
            
            # Query to count
            query_result = collection.query(
                expr=f'job_id >= "{start_job_id}" and job_id < "{end_job_id}"',
                output_fields=["job_id"],
                limit=100  # Just check if it works
            )
            
            if query_result:
                # Count all (need to query without limit or use count)
                # Query all to count
                all_results = collection.query(
                    expr=f'job_id >= "{start_job_id}" and job_id < "{end_job_id}"',
                    output_fields=["job_id"]
                )
                
                count = len(all_results)
                print(f"   ✅ Found {count} videos with format {format_name}")
                
                # Extract job numbers
                job_nums = []
                for item in all_results:
                    try:
                        num = int(item['job_id'].split('_')[1])
                        job_nums.append(num)
                    except:
                        pass
                
                if job_nums:
                    min_job = min(job_nums)
                    max_job = max(job_nums)
                    print(f"   📊 Job ID range: {min_job} to {max_job}")
                    print(f"   📊 Expected range: {chunk_start} to {chunk_end-1}")
                    
                    # Check if all are in range
                    in_range = [n for n in job_nums if chunk_start <= n < chunk_end]
                    out_of_range = [n for n in job_nums if n < chunk_start or n >= chunk_end]
                    
                    print(f"   ✅ In range [{chunk_start}, {chunk_end}): {len(in_range)}")
                    if out_of_range:
                        print(f"   ⚠️  Out of range: {len(out_of_range)} (min: {min(out_of_range)}, max: {max(out_of_range)})")
                
                print()
                return count, format_name
            else:
                print(f"   ⚠️  No results with format {format_name}")
                print()
        except Exception as e:
            print(f"   ❌ Error with format {format_name}: {e}")
            print()
    
    print("❌ Could not find any videos in chunk range!")
    return 0, None


if __name__ == "__main__":
    if len(sys.argv) < 4:
        print("Usage: python check_chunk_videos.py <collection_name> <chunk_start> <chunk_end>")
        print("Example: python check_chunk_videos.py video_dedup_tri_quan 0 5000")
        sys.exit(1)
    
    collection_name = sys.argv[1]
    chunk_start = int(sys.argv[2])
    chunk_end = int(sys.argv[3])
    
    count, format_name = check_chunk_videos(collection_name, chunk_start, chunk_end)
    
    print(f"\n{'='*80}")
    print(f"📊 KẾT QUẢ:")
    print(f"   Collection: {collection_name}")
    print(f"   Chunk range: [{chunk_start}, {chunk_end})")
    print(f"   Videos found: {count}")
    print(f"   Expected: {chunk_end - chunk_start}")
    print(f"   Difference: {chunk_end - chunk_start - count} videos")
    print(f"{'='*80}\n")
    
    if count < chunk_end - chunk_start:
        print("💡 GIẢI THÍCH:")
        print("   Collection không có đủ videos trong range này!")
        print("   Có thể:")
        print("   1. Videos được upload với job_id không liên tục")
        print("   2. Nhiều videos upload thất bại")
        print("   3. Job_id format không khớp")
        print("   4. Collection chỉ có videos ở range khác")

