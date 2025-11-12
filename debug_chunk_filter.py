"""
Debug script để kiểm tra filter logic
"""

import sys
from pymilvus import connections, Collection, utility
from milvus_config import get_connection_params

def debug_filter(collection_name: str, chunk_start: int, chunk_end: int):
    """Debug filter logic"""
    
    params = get_connection_params()
    connections.connect("default", **params)
    
    collection = Collection(collection_name)
    collection.load()
    
    # Query như script làm
    start_job_id = f"url_{chunk_start:04d}"
    end_job_id = f"url_{chunk_end:04d}"
    
    print(f"Query: job_id >= '{start_job_id}' and job_id < '{end_job_id}'")
    
    query_result = collection.query(
        expr=f'job_id >= "{start_job_id}" and job_id < "{end_job_id}"',
        output_fields=["job_id"]
    )
    
    print(f"Total queried: {len(query_result)}")
    
    # Filter như script làm
    def extract_job_num(job_id_str):
        try:
            return int(job_id_str.split('_')[1])
        except:
            return -1
    
    filtered_data = [
        item for item in query_result
        if chunk_start <= extract_job_num(item["job_id"]) < chunk_end
    ]
    
    print(f"After filtering: {len(filtered_data)}")
    
    # Check job_id distribution
    job_nums = [extract_job_num(item["job_id"]) for item in query_result]
    in_range = [n for n in job_nums if chunk_start <= n < chunk_end]
    out_of_range = [n for n in job_nums if n < chunk_start or n >= chunk_end]
    
    print(f"\nJob ID distribution:")
    print(f"  In range [{chunk_start}, {chunk_end}): {len(in_range)}")
    print(f"  Out of range: {len(out_of_range)}")
    
    if out_of_range:
        print(f"\nOut of range samples:")
        out_of_range_sample = sorted(set(out_of_range))[:10]
        for n in out_of_range_sample:
            count = job_nums.count(n)
            print(f"  job_id {n}: {count} videos")
    
    print(f"\nExpected: {chunk_end - chunk_start}")
    print(f"Got after filter: {len(filtered_data)}")
    print(f"Difference: {chunk_end - chunk_start - len(filtered_data)}")

if __name__ == "__main__":
    debug_filter("video_dedup_tri_quan", 0, 5000)

