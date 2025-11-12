"""
Script để phân tích kết quả chunk và tìm ra tại sao có nhiều videos "mất tích"
"""

import csv
import sys
from collections import defaultdict

def analyze_chunk_results(unique_csv, duplicates_csv, chunk_start, chunk_end):
    """Phân tích kết quả chunk để tìm videos bị thiếu"""
    
    print(f"\n{'='*80}")
    print(f"📊 PHÂN TÍCH KẾT QUẢ CHUNK {chunk_start}-{chunk_end}")
    print(f"{'='*80}\n")
    
    # Đọc FINAL_RESULT (unique videos)
    unique_job_ids = set()
    unique_urls = set()
    
    try:
        with open(unique_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                url = row.get('decoded_url', '').strip().strip('"')
                if url:
                    unique_urls.add(url)
    except Exception as e:
        print(f"❌ Lỗi đọc {unique_csv}: {e}")
        return
    
    print(f"📄 FINAL_RESULT.csv:")
    print(f"   - Số URLs unique: {len(unique_urls)}")
    
    # Đọc duplicates.csv
    duplicate_job_ids = set()
    cross_chunk_duplicates = 0
    within_chunk_duplicates = 0
    duplicate_urls = set()
    original_job_ids_in_chunk = set()
    original_job_ids_out_chunk = set()
    
    try:
        with open(duplicates_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                dup_job_id = row.get('duplicate_job_id', '').strip()
                orig_job_id = row.get('original_job_id', '').strip()
                dup_url = row.get('duplicate_url', '').strip().strip('"')
                orig_url = row.get('original_url', '').strip().strip('"')
                
                if dup_job_id:
                    duplicate_job_ids.add(dup_job_id)
                    duplicate_urls.add(dup_url)
                
                # Kiểm tra cross-chunk
                if '[CROSS-CHUNK:' in orig_url or orig_url.startswith('[CROSS-CHUNK:'):
                    cross_chunk_duplicates += 1
                    original_job_ids_out_chunk.add(orig_job_id)
                else:
                    within_chunk_duplicates += 1
                    # Extract job_id từ original_job_id
                    if orig_job_id.startswith('url_'):
                        try:
                            job_num = int(orig_job_id.split('_')[1])
                            if chunk_start <= job_num < chunk_end:
                                original_job_ids_in_chunk.add(orig_job_id)
                        except:
                            pass
    except Exception as e:
        print(f"❌ Lỗi đọc {duplicates_csv}: {e}")
        return
    
    print(f"\n📄 duplicates.csv:")
    print(f"   - Tổng số duplicates: {len(duplicate_job_ids)}")
    print(f"   - Within-chunk duplicates: {within_chunk_duplicates}")
    print(f"   - Cross-chunk duplicates: {cross_chunk_duplicates}")
    print(f"   - Original job_ids trong chunk: {len(original_job_ids_in_chunk)}")
    print(f"   - Original job_ids ngoài chunk: {len(original_job_ids_out_chunk)}")
    
    # Tính toán
    expected_total = chunk_end - chunk_start
    accounted_for = len(unique_urls) + len(duplicate_job_ids)
    missing = expected_total - accounted_for
    
    print(f"\n📊 TỔNG KẾT:")
    print(f"   - Expected videos (chunk {chunk_start}-{chunk_end}): {expected_total}")
    print(f"   - Unique videos (FINAL_RESULT): {len(unique_urls)}")
    print(f"   - Duplicate videos (duplicates.csv): {len(duplicate_job_ids)}")
    print(f"   - Tổng đã tính: {accounted_for}")
    print(f"   - ⚠️  THIẾU: {missing} videos ({missing/expected_total*100:.1f}%)")
    
    # Phân tích nguyên nhân
    print(f"\n🔍 PHÂN TÍCH NGUYÊN NHÂN:")
    print(f"\n   1. Cross-chunk duplicates:")
    print(f"      - {cross_chunk_duplicates} videos bị phát hiện là duplicate của video ngoài chunk")
    print(f"      - Những video này KHÔNG có trong FINAL_RESULT (đúng)")
    print(f"      - Nhưng chỉ có {cross_chunk_duplicates} videos trong duplicates.csv")
    print(f"      - ⚠️  Có thể có nhiều videos bị duplicate nhưng không được ghi vào file!")
    
    print(f"\n   2. Possible issues:")
    print(f"      a) Script chỉ tìm TOP_K (mặc định 10) duplicates gần nhất")
    print(f"         → Nếu video có > 10 duplicates, chỉ có 10 được phát hiện")
    print(f"      b) Nếu tất cả TOP_K duplicates đều ngoài chunk:")
    print(f"         → Video sẽ bị đánh dấu cross-chunk duplicate")
    print(f"         → Nhưng có thể không được ghi vào duplicates.csv đầy đủ")
    print(f"      c) Auto-clean có thể loại bỏ nhiều URLs:")
    print(f"         → Nếu --auto_clean được bật, nhiều URLs có thể bị loại")
    print(f"         → Kiểm tra file invalid_urls_chunk_*.csv")
    
    print(f"\n   3. Recommendation:")
    print(f"      - Kiểm tra log khi chạy script để xem:")
    print(f"        + Số lượng videos loaded từ chunk")
    print(f"        + Số lượng cross-chunk duplicates được phát hiện")
    print(f"        + Số lượng standalone videos")
    print(f"        + Số lượng invalid URLs (nếu auto-clean)")
    print(f"      - Chạy lại với --top_k lớn hơn (ví dụ: 50) để tìm nhiều duplicates hơn")
    print(f"      - Kiểm tra file invalid_urls nếu có")
    
    # Kiểm tra invalid URLs file
    invalid_csv = f"invalid_urls_chunk_{chunk_start}_{chunk_end}.csv"
    try:
        with open(invalid_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            invalid_count = sum(1 for _ in reader)
            if invalid_count > 0:
                print(f"\n   4. Invalid URLs (auto-clean):")
                print(f"      - Tìm thấy {invalid_count} invalid URLs trong {invalid_csv}")
                print(f"      - Những URLs này đã bị loại khỏi FINAL_RESULT")
                print(f"      - Đây có thể là một phần của {missing} videos bị thiếu")
    except FileNotFoundError:
        print(f"\n   4. Invalid URLs:")
        print(f"      - Không tìm thấy file {invalid_csv}")
        print(f"      - Có thể --auto_clean không được bật")
    except Exception as e:
        print(f"\n   4. Invalid URLs:")
        print(f"      - Lỗi đọc file: {e}")
    
    print(f"\n{'='*80}\n")


if __name__ == "__main__":
    if len(sys.argv) < 5:
        print("Usage: python analyze_chunk_results.py <unique_csv> <duplicates_csv> <chunk_start> <chunk_end>")
        print("Example: python analyze_chunk_results.py FINAL_RESULT_chunk_0_5000.csv duplicates_chunk_0_5000.csv 0 5000")
        sys.exit(1)
    
    unique_csv = sys.argv[1]
    duplicates_csv = sys.argv[2]
    chunk_start = int(sys.argv[3])
    chunk_end = int(sys.argv[4])
    
    analyze_chunk_results(unique_csv, duplicates_csv, chunk_start, chunk_end)

