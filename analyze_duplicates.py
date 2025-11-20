"""
Script để phân tích kết quả duplicate detection
Giúp hiểu tại sao tỷ lệ unique quá thấp
"""

import csv
import sys
from collections import defaultdict

def analyze_duplicates(duplicates_csv: str, unique_csv: str):
    """Phân tích file duplicates và unique"""
    
    print("="*70)
    print("📊 PHÂN TÍCH KẾT QUẢ DUPLICATE DETECTION")
    print("="*70)
    
    # Đọc duplicates
    duplicates = []
    similarity_scores = []
    original_counts = defaultdict(int)
    
    try:
        with open(duplicates_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                duplicates.append(row)
                similarity = float(row['similarity'])
                similarity_scores.append(similarity)
                original_counts[row['original_job_id']] += 1
    except FileNotFoundError:
        print(f"❌ File không tìm thấy: {duplicates_csv}")
        return
    
    # Đọc unique
    unique_count = 0
    try:
        with open(unique_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            unique_count = sum(1 for _ in reader)
    except FileNotFoundError:
        print(f"❌ File không tìm thấy: {unique_csv}")
        return
    
    print(f"\n📈 THỐNG KÊ:")
    print(f"   Unique videos: {unique_count}")
    print(f"   Duplicates found: {len(duplicates)}")
    
    if similarity_scores:
        print(f"\n📊 SIMILARITY SCORE DISTRIBUTION:")
        print(f"   Min: {min(similarity_scores):.6f}")
        print(f"   Max: {max(similarity_scores):.6f}")
        print(f"   Avg: {sum(similarity_scores)/len(similarity_scores):.6f}")
        print(f"   Median: {sorted(similarity_scores)[len(similarity_scores)//2]:.6f}")
        
        # Phân loại theo threshold
        below_995 = sum(1 for s in similarity_scores if s < 0.995)
        above_995 = len(similarity_scores) - below_995
        
        print(f"\n   ⚠️  Với threshold 0.995:")
        print(f"      - Duplicates >= 0.995: {above_995} ({above_995/len(similarity_scores)*100:.1f}%)")
        print(f"      - Duplicates < 0.995: {below_995} ({below_995/len(similarity_scores)*100:.1f}%)")
        print(f"      → {below_995} duplicates sẽ BỊ BỎ QUA với threshold 0.995!")
        
        # Phân loại theo threshold 0.98
        below_98 = sum(1 for s in similarity_scores if s < 0.98)
        above_98 = len(similarity_scores) - below_98
        
        print(f"\n   ✅ Với threshold 0.98:")
        print(f"      - Duplicates >= 0.98: {above_98} ({above_98/len(similarity_scores)*100:.1f}%)")
        print(f"      - Duplicates < 0.98: {below_98} ({below_98/len(similarity_scores)*100:.1f}%)")
    
    # Top originals (videos có nhiều duplicates nhất)
    if original_counts:
        print(f"\n🔝 TOP 10 VIDEOS CÓ NHIỀU DUPLICATES NHẤT:")
        sorted_originals = sorted(original_counts.items(), key=lambda x: x[1], reverse=True)
        for i, (job_id, count) in enumerate(sorted_originals[:10], 1):
            print(f"   {i}. {job_id}: {count} duplicates")
    
    # Phân tích URL patterns
    print(f"\n🔍 PHÂN TÍCH URL PATTERNS:")
    url_patterns = defaultdict(int)
    for dup in duplicates:
        url = dup['duplicate_url']
        if 'videoplayback' in url:
            url_patterns['Google CDN'] += 1
        elif 'flashtalking.com' in url:
            url_patterns['Flashtalking'] += 1
        elif 'fptplay.net' in url:
            url_patterns['FPT Play'] += 1
        elif 'b-cdn.net' in url:
            url_patterns['Bunny CDN'] += 1
        else:
            url_patterns['Other'] += 1
    
    for pattern, count in sorted(url_patterns.items(), key=lambda x: x[1], reverse=True):
        print(f"   {pattern}: {count} duplicates ({count/len(duplicates)*100:.1f}%)")
    
    print("\n" + "="*70)
    print("💡 KHUYẾN NGHỊ:")
    print("="*70)
    
    if similarity_scores:
        below_995_pct = below_995 / len(similarity_scores) * 100
        if below_995_pct > 20:
            print(f"   ⚠️  {below_995_pct:.1f}% duplicates có similarity < 0.995")
            print(f"   → Nên giảm threshold xuống 0.98-0.99 để bắt được nhiều duplicates hơn")
        
        if below_98 > 0:
            below_98_pct = below_98 / len(similarity_scores) * 100
            print(f"   ⚠️  {below_98_pct:.1f}% duplicates có similarity < 0.98")
            print(f"   → Có thể có false positives nếu giảm threshold quá thấp")
    
    print(f"\n   📝 Thử chạy lại với:")
    print(f"      python search_duplicates_aggregated.py --cosine_thresh 0.98 --chunk_start 0 --chunk_end 23000")
    print(f"      python search_duplicates_aggregated.py --cosine_thresh 0.99 --chunk_start 0 --chunk_end 23000")
    print("\n" + "="*70)


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python analyze_duplicates.py <duplicates_csv> <unique_csv>")
        print("Example: python analyze_duplicates.py duplicates_chunk_0_23000.csv FINAL_RESULT_chunk_0_23000.csv")
        sys.exit(1)
    
    duplicates_csv = sys.argv[1]
    unique_csv = sys.argv[2]
    
    analyze_duplicates(duplicates_csv, unique_csv)


