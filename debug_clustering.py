"""
Script để debug clustering và tìm nguyên nhân tại sao chỉ có ít unique videos
"""

import argparse
import csv
from collections import defaultdict

def analyze_duplicates_report(report_csv: str):
    """Phân tích file duplicates để tìm pattern"""
    print(f"📊 Analyzing duplicates report: {report_csv}")
    
    # Đếm số duplicate pairs theo similarity ranges
    similarity_ranges = {
        "0.99-1.00": 0,
        "0.97-0.99": 0,
        "0.95-0.97": 0,
        "0.90-0.95": 0,
        "< 0.90": 0
    }
    
    # Đếm số video bị duplicate với bao nhiêu video khác
    video_dup_count = defaultdict(int)
    
    # Đếm số original video có bao nhiêu duplicates
    original_dup_count = defaultdict(int)
    
    total_pairs = 0
    
    try:
        with open(report_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total_pairs += 1
                similarity = float(row.get('similarity', 0))
                
                # Phân loại similarity
                if similarity >= 0.99:
                    similarity_ranges["0.99-1.00"] += 1
                elif similarity >= 0.97:
                    similarity_ranges["0.97-0.99"] += 1
                elif similarity >= 0.95:
                    similarity_ranges["0.95-0.97"] += 1
                elif similarity >= 0.90:
                    similarity_ranges["0.90-0.95"] += 1
                else:
                    similarity_ranges["< 0.90"] += 1
                
                # Đếm số duplicate của mỗi video
                dup_id = row.get('duplicate_job_id', '')
                orig_id = row.get('original_job_id', '')
                
                if dup_id:
                    video_dup_count[dup_id] += 1
                if orig_id:
                    original_dup_count[orig_id] += 1
        
        print(f"\n📈 STATISTICS:")
        print(f"   Total duplicate pairs: {total_pairs:,}")
        print(f"\n📊 Similarity distribution:")
        for range_name, count in similarity_ranges.items():
            if count > 0:
                percent = (count / total_pairs * 100) if total_pairs > 0 else 0
                print(f"   {range_name}: {count:,} pairs ({percent:.1f}%)")
        
        print(f"\n📊 Duplicate videos analysis:")
        if video_dup_count:
            max_dups = max(video_dup_count.values())
            avg_dups = sum(video_dup_count.values()) / len(video_dup_count)
            print(f"   Videos with duplicates: {len(video_dup_count):,}")
            print(f"   Max duplicates per video: {max_dups}")
            print(f"   Average duplicates per video: {avg_dups:.2f}")
        
        print(f"\n📊 Original videos analysis:")
        if original_dup_count:
            max_orig_dups = max(original_dup_count.values())
            avg_orig_dups = sum(original_dup_count.values()) / len(original_dup_count)
            print(f"   Original videos: {len(original_dup_count):,}")
            print(f"   Max duplicates per original: {max_orig_dups}")
            print(f"   Average duplicates per original: {avg_orig_dups:.2f}")
            
            # Top 10 originals với nhiều duplicates nhất
            top_originals = sorted(original_dup_count.items(), key=lambda x: x[1], reverse=True)[:10]
            print(f"\n   Top 10 originals with most duplicates:")
            for orig_id, count in top_originals:
                print(f"      {orig_id}: {count} duplicates")
    
    except Exception as e:
        print(f"❌ Error analyzing report: {e}")


def analyze_unique_videos(unique_csv: str):
    """Phân tích unique videos để xem pattern"""
    print(f"\n📊 Analyzing unique videos: {unique_csv}")
    
    url_patterns = defaultdict(int)
    total = 0
    
    try:
        with open(unique_csv, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                total += 1
                url = row.get('decoded_url', '')
                
                # Phân loại theo domain/pattern
                if 'gcdn.2mdn.net' in url or 'videoplayback' in url:
                    url_patterns['Google CDN'] += 1
                elif 'cdn.flashtalking.com' in url:
                    url_patterns['Flashtalking CDN'] += 1
                elif 'vz-' in url and '.b-cdn.net' in url:
                    url_patterns['Bunny CDN'] += 1
                elif 'ads-cdn.fptplay.net' in url:
                    url_patterns['FPT Play CDN'] += 1
                elif 'crcdn09.adnxs-simple.com' in url:
                    url_patterns['AppNexus CDN'] += 1
                elif 'static.aiactiv.io' in url:
                    url_patterns['AIActiv'] += 1
                elif 'assets.springserve.com' in url:
                    url_patterns['SpringServe'] += 1
                else:
                    url_patterns['Other'] += 1
        
        print(f"   Total unique videos: {total:,}")
        print(f"\n📊 URL pattern distribution:")
        for pattern, count in sorted(url_patterns.items(), key=lambda x: x[1], reverse=True):
            percent = (count / total * 100) if total > 0 else 0
            print(f"   {pattern}: {count:,} ({percent:.1f}%)")
    
    except Exception as e:
        print(f"❌ Error analyzing unique videos: {e}")


def main():
    parser = argparse.ArgumentParser(description="Debug clustering results")
    parser.add_argument("--duplicates_csv", required=True, help="Duplicates report CSV")
    parser.add_argument("--unique_csv", required=True, help="Unique videos CSV")
    
    args = parser.parse_args()
    
    print("=" * 70)
    print("🔍 CLUSTERING DEBUG ANALYSIS")
    print("=" * 70)
    
    analyze_duplicates_report(args.duplicates_csv)
    analyze_unique_videos(args.unique_csv)
    
    print("\n" + "=" * 70)
    print("💡 INTERPRETATION:")
    print("=" * 70)
    print("""
1. Nếu nhiều pairs có similarity >= 0.99:
   → Có thể thực sự có nhiều duplicate videos
   
2. Nếu nhiều pairs có similarity 0.95-0.98:
   → Threshold quá thấp hoặc transitive closure tạo cluster lớn
   
3. Nếu một original video có rất nhiều duplicates:
   → Có thể video đó là template/intro được dùng nhiều lần
   
4. Nếu unique videos chủ yếu từ một domain:
   → Có thể embedding từ domain đó không đủ đa dạng
    """)


if __name__ == "__main__":
    main()


