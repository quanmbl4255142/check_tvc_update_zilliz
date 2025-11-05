"""
Dedupe Google Ads URLs - Group by video ID
Google tạo nhiều versions với expire time khác nhau, chỉ giữ 1
"""

import csv
import re
from urllib.parse import urlparse, parse_qs
from collections import defaultdict

def extract_google_ads_id(url):
    """Extract video ID from Google Ads URL"""
    # Pattern: id/XXXX/itag/YYY
    match = re.search(r'/id/([a-f0-9]+)/', url)
    if match:
        return match.group(1)
    return None

def extract_itag(url):
    """Extract itag (quality) from URL"""
    match = re.search(r'/itag/(\d+)/', url)
    if match:
        return match.group(1)
    return None

def dedupe_google_ads(input_csv, output_csv, report_csv):
    """Dedupe Google Ads URLs, keep only one per video ID + itag"""
    
    urls = []
    with open(input_csv, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get('decoded_url', '').strip()
            if url.startswith('"') and url.endswith('"'):
                url = url[1:-1]
            urls.append(url)
    
    # Group URLs
    google_ads_groups = defaultdict(list)  # (video_id, itag) -> [urls]
    other_urls = []
    
    for url in urls:
        if 'gcdn.2mdn.net' in url or 'googlevideo.com' in url:
            video_id = extract_google_ads_id(url)
            itag = extract_itag(url)
            
            if video_id and itag:
                key = (video_id, itag)
                google_ads_groups[key].append(url)
            else:
                other_urls.append(url)
        else:
            other_urls.append(url)
    
    # Keep only first URL per group
    unique_urls = []
    duplicates = []
    
    for (video_id, itag), group_urls in google_ads_groups.items():
        if len(group_urls) > 1:
            # Keep first, mark others as duplicate
            unique_urls.append(group_urls[0])
            for dup_url in group_urls[1:]:
                duplicates.append({
                    'duplicate_url': dup_url,
                    'original_url': group_urls[0],
                    'reason': f'Google Ads - same video_id ({video_id}) and itag ({itag})'
                })
        else:
            unique_urls.append(group_urls[0])
    
    # Add other URLs
    unique_urls.extend(other_urls)
    
    # Write unique
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['decoded_url'])
        for url in unique_urls:
            writer.writerow([url])
    
    # Write duplicates
    with open(report_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['duplicate_url', 'original_url', 'reason'])
        for dup in duplicates:
            writer.writerow([dup['duplicate_url'], dup['original_url'], dup['reason']])
    
    return len(unique_urls), len(duplicates)


if __name__ == "__main__":
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else "FINAL_RESULT_CLEAN.csv"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "FINAL_RESULT_ULTRA_CLEAN.csv"
    report_file = sys.argv[3] if len(sys.argv) > 3 else "google_ads_duplicates.csv"
    
    print(f"📥 Processing: {input_file}")
    unique, dups = dedupe_google_ads(input_file, output_file, report_file)
    
    print(f"\n✅ DONE!")
    print(f"   ✅ Unique URLs: {unique}")
    print(f"   ❌ Google Ads duplicates: {dups}")
    print(f"\n📁 Output:")
    print(f"   → {output_file}")
    print(f"   → {report_file}")

