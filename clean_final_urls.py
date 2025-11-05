"""
Clean FINAL_RESULT - Loại bỏ:
- File PNG/ảnh (không phải video)
- URLs lỗi/không hợp lệ
- URLs quá ngắn
"""

import csv
import re
from urllib.parse import urlparse

def is_valid_video_url(url):
    """Kiểm tra URL có phải video hợp lệ không"""
    
    # Loại bỏ URLs quá ngắn hoặc lỗi
    if not url or len(url) < 20:
        return False, "URL quá ngắn hoặc lỗi"
    
    # Phải bắt đầu với http/https
    if not url.startswith(('http://', 'https://')):
        return False, "Không phải URL hợp lệ"
    
    # Loại bỏ file ảnh
    image_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.webp', '.bmp']
    url_lower = url.lower()
    for ext in image_extensions:
        if url_lower.endswith(ext) or ext in url_lower.split('?')[0]:
            return False, f"File ảnh ({ext})"
    
    # Phải có video extension hoặc streaming format
    video_indicators = [
        '.mp4', '.webm', '.mov', '.avi', '.mkv', '.flv',
        '.m3u8', 'video', 'play_', 'videoplayback'
    ]
    
    has_video_indicator = any(indicator in url_lower for indicator in video_indicators)
    if not has_video_indicator:
        return False, "Không có indicator video"
    
    # Kiểm tra domain hợp lệ
    try:
        parsed = urlparse(url)
        if not parsed.netloc:
            return False, "Domain không hợp lệ"
    except:
        return False, "Parse URL thất bại"
    
    return True, "OK"


def clean_final_result(input_csv, output_csv, report_csv):
    """Clean final result và tạo báo cáo"""
    
    valid_urls = []
    invalid_urls = []
    
    # Read input
    with open(input_csv, 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = row.get('decoded_url', '').strip()
            
            # Remove quotes if exists
            if url.startswith('"') and url.endswith('"'):
                url = url[1:-1]
            
            is_valid, reason = is_valid_video_url(url)
            
            if is_valid:
                valid_urls.append(url)
            else:
                invalid_urls.append({
                    'url': url,
                    'reason': reason
                })
    
    # Write valid URLs
    with open(output_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['decoded_url'])
        for url in valid_urls:
            writer.writerow([url])
    
    # Write invalid URLs report
    with open(report_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['invalid_url', 'reason'])
        for item in invalid_urls:
            writer.writerow([item['url'], item['reason']])
    
    return len(valid_urls), len(invalid_urls)


if __name__ == "__main__":
    import sys
    
    input_file = sys.argv[1] if len(sys.argv) > 1 else "FINAL_RESULT_v2.csv"
    output_file = sys.argv[2] if len(sys.argv) > 2 else "FINAL_RESULT_CLEAN.csv"
    report_file = sys.argv[3] if len(sys.argv) > 3 else "invalid_urls_report.csv"
    
    print(f"📥 Reading: {input_file}")
    valid_count, invalid_count = clean_final_result(input_file, output_file, report_file)
    
    print(f"\n✅ DONE!")
    print(f"   ✅ Valid videos: {valid_count}")
    print(f"   ❌ Invalid URLs: {invalid_count}")
    print(f"\n📁 Output files:")
    print(f"   → {output_file} (Clean URLs)")
    print(f"   → {report_file} (Invalid URLs report)")

