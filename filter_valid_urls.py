"""
Script để lọc các URL hợp lệ (không bị 403) trước khi upload lên Zilliz
Sử dụng HEAD request để kiểm tra nhanh hơn
"""

import argparse
import csv
import os
import sys
import time
from typing import List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

try:
    import requests
except ImportError:
    print("❌ Cần cài đặt requests: pip install requests")
    sys.exit(1)


def check_url_status(url: str, timeout: int = 10) -> Tuple[str, int, str]:
    """
    Kiểm tra status code của URL bằng HEAD request (nhanh hơn GET)
    
    Returns:
        (url, status_code, error_message)
    """
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
        'Accept': '*/*',
    }
    
    try:
        # Thử HEAD request trước (nhanh hơn, không download data)
        response = requests.head(url, headers=headers, timeout=timeout, allow_redirects=True)
        status = response.status_code
        
        # Nếu HEAD không được hỗ trợ (405), thử GET với stream
        if status == 405:
            response = requests.get(url, headers=headers, timeout=timeout, stream=True, allow_redirects=True)
            status = response.status_code
            response.close()
        
        return (url, status, "")
        
    except requests.exceptions.Timeout:
        return (url, 0, "Timeout")
    except requests.exceptions.ConnectionError:
        return (url, 0, "Connection error")
    except requests.exceptions.RequestException as e:
        return (url, 0, str(e))
    except Exception as e:
        return (url, 0, f"Unexpected error: {str(e)}")


def filter_urls(
    input_csv: str,
    output_csv: str,
    invalid_csv: str,
    column: str = "decoded_url",
    start: int = 0,
    end: int = None,
    max_workers: int = 10,
    timeout: int = 10
):
    """
    Lọc URLs từ CSV, loại bỏ các URL bị 403 hoặc lỗi khác
    """
    print(f"📖 Đọc URLs từ {input_csv}...")
    
    # Đọc URLs
    urls: List[str] = []
    with open(input_csv, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        if column in reader.fieldnames:
            for row in reader:
                u = (row.get(column) or "").strip().strip('"')
                if u:
                    urls.append(u)
        else:
            # Fallback: first column
            f.seek(0)
            reader2 = csv.reader(f)
            for i, row in enumerate(reader2):
                if not row:
                    continue
                cell = row[0].strip().strip('"')
                if i == 0 and cell.lower() in {"decoded_url", "url", "tvc"}:
                    continue
                if cell:
                    urls.append(cell)
    
    if not urls:
        print("❌ Không tìm thấy URL nào!")
        return
    
    # Xác định range
    if end is None or end > len(urls):
        end = len(urls)
    start = max(0, start)
    
    if start >= end:
        print("❌ Range không hợp lệ!")
        return
    
    urls_to_check = urls[start:end]
    print(f"📊 Kiểm tra {len(urls_to_check)} URLs (index {start} đến {end-1})...")
    print(f"⚙️  Sử dụng {max_workers} workers, timeout {timeout}s\n")
    
    # Kiểm tra URLs với thread pool
    valid_urls = []
    invalid_urls = []
    
    stats = {
        "total": len(urls_to_check),
        "valid": 0,
        "403": 0,
        "404": 0,
        "other_error": 0,
        "timeout": 0
    }
    
    t0 = time.time()
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_url = {
            executor.submit(check_url_status, url, timeout): url 
            for url in urls_to_check
        }
        
        # Process results as they complete
        for idx, future in enumerate(as_completed(future_to_url)):
            url, status, error = future.result()
            global_idx = start + idx
            
            if status == 200:
                valid_urls.append(url)
                stats["valid"] += 1
                print(f"✅ [{global_idx}] {url[:60]}... - OK (200)")
            elif status == 403:
                invalid_urls.append((url, 403, "Forbidden"))
                stats["403"] += 1
                print(f"❌ [{global_idx}] {url[:60]}... - 403 Forbidden")
            elif status == 404:
                invalid_urls.append((url, 404, "Not Found"))
                stats["404"] += 1
                print(f"❌ [{global_idx}] {url[:60]}... - 404 Not Found")
            elif status == 0:
                invalid_urls.append((url, 0, error))
                stats["timeout"] += 1
                print(f"⚠️  [{global_idx}] {url[:60]}... - Error: {error}")
            else:
                invalid_urls.append((url, status, f"HTTP {status}"))
                stats["other_error"] += 1
                print(f"⚠️  [{global_idx}] {url[:60]}... - HTTP {status}")
            
            # Progress update
            if (idx + 1) % 50 == 0:
                elapsed = time.time() - t0
                rate = (idx + 1) / elapsed if elapsed > 0 else 0
                remaining = (len(urls_to_check) - idx - 1) / rate if rate > 0 else 0
                print(f"\n📊 Progress: {idx+1}/{len(urls_to_check)} | Rate: {rate:.2f} URLs/s | ETA: {remaining/60:.1f} min\n")
    
    # Ghi kết quả
    print(f"\n💾 Ghi kết quả...")
    
    # Valid URLs
    with open(output_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([column])
        for url in valid_urls:
            writer.writerow([url])
    
    # Invalid URLs với thông tin lỗi
    with open(invalid_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([column, "status_code", "error"])
        for url, status, error in invalid_urls:
            writer.writerow([url, status, error])
    
    # Summary
    total_time = time.time() - t0
    print("\n" + "="*60)
    print("✅ HOÀN THÀNH!")
    print("="*60)
    print(f"Tổng số URLs kiểm tra: {stats['total']}")
    print(f"✅ URLs hợp lệ (200): {stats['valid']} ({stats['valid']/stats['total']*100:.1f}%)")
    print(f"❌ URLs bị 403: {stats['403']} ({stats['403']/stats['total']*100:.1f}%)")
    print(f"❌ URLs bị 404: {stats['404']} ({stats['404']/stats['total']*100:.1f}%)")
    print(f"⚠️  Timeout/Lỗi khác: {stats['timeout'] + stats['other_error']} ({(stats['timeout'] + stats['other_error'])/stats['total']*100:.1f}%)")
    print(f"\n⏱️  Thời gian: {total_time/60:.1f} phút")
    print(f"📁 File hợp lệ: {output_csv}")
    print(f"📁 File lỗi: {invalid_csv}")
    print("="*60)


def main():
    parser = argparse.ArgumentParser(
        description="Lọc các URL hợp lệ (loại bỏ 403/404) trước khi upload"
    )
    parser.add_argument(
        "--input",
        default="url-tvc.unique.csv",
        help="File CSV input (default: url-tvc.unique.csv)"
    )
    parser.add_argument(
        "--output",
        default="url-tvc.valid.csv",
        help="File CSV output chứa URLs hợp lệ (default: url-tvc.valid.csv)"
    )
    parser.add_argument(
        "--invalid",
        default="url-tvc.invalid.csv",
        help="File CSV chứa URLs lỗi (default: url-tvc.invalid.csv)"
    )
    parser.add_argument(
        "--column",
        default="decoded_url",
        help="Tên cột chứa URLs (default: decoded_url)"
    )
    parser.add_argument(
        "--start",
        type=int,
        default=0,
        help="Index bắt đầu (default: 0)"
    )
    parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="Index kết thúc (default: all)"
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Số lượng workers đồng thời (default: 10)"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=10,
        help="Timeout cho mỗi request (seconds, default: 10)"
    )
    
    args = parser.parse_args()
    
    if not os.path.isfile(args.input):
        print(f"❌ File không tồn tại: {args.input}")
        sys.exit(1)
    
    filter_urls(
        args.input,
        args.output,
        args.invalid,
        args.column,
        args.start,
        args.end,
        args.workers,
        args.timeout
    )


if __name__ == "__main__":
    main()

