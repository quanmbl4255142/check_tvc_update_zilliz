# 📹 Video Deduplication System - Hướng Dẫn Sử Dụng

Hệ thống lọc trùng video sử dụng CLIP embeddings và Milvus/Zilliz để phát hiện và loại bỏ video trùng lặp, bao gồm cả các video giống nhau nhưng khác độ phân giải hoặc kích thước khung hình.

---

## 📋 Mục Lục

1. [Tổng Quan](#tổng-quan)
2. [Luồng Xử Lý](#luồng-xử-lý)
3. [Cài Đặt](#cài-đặt)
4. [Hướng Dẫn Sử Dụng](#hướng-dẫn-sử-dụng)
5. [Chi Tiết Các File](#chi-tiết-các-file)
6. [Best Practices](#best-practices)
7. [Troubleshooting](#troubleshooting)

---

## 🎯 Tổng Quan

Hệ thống này xử lý video deduplication qua các bước:

1. **Decode URLs**: Giải mã URLs từ CSV
2. **Dedupe URLs**: Loại bỏ URL trùng lặp cơ bản (optional)
3. **Filter Valid URLs**: Lọc các URL hợp lệ (loại bỏ 403/404) - **Khuyến nghị**
4. **Tạo Collection**: Tạo collection trong Milvus/Zilliz
5. **Upload Embeddings**: Extract embeddings từ video và upload lên Zilliz
6. **Tìm Duplicates**: Sử dụng vector similarity search để tìm video trùng lặp
7. **Clean Jobs**: Dọn dẹp các job folder rỗng (optional)

### ✨ Tính Năng Chính

- ✅ **Xử lý khác độ phân giải**: Tự động chọn video có resolution cao nhất (1080p > 720p > 480p)
- ✅ **Xử lý khung hình to nhỏ**: Dùng embeddings để so sánh nội dung, không phụ thuộc kích thước pixel
- ✅ **Pre-filtering thông minh**: Loại bỏ cùng video ID với signature/itag khác nhau
- ✅ **Cross-chunk detection**: Phát hiện duplicates giữa các chunks
- ✅ **Batch processing**: Xử lý song song với nhiều threads
- ✅ **Auto-clean**: Tự động loại bỏ PNG/images và URLs lỗi

---

## 🔄 Luồng Xử Lý

```
┌─────────────────┐
│  url-tvc.csv    │  ← Input file chứa URLs
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ decode_urls.py  │  ← Bước 1: Decode URLs
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│url-tvc.decoded  │
│     .csv        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ dedupe_urls.py  │  ← Bước 2: Loại bỏ URL trùng (optional)
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│url-tvc.unique   │
│     .csv        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│filter_valid_urls│  ← Bước 3: Lọc URL hợp lệ (loại bỏ 403/404)
│      .py        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│url-tvc.valid    │
│     .csv        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│create_collection│  ← Bước 4: Tạo collection (nếu chưa có)
│      .py        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│direct_upload_to │  ← Bước 5: Upload embeddings lên Zilliz
│  _zilliz.py     │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│   Zilliz Cloud  │  ← Collection chứa embeddings
│   (Milvus)      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│search_duplicates│  ← Bước 6: Tìm duplicates
│  _aggregated.py │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ FINAL_RESULT_   │  ← Output: Unique URLs
│   AGG.csv       │
│                 │
│duplicate_videos │  ← Output: Duplicates report
│   _agg.csv      │
└─────────────────┘
```

---

## 🛠️ Cài Đặt

### Yêu Cầu

- Python 3.8+
- Milvus/Zilliz Cloud account
- Các thư viện: `pymilvus`, `opencv-python`, `PIL`, `numpy`, `tqdm`, `psutil`

### Cài Đặt Dependencies

```bash
pip install -r requirements.txt
```

### Cấu Hình Milvus/Zilliz

Chỉnh sửa file `milvus_config.py` với thông tin kết nối của bạn:

```python
# Zilliz Cloud connection
ZILLIZ_URI = "your-zilliz-uri"
ZILLIZ_TOKEN = "your-zilliz-token"
```

---

## 📖 Hướng Dẫn Sử Dụng

### Bước 1: Decode URLs

Giải mã URLs từ file CSV gốc:

```bash
python decode_urls.py --input url-tvc.csv --output url-tvc.decoded.csv
```

**Chức năng:**
- Decode percent-encoding trong URLs
- Fix protocol-relative URLs (//example.com → https://example.com)
- Validate URLs

**Output:** `url-tvc.decoded.csv` (1 cột: `decoded_url`)

---

### Bước 2: Dedupe URLs (Optional)

Loại bỏ URL trùng lặp dựa trên URL string (không phải nội dung video):

```bash
python dedupe_urls.py --input url-tvc.decoded.csv --output url-tvc.unique.csv --report url-tvc.duplicates.csv
```

**Chức năng:**
- Normalize URLs (lowercase, remove trailing slash)
- Loại bỏ URLs trùng lặp
- Tạo report các URLs bị loại bỏ

**Output:**
- `url-tvc.unique.csv`: URLs unique
- `url-tvc.duplicates.csv`: URLs bị loại bỏ

**Lưu ý:** Bước này chỉ loại bỏ URL string trùng lặp. Video có URL khác nhau nhưng nội dung giống nhau sẽ được xử lý ở bước 6.

---

### Bước 3: Filter Valid URLs (Khuyến nghị)

Lọc các URL hợp lệ, loại bỏ các URL bị 403 (Forbidden) hoặc 404 (Not Found) trước khi upload:

```bash
# Lọc toàn bộ file
python filter_valid_urls.py --input url-tvc.unique.csv --output url-tvc.valid.csv --invalid url-tvc.invalid.csv

# Lọc một phần (ví dụ: từ index 15500)
python filter_valid_urls.py --input url-tvc.unique.csv --start 15500 --end 16000

# Tùy chỉnh số workers và timeout (nhanh hơn)
python filter_valid_urls.py --workers 20 --timeout 15
```

**Chức năng:**
- Kiểm tra status code của từng URL bằng HEAD request (nhanh hơn GET)
- **Kiểm tra HLS manifest URLs**: Tự động phát hiện và kiểm tra HLS manifest URLs với FFmpeg
- Xử lý đa luồng với thread pool (mặc định 10 workers)
- Tách file: URLs hợp lệ (200) và URLs lỗi (403/404/HLS invalid/timeout)
- Báo cáo chi tiết: số lượng 403, 404, HLS invalid, timeout, v.v.

**Tham số:**
- `--input`: File CSV input (default: `url-tvc.unique.csv`)
- `--output`: File CSV output chứa URLs hợp lệ (default: `url-tvc.valid.csv`)
- `--invalid`: File CSV chứa URLs lỗi (default: `url-tvc.invalid.csv`)
- `--column`: Tên cột chứa URLs (default: `decoded_url`)
- `--start`: Index bắt đầu (default: 0)
- `--end`: Index kết thúc (default: all)
- `--workers`: Số lượng workers đồng thời (default: 10)
- `--timeout`: Timeout cho mỗi request (seconds, default: 10)
- `--skip-hls-check`: Bỏ qua kiểm tra HLS manifest với FFmpeg (nhanh hơn nhưng có thể bỏ sót lỗi)

**Output:**
- `url-tvc.valid.csv`: URLs hợp lệ (status 200, bao gồm cả HLS manifest hợp lệ)
- `url-tvc.invalid.csv`: URLs lỗi với thông tin status code và error (bao gồm HLS invalid)

**Lưu ý:** 
- Bước này giúp tiết kiệm thời gian và tài nguyên khi upload lên Zilliz
- Nên chạy bước này trước khi upload, đặc biệt với dataset lớn
- Sử dụng file `url-tvc.valid.csv` làm input cho bước upload
- **Cần cài FFmpeg** để kiểm tra HLS manifest URLs (nếu không có, script sẽ cảnh báo nhưng vẫn chạy)
- Nếu không muốn kiểm tra HLS (nhanh hơn), dùng `--skip-hls-check`

---

### Bước 4: Tạo Collection

Tạo collection mới trong Milvus/Zilliz (nếu chưa có):

```bash
python create_collection.py --collection video_dedup_v2 --schema video_dedup
```

**Các schema types:**
- `video_dedup`: 1 vector per video (direct upload) - **Khuyến nghị**
- `video_frames`: Multiple frames per video
- `aggregated`: Aggregated vectors (3 frames → 1 vector)

**Lưu ý:** Nếu collection đã tồn tại, script sẽ hỏi có muốn drop và recreate không.

---

### Bước 5: Upload Embeddings

Upload embeddings từ CSV lên Zilliz:

```bash
# Upload toàn bộ (sử dụng file đã lọc)
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection video_dedup_v2

# Upload một phần (chunk)
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection video_dedup_v2 --start 0 --end 10000

# Tiếp tục upload từ index 10000
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection video_dedup_v2 --start 10000 --end 20000
```

**Chức năng:**
- Đọc URLs từ CSV
- Extract frame đầu tiên từ video
- Tạo CLIP embedding (512 dimensions)
- Upload lên Zilliz với batch size tự động

**Tham số:**
- `--input`: File CSV chứa URLs
- `--column`: Tên cột chứa URLs (default: `decoded_url`)
- `--collection`: Tên collection trong Zilliz
- `--start`: Index bắt đầu (inclusive)
- `--end`: Index kết thúc (exclusive)
- `--overwrite`: Cho phép thêm vào collection đã có

**Output:** Embeddings được lưu trong Zilliz collection

**Lưu ý:** Nên sử dụng file `url-tvc.valid.csv` (đã lọc 403/404) thay vì `url-tvc.unique.csv` để tránh lãng phí thời gian xử lý các URL không hợp lệ.

---

### Bước 6: Tìm Duplicates

Tìm video trùng lặp dựa trên vector similarity:

```bash
# Tìm duplicates toàn bộ collection
python search_duplicates_aggregated.py \
    --collection video_dedup_v2 \
    --cosine_thresh 0.95 \
    --unique_csv FINAL_RESULT_AGG.csv \
    --report_csv duplicate_videos_agg.csv \
    --auto_clean

# Tìm duplicates trong chunk (xử lý từng phần)
python search_duplicates_aggregated.py \
    --collection video_dedup_v2 \
    --cosine_thresh 0.95 \
    --chunk_start 0 \
    --chunk_end 10000 \
    --unique_csv FINAL_RESULT_AGG.csv \
    --report_csv duplicate_videos_agg.csv \
    --auto_clean

# Fast mode (nhanh hơn 2-4x, độ chính xác giảm nhẹ)
python search_duplicates_aggregated.py \
    --collection video_dedup_v2 \
    --cosine_thresh 0.95 \
    --fast_mode \
    --batch_size 10 \
    --num_threads 8 \
    --auto_clean
```

**Chức năng:**
- Load embeddings từ Zilliz
- Pre-filtering: Loại bỏ cùng video ID với signature/itag khác nhau
- Pass 1: Tìm tất cả duplicate pairs bằng vector similarity search
- Pass 2: Nhóm thành clusters và chọn original (video có resolution cao nhất)
- Auto-clean: Loại bỏ PNG/images và URLs lỗi

**Tham số quan trọng:**
- `--cosine_thresh`: Ngưỡng similarity (0.0-1.0). Mặc định: 0.95
  - `0.95`: Cân bằng (khuyến nghị)
  - `0.98`: Chặt chẽ hơn (chỉ video gần như giống hệt)
  - `0.90`: Lỏng hơn (có thể bắt được video tương tự)
- `--chunk_start`, `--chunk_end`: Xử lý từng chunk (hữu ích cho dataset lớn)
- `--skip_url_dedup`: Bỏ qua pre-filtering (nếu video có URL giống nhưng nội dung khác)
- `--skip_cross_chunk`: Bỏ qua cross-chunk duplicate removal
- `--cross_chunk_threshold`: Ngưỡng cho cross-chunk duplicates (default: 0.98)
- `--fast_mode`: Sử dụng search params tối ưu (nhanh hơn 2-4x)
- `--batch_size`: Số video search cùng lúc (max: 10, default: 10)
- `--num_threads`: Số threads song song (default: 4)
- `--auto_clean`: Tự động loại bỏ invalid URLs

**Output:**
- `FINAL_RESULT_AGG.csv`: Danh sách URLs unique (1 cột: `decoded_url`)
- `duplicate_videos_agg.csv`: Report duplicates với mapping đến original
- `invalid_urls.csv`: Invalid URLs (nếu dùng `--auto_clean`)

---

### Bước 7: Clean Empty Jobs (Optional)

Dọn dẹp các job folder rỗng:

```bash
# Dry run (xem sẽ xóa gì)
python clean_empty_jobs.py --root batch_outputs --dry_run

# Thực sự xóa
python clean_empty_jobs.py --root batch_outputs
```

**Chức năng:**
- Tìm các folder `url_*` chỉ chứa `url.txt` (không có `.npy` files)
- Xóa các folder này để tiết kiệm dung lượng

---

## 📁 Chi Tiết Các File

### 1. `decode_urls.py`

**Mục đích:** Decode URLs từ CSV gốc

**Input:** `url-tvc.csv` (có thể có header: `tvc`, `url`, `links`)

**Output:** `url-tvc.decoded.csv` (1 cột: `decoded_url`)

**Chức năng:**
- Decode percent-encoding (`%20` → space)
- Fix protocol-relative URLs
- Validate URLs

---

### 2. `dedupe_urls.py`

**Mục đích:** Loại bỏ URL trùng lặp dựa trên URL string

**Input:** `url-tvc.decoded.csv`

**Output:**
- `url-tvc.unique.csv`: URLs unique
- `url-tvc.duplicates.csv`: URLs bị loại bỏ

**Chức năng:**
- Normalize URLs (lowercase, remove trailing slash)
- Hash table để tìm duplicates
- Giữ lại URL đầu tiên gặp

**Lưu ý:** Chỉ loại bỏ URL string trùng lặp, không phải video trùng lặp về nội dung.

---

### 3. `filter_valid_urls.py`

**Mục đích:** Lọc các URL hợp lệ, loại bỏ các URL bị 403/404 và HLS manifest không hợp lệ trước khi upload

**Input:** `url-tvc.unique.csv` (hoặc file CSV chứa URLs)

**Output:**
- `url-tvc.valid.csv`: URLs hợp lệ (status 200, bao gồm cả HLS manifest hợp lệ)
- `url-tvc.invalid.csv`: URLs lỗi với status code và error message (bao gồm HLS invalid)

**Chức năng:**
- Kiểm tra status code bằng HEAD request (nhanh hơn GET)
- **Phát hiện và kiểm tra HLS manifest URLs**: Tự động nhận diện HLS manifest (`.m3u8`, `/manifest/hls`) và kiểm tra với FFmpeg
- Xử lý đa luồng với thread pool
- Phân loại: 200 (OK), 403 (Forbidden), 404 (Not Found), HLS invalid, timeout, lỗi khác
- Báo cáo chi tiết số lượng từng loại lỗi

**Ưu điểm:**
- Tiết kiệm thời gian: Loại bỏ các URL không hợp lệ trước khi upload (bao gồm cả HLS manifest không hợp lệ)
- Tiết kiệm tài nguyên: Không cần xử lý các URL bị 403/404 hoặc HLS invalid
- Xử lý nhanh: HEAD request + đa luồng
- **Kiểm tra HLS**: Phát hiện sớm các HLS manifest URLs không thể xử lý được

**Yêu cầu:**
- FFmpeg (để kiểm tra HLS manifest URLs) - nếu không có, script vẫn chạy nhưng sẽ cảnh báo
- Có thể bỏ qua kiểm tra HLS bằng `--skip-hls-check` để tăng tốc

---

### 4. `create_collection.py`

**Mục đích:** Tạo collection mới trong Milvus/Zilliz

**Schema types:**
- `video_dedup`: 1 vector per video (khuyến nghị cho direct upload)
- `video_frames`: Multiple frames per video
- `aggregated`: Aggregated vectors

**Chức năng:**
- Tạo schema với fields: `id`, `url`, `job_id`, `embedding`
- Tạo index trên field `embedding`
- Load collection để sẵn sàng sử dụng

---

### 5. `direct_upload_to_zilliz.py`

**Mục đích:** Upload embeddings trực tiếp từ CSV lên Zilliz

**Input:** CSV file với URLs

**Chức năng:**
- Đọc URLs từ CSV
- Download video hoặc mở trực tiếp từ URL
- Extract frame đầu tiên
- Tạo CLIP embedding (512 dims, L2-normalized)
- Upload lên Zilliz với batch size tự động

**Ưu điểm:**
- Không cần lưu video local (tiết kiệm disk)
- Xử lý song song với batch
- Tự động retry khi lỗi

---

### 6. `search_duplicates_aggregated.py`

**Mục đích:** Tìm video trùng lặp dựa trên vector similarity

**Input:** Collection trong Zilliz

**Output:**
- `FINAL_RESULT_AGG.csv`: Unique URLs
- `duplicate_videos_agg.csv`: Duplicates report
- `invalid_urls.csv`: Invalid URLs (nếu dùng `--auto_clean`)

**Luồng xử lý:**

1. **Load Data**: Query embeddings từ Zilliz (có thể theo chunk)
2. **Pre-filtering** (nếu không dùng `--skip_url_dedup`):
   - Extract video ID từ URL (Google CDN, YouTube)
   - Nhóm videos theo video ID
   - Chọn video có itag cao nhất (resolution cao nhất)
   - Loại bỏ các video còn lại trong group
3. **Pass 1 - Find Pairs**:
   - Batch search với vector similarity
   - Tìm tất cả pairs có similarity >= threshold
   - Phân loại: within-chunk và cross-chunk pairs
4. **Pass 2 - Cluster & Select**:
   - Build graph từ duplicate pairs
   - DFS clustering với path validation (tránh transitive closure)
   - Chọn original: video có resolution cao nhất trong cluster
   - Xử lý cross-chunk duplicates
5. **Auto-clean** (nếu dùng `--auto_clean`):
   - Loại bỏ PNG/images
   - Loại bỏ URLs lỗi
6. **Write Results**: Ghi CSV files

**Tính năng đặc biệt:**
- ✅ Xử lý khác độ phân giải: Tự động chọn video có resolution cao nhất
- ✅ Xử lý khung hình to nhỏ: Dùng embeddings, không phụ thuộc pixel size
- ✅ Pre-filtering thông minh: Loại bỏ cùng video với signature/itag khác
- ✅ Cross-chunk detection: Phát hiện duplicates giữa chunks
- ✅ Path validation: Tránh transitive closure (A-B, B-C không có nghĩa A-C)

---

### 7. `clean_empty_jobs.py`

**Mục đích:** Dọn dẹp các job folder rỗng

**Chức năng:**
- Tìm các folder `url_*` chỉ chứa `url.txt` (không có `.npy`)
- Xóa các folder này

**Lưu ý:** Chỉ xóa folder không có embeddings (`.npy` files).

---

### 8. `create_product_embeddings_collection.py`

**Mục đích:** Tạo collection cho product embeddings (schema đặc biệt)

**Chức năng:** Tương tự `create_collection.py` nhưng với schema cho product embeddings

---

## 💡 Best Practices

### 1. Xử Lý Dataset Lớn

Nếu dataset > 10,000 videos, nên xử lý theo chunks:

```bash
# Lọc URL hợp lệ trước (khuyến nghị)
python filter_valid_urls.py --input urls.csv --output urls.valid.csv

# Upload từng chunk (sử dụng file đã lọc)
python direct_upload_to_zilliz.py --input urls.valid.csv --start 0 --end 10000
python direct_upload_to_zilliz.py --input urls.valid.csv --start 10000 --end 20000
# ...

# Tìm duplicates từng chunk
python search_duplicates_aggregated.py --collection video_dedup_v2 --chunk_start 0 --chunk_end 10000
python search_duplicates_aggregated.py --collection video_dedup_v2 --chunk_start 10000 --chunk_end 20000
# ...
```

### 2. Tối Ưu Performance

- **Fast mode**: Dùng `--fast_mode` để tăng tốc 2-4x (giảm độ chính xác nhẹ)
- **Batch size**: Tăng `--batch_size` lên 10 (max) và `--num_threads` lên 8-16
- **Chunk processing**: Xử lý từng chunk để tránh memory issues

### 3. Điều Chỉnh Threshold

- **0.95** (default): Cân bằng, phù hợp hầu hết trường hợp
- **0.98**: Chặt chẽ hơn, chỉ bắt video gần như giống hệt
- **0.90**: Lỏng hơn, có thể bắt được video tương tự (nhưng có thể có false positives)

### 4. Pre-filtering

- **Nên dùng** (mặc định): Nếu video có cùng video ID (Google CDN/YouTube) nhưng khác signature/itag
- **Không dùng** (`--skip_url_dedup`): Nếu video có URL giống nhưng nội dung khác nhau

### 5. Cross-chunk Duplicates

- **Nên dùng** (mặc định): Để loại bỏ duplicates giữa các chunks
- **Không dùng** (`--skip_cross_chunk`): Nếu muốn xử lý mỗi chunk độc lập

---

## 🔧 Troubleshooting

### Lỗi: "Collection not found"

**Giải pháp:**
```bash
python create_collection.py --collection video_dedup_v2 --schema video_dedup
```

### Lỗi: "Message larger than max"

**Nguyên nhân:** Batch size quá lớn khi query từ Zilliz

**Giải pháp:** Script tự động retry với batch size nhỏ hơn. Nếu vẫn lỗi, giảm `--chunk_end - --chunk_start`.

### Lỗi: "Memory error"

**Nguyên nhân:** Dataset quá lớn, không đủ RAM

**Giải pháp:**
- Xử lý theo chunks nhỏ hơn
- Dùng `--skip_url_dedup` để giảm memory usage
- Tăng RAM hoặc dùng máy có RAM lớn hơn

### Video giống nhau nhưng không bị phát hiện

**Nguyên nhân:** Threshold quá cao

**Giải pháp:**
- Giảm `--cosine_thresh` xuống 0.90-0.92
- Kiểm tra xem embeddings có được tạo đúng không

### Video khác nhau nhưng bị đánh dấu duplicate

**Nguyên nhân:** Threshold quá thấp hoặc video thực sự tương tự

**Giải pháp:**
- Tăng `--cosine_thresh` lên 0.98
- Kiểm tra manual một số cases trong `duplicate_videos_agg.csv`

### Upload chậm

**Giải pháp:**
- **Lọc URL hợp lệ trước**: Chạy `filter_valid_urls.py` để loại bỏ 403/404
- Kiểm tra network connection
- Đảm bảo video URLs accessible
- Xử lý theo chunks nhỏ hơn để tránh timeout

### Nhiều URL bị 403/404 hoặc HLS manifest không hợp lệ

**Giải pháp:**
- Chạy `filter_valid_urls.py` để lọc các URL hợp lệ trước khi upload
- Script sẽ tự động phát hiện và kiểm tra HLS manifest URLs với FFmpeg
- Sử dụng file `url-tvc.valid.csv` làm input cho `direct_upload_to_zilliz.py`
- Kiểm tra file `url-tvc.invalid.csv` để xem các URL bị lỗi (bao gồm cả HLS invalid)
- Nếu không có FFmpeg, cài đặt FFmpeg hoặc dùng `--skip-hls-check` để bỏ qua kiểm tra HLS

### Lỗi: "exceeded the limit number of collections"

**Nguyên nhân:** Zilliz Cloud plan của bạn đã đạt giới hạn số lượng collections (thường là 5 collections cho plan miễn phí/cơ bản).

**Giải pháp:**

1. **Xem collections hiện có:**
```bash
python list_collections.py
```

2. **Xóa collection không cần thiết:**
```bash
# Xóa với xác nhận
python delete_collection.py --collection collection_name

# Xóa không cần xác nhận (cẩn thận!)
python delete_collection.py --collection collection_name --force
```

3. **Hoặc sử dụng collection đã có:**
```bash
# Kiểm tra collection có sẵn
python list_collections.py

# Sử dụng collection đã có thay vì tạo mới
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection existing_collection_name
```

4. **Nâng cấp plan Zilliz** (nếu cần nhiều collections hơn):
- Đăng nhập vào Zilliz Cloud console
- Nâng cấp plan để có nhiều collections hơn

**Lưu ý:** 
- ⚠️ Xóa collection sẽ xóa **TẤT CẢ** dữ liệu trong collection đó (không thể hoàn tác!)
- Nên backup dữ liệu quan trọng trước khi xóa
- Kiểm tra kỹ collection nào cần xóa bằng `list_collections.py`

---

## 📊 Output Format

### `FINAL_RESULT_AGG.csv`

```csv
decoded_url
https://example.com/video1.mp4
https://example.com/video2.mp4
...
```

### `duplicate_videos_agg.csv`

```csv
duplicate_url,duplicate_job_id,original_job_id,original_url,similarity
https://example.com/video1_720p.mp4,url_0001,url_0000,https://example.com/video1_1080p.mp4,0.987654
...
```

### `invalid_urls.csv` (nếu dùng `--auto_clean`)

```csv
invalid_url,job_id,reason
https://example.com/image.png,url_1234,File ảnh (.png)
...
```

---

## 🎓 Ví Dụ Workflow Hoàn Chỉnh

```bash
# Bước 1: Decode URLs
python decode_urls.py --input url-tvc.csv --output url-tvc.decoded.csv

# Bước 2: Dedupe URLs (optional)
python dedupe_urls.py --input url-tvc.decoded.csv --output url-tvc.unique.csv

# Bước 3: Lọc URL hợp lệ (khuyến nghị - loại bỏ 403/404)
python filter_valid_urls.py --input url-tvc.unique.csv --output url-tvc.valid.csv --invalid url-tvc.invalid.csv

# Bước 4: Tạo collection (nếu chưa có)
python create_collection.py --collection video_dedup_v2 --schema video_dedup

# Bước 5: Upload embeddings (xử lý từng chunk 10k - sử dụng file đã lọc)
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection video_dedup_v2 --start 0 --end 10000
python direct_upload_to_zilliz.py --input url-tvc.valid.csv --collection video_dedup_v2 --start 10000 --end 20000
# ... tiếp tục cho đến hết

# Bước 6: Tìm duplicates (xử lý từng chunk)
python search_duplicates_aggregated.py \
    --collection video_dedup_v2 \
    --cosine_thresh 0.95 \
    --chunk_start 0 \
    --chunk_end 10000 \
    --unique_csv FINAL_RESULT_AGG_chunk_0_10000.csv \
    --report_csv duplicate_videos_agg_chunk_0_10000.csv \
    --auto_clean \
    --fast_mode

# Bước 7: Clean empty jobs (optional)
python clean_empty_jobs.py --root batch_outputs
```

---

## 📝 Notes

- **Embeddings**: Sử dụng CLIP model (`openai/clip-vit-base-patch32`) với 512 dimensions
- **Similarity metric**: Inner Product (IP) với L2-normalized vectors
- **Resolution detection**: Tự động extract từ itag (Google CDN) hoặc URL pattern
- **Job ID format**: `url_XXXX` với XXXX là số (4 digits với leading zeros)

---

## 🤝 Support

Nếu gặp vấn đề, kiểm tra:
1. Logs trong console output
2. File `duplicate_videos_agg.csv` để xem các duplicates được phát hiện
3. File `invalid_urls.csv` (nếu dùng `--auto_clean`) để xem URLs bị loại bỏ

---

**Happy Deduplicating! 🎉**

