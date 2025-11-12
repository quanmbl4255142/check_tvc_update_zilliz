# 🎬 Video Deduplication Tool - Optimized with Zilliz Cloud

Hệ thống tự động phát hiện và loại bỏ video trùng lặp sử dụng **CLIP embeddings** + **Zilliz Cloud vector database**.

---

## 📚 Phân tích dự án

### 📁 Nhiệm vụ các file trong dự án

#### **🔧 Core Processing Scripts**

| File | Nhiệm vụ | Mô tả |
|------|---------|-------|
| `decode_urls.py` | Giải mã URLs | Decode percent-encoding URLs từ CSV, chuẩn hóa format (fix protocol-relative URLs) |
| `dedupe_urls.py` | Loại bỏ URL trùng lặp | Text-based deduplication: normalize URLs, dùng hash table để tìm duplicates |
| `batch_extract_from_urls.py` | Trích xuất frames & embeddings | Stream video từ URL → Extract 1-3 frames → Tạo CLIP embeddings → Lưu local (batch_outputs/) |
| `direct_upload_to_zilliz.py` | ⭐ Upload trực tiếp | Extract frame → Tạo embedding → Upload Zilliz ngay (không lưu local) - **Khuyến nghị** |
| `upload_to_milvus.py` | Upload từ local | Đọc embeddings từ batch_outputs/ → Upload lên Zilliz (1 vector per frame) |
| `upload_aggregated_to_milvus.py` | Upload aggregated vectors | Gộp 3 frames thành 1 vector (average/max pooling) → Upload (tiết kiệm storage) |
| `search_duplicates_aggregated.py` | Tìm duplicates | ANN search trên Zilliz → Cosine similarity → Auto-clean invalid URLs (PNG/ảnh) |
| `clean_empty_jobs.py` | Dọn dẹp | Xóa các job folders thất bại (không có .npy files) |

#### **⚙️ Configuration & Utilities**

| File | Nhiệm vụ | Mô tả |
|------|---------|-------|
| `milvus_config.py` | Cấu hình Milvus/Zilliz | Quản lý connection params, index settings, batch size (hỗ trợ Zilliz Cloud, Milvus server, local) |
| `app.py` | Core utilities | CLIP model loading, embedding functions (`embed_image_clip_to_npy`), frame extraction |
| `test_milvus_connection.py` | Test kết nối | Kiểm tra connection, list collections, tạo test collection |
| `create_collection.py` | Tạo collection | Tạo collection mới với schema tùy chọn (video_dedup, video_frames, aggregated) |
| `list_collections.py` | Liệt kê collections | Hiển thị tất cả collections, schema, số lượng vectors, sample data |

---

### 🔄 Luồng xử lý tổng quan

#### **🚀 OPTION 1: Direct Upload Mode (Khuyến nghị)**

```
┌─────────────────────────────────────────────────────────────────┐
│                    INPUT: CSV với URLs                          │
│                    (tvcQc.csv - 600 URLs)                        │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        ┌────────────────────────────────────────┐
        │  1. decode_urls.py                     │
        │     • Decode percent-encoding          │
        │     • Fix protocol-relative URLs        │
        │     Output: tvcQc.decoded.csv           │
        └────────────────┬───────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │  2. dedupe_urls.py                     │
        │     • Normalize URLs (lowercase,       │
        │       remove trailing slash)           │
        │     • Hash table deduplication         │
        │     Output: tvcQc.unique.csv          │
        │            tvcQc.duplicates.csv        │
        └────────────────┬───────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │  3. direct_upload_to_zilliz.py         │
        │     For each video:                    │
        │     ├─ Stream video từ URL             │
        │     ├─ Extract first frame (0%)        │
        │     ├─ CLIP embedding (512 dims)        │
        │     ├─ L2 normalize                    │
        │     ├─ Upload batch (1000 vectors)     │
        │     └─ Delete temp files               │
        │     Output: Zilliz collection           │
        │            (video_dedup_direct)         │
        └────────────────┬───────────────────────┘
                         │
                         ▼
        ┌────────────────────────────────────────┐
        │  4. search_duplicates_aggregated.py   │
        │     For each video:                    │
        │     ├─ Query Zilliz (ANN search)      │
        │     ├─ Cosine similarity ≥ 0.85       │
        │     ├─ Mark as duplicate               │
        │     └─ Auto-clean (PNG/ảnh/invalid)    │
        │     Output: FINAL_RESULT.csv           │
        │            duplicates.csv               │
        │            invalid_urls.csv            │
        └────────────────────────────────────────┘
```

**Thời gian:** ~20-40 giờ cho 90k videos (có thể resume với `--start`/`--end`)

#### **💾 OPTION 2: Batch Mode (Lưu local)**

```
┌─────────────────────────────────────────────────────────────────┐
│                    INPUT: CSV với URLs                          │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
        [decode_urls.py] → [dedupe_urls.py] → [batch_extract_from_urls.py]
                                                      │
                                                      ▼
                                    ┌─────────────────────────────┐
                                    │  batch_outputs/             │
                                    │  ├── url_0000/              │
                                    │  │   ├── first_frame.npy    │
                                    │  │   └── url.txt            │
                                    │  ├── url_0001/              │
                                    │  └── ...                    │
                                    └──────────┬──────────────────┘
                                               │
                                               ▼
                                    [clean_empty_jobs.py]
                                               │
                                               ▼
                                    ┌─────────────────────────────┐
                                    │  Option A: upload_to_milvus │
                                    │  (1 frame = 1 vector)       │
                                    │                             │
                                    │  Option B: upload_aggregated│
                                    │  (3 frames → 1 vector)      │
                                    └──────────┬──────────────────┘
                                               │
                                               ▼
                                    [search_duplicates_aggregated.py]
                                               │
                                               ▼
                                    ┌─────────────────────────────┐
                                    │  FINAL_RESULT.csv          │
                                    └────────────────────────────┘
```

**Thời gian:** ~20-40 phút cho 500 videos

---

### 🛠️ Các thư viện Core

#### **📦 Core Libraries (requirements.txt)**

| Thư viện | Version | Nhiệm vụ |
|----------|---------|----------|
| **opencv-python** | ≥4.9.0 | Video processing: stream từ URL, extract frames, convert BGR→RGB |
| **Pillow** | ≥10.3.0 | Image processing: load/save PNG, convert numpy array → PIL Image |
| **transformers** | ≥4.44.0 | CLIP model: `AutoProcessor`, `CLIPModel` (openai/clip-vit-base-patch32) |
| **torch** | ≥2.3.0 | Deep learning backend: CLIP model inference (CPU/GPU) |
| **numpy** | ≥1.26.0 | Vector operations: L2 normalization, array manipulation |
| **pymilvus** | ≥2.3.0 | ⭐ **Vector database client**: connect Zilliz Cloud, insert/search vectors |
| **pandas** | ≥2.2.2 | CSV processing: read/write CSV files |
| **python-dotenv** | ≥1.0.1 | Environment variables: load `.env` file |
| **rich** | ≥13.7.1 | Terminal UI: progress bars, colored output |
| **click** | ≥8.1.7 | CLI framework: command-line argument parsing |
| **tqdm** | ≥4.66.0 | Progress bars: hiển thị tiến độ xử lý |
| **psutil** | ≥5.9.0 | System monitoring: RAM/CPU usage tracking |

#### **🔑 Key Dependencies Flow**

```
┌─────────────────────────────────────────────────────────────┐
│                    Video Processing                         │
│  opencv-python → Extract frames from video URL              │
│  Pillow → Convert BGR to RGB, save as PNG                   │
└────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Embedding Generation                     │
│  transformers → Load CLIP model (openai/clip-vit-base-patch32)│
│  torch → Run inference (CPU/GPU)                             │
│  numpy → L2 normalize vector (512 dims)                      │
└────────────────────┬────────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│                    Vector Database                          │
│  pymilvus → Connect Zilliz Cloud                             │
│           → Insert vectors (batch 1000)                      │
│           → ANN search (IVF_FLAT index)                       │
│           → Cosine similarity (IP metric)                    │
└─────────────────────────────────────────────────────────────┘
```

#### **📊 Data Flow**

```
CSV URLs → decode → dedupe → Extract → CLIP → Embedding (512d) → Zilliz
                                                                    │
                                                                    ▼
                                                              ANN Search
                                                                    │
                                                                    ▼
                                                              Duplicates
```

---

### 🚀 Lệnh chạy luồng trong dự án

#### **🔥 Luồng Direct Upload (Khuyến nghị - 4 bước)**

```powershell
# Bước 0: Kích hoạt virtual environment
.\venv\Scripts\Activate.ps1

# Bước 1: Giải mã URLs (percent-encoding)
python decode_urls.py --input tvcQc.csv --output tvcQc.decoded.csv
# Input:  tvcQc.csv (600 URLs)
# Output: tvcQc.decoded.csv (URLs đã decode)

# Bước 2: Loại bỏ URL trùng lặp (text-based)
python dedupe_urls.py --input tvcQc.decoded.csv --output tvcQc.unique.csv --report tvcQc.duplicates.csv
# Input:  tvcQc.decoded.csv
# Output: tvcQc.unique.csv (478 URLs unique)
#         tvcQc.duplicates.csv (122 URLs trùng)

# Bước 3: Upload trực tiếp lên Zilliz (extract + upload on-the-fly)
python direct_upload_to_zilliz.py --input tvcQc.unique.csv --column decoded_url --collection video_dedup_direct --end 90000
# Input:  tvcQc.unique.csv
# Process: Stream video → Extract first frame → CLIP embedding → Upload Zilliz
# Output: Zilliz collection (video_dedup_direct)
# Options: --start 0 --end 10000 (chia batch), --overwrite (ghi đè)

# Bước 4: Tìm duplicates + Auto-clean URLs
python search_duplicates_aggregated.py --collection video_dedup_direct --cosine_thresh 0.85 --unique_csv FINAL_RESULT.csv --report_csv duplicates.csv --auto_clean --invalid_csv invalid_urls.csv
# Input:  Zilliz collection (video_dedup_direct)
# Process: ANN search → Cosine similarity → Auto-clean (PNG/ảnh/invalid)
# Output: FINAL_RESULT.csv (37 videos duy nhất)
#         duplicates.csv (416 duplicates + scores)
#         invalid_urls.csv (4 invalid URLs)
```

**Tổng thời gian:** ~20-40 giờ cho 90k videos

---

#### **💾 Luồng Batch Mode (7 bước - Lưu local)**

```powershell
# Bước 0: Kích hoạt virtual environment
.\venv\Scripts\Activate.ps1

# Bước 1: Giải mã URLs
python decode_urls.py --input tvcQc.csv --output tvcQc.decoded.csv

# Bước 2: Loại bỏ URL trùng lặp
python dedupe_urls.py --input tvcQc.decoded.csv --output tvcQc.unique.csv --report tvcQc.duplicates.csv

# Bước 3: Extract frames & tạo embeddings (lưu local)
python batch_extract_from_urls.py --input tvcQc.unique.csv --column decoded_url --out_dir batch_outputs --num_frames 1
# Options: --num_frames 1 (fast) hoặc --num_frames 3 (better accuracy)
#          --start 0 --end 100 (chia batch)
# Output: batch_outputs/url_XXXX/first_frame.npy

# Bước 4: Dọn dẹp job folders thất bại
python clean_empty_jobs.py --root batch_outputs
# Xóa các folder không có .npy files

# Bước 5A: Upload 1 frame per video (mặc định)
python upload_to_milvus.py --root batch_outputs --collection video_dedup_simple
# Output: 457 vectors (1 vector per video)

# HOẶC Bước 5B: Upload aggregated vectors (nếu dùng 3 frames)
python upload_aggregated_to_milvus.py --root batch_outputs --collection video_dedup_aggregated --method average
# Output: 457 vectors (3 frames → 1 vector aggregated)

# Bước 6: Tìm duplicates + Auto-clean
python search_duplicates_aggregated.py --collection video_dedup_simple --cosine_thresh 0.85 --unique_csv FINAL_RESULT.csv --report_csv duplicates.csv --auto_clean --invalid_csv invalid_urls.csv
```

**Tổng thời gian:** ~20-40 phút cho 500 videos

---

#### **🔧 Utility Commands**

```powershell
# Test kết nối Zilliz
python test_milvus_connection.py
# Kiểm tra connection, list collections, tạo test collection

# Tạo collection mới
python create_collection.py --collection my_collection --schema video_dedup
# Options: --schema video_dedup | video_frames | aggregated

# Liệt kê tất cả collections
python list_collections.py
# Hiển thị schema, số lượng vectors, sample data
```

---

