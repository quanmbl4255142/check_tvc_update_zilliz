# FLOWCHART: Luồng Lọc Trùng - search_duplicates_aggregated.py

## ✨ CẬP NHẬT MỚI (2025)

### Các Cải Tiến Chính:
- ✅ **Video ID Extraction**: Hỗ trợ 8 CDN mới (FlashTalking, FPT Play, Adnxs, VZ CDN, UpPremium, BlueAdss, Adsrvr, AIActiv)
- ✅ **Resolution Extraction**: Hỗ trợ nhiều patterns mới (FlashTalking, Adnxs, VZ CDN, query params)
- ✅ **Best Quality Selection**: Dùng `resolution_score` (width × height) thay vì chỉ itag
- ✅ **Cross-chunk Detection**: Phát hiện duplicates giữa các chunks với threshold riêng
- ✅ **Path Validation**: DFS với validation để tránh transitive closure

---

## Mermaid Flowchart - Tổng Quan

```mermaid
flowchart TD
    Start([Bắt đầu]) --> Connect[🔌 Kết nối Milvus<br/>Load Collection]
    
    Connect --> CheckChunk{📦 Chunk Mode?}
    
    CheckChunk -->|Có| QueryChunk[Query theo job_id range<br/>url_XXXX format<br/>Batch query để tránh message size limit]
    CheckChunk -->|Không| QueryAll[Query tất cả videos<br/>Sử dụng offset/limit + ID range]
    
    QueryChunk --> LoadData[📥 Load embeddings + URLs<br/>all_data = job_id, url, embedding]
    QueryAll --> LoadData
    
    LoadData --> BuildVideoInfo["🔧 Xây dựng video_info map<br/>job_id to url, embedding"]
    
    BuildVideoInfo --> CheckSkipURL{"--skip_url_dedup?"}
    
    CheckSkipURL -->|Có| SkipPreFilter[⏭️ Bỏ qua Pre-filtering<br/>Giữ tất cả videos]
    CheckSkipURL -->|Không| PreFilter[🔍 PRE-FILTERING]
    
    PreFilter --> ExtractVideoID[📋 Extract Video ID từ URL<br/>✨ 8 CDN được hỗ trợ:<br/>- Google CDN, YouTube<br/>- FlashTalking, FPT Play<br/>- Adnxs, VZ CDN<br/>- UpPremium, BlueAdss<br/>- Adsrvr, AIActiv]
    
    ExtractVideoID --> GroupByID["📊 Nhóm videos theo Video ID<br/>video_id_groups: video_id to list of job_ids"]
    
    GroupByID --> CheckChunkMode{Chunk Mode?}
    
    CheckChunkMode -->|Có| CrossChunkCheck[🔗 Kiểm tra Video ID<br/>trong chunks khác<br/>Batch query để tối ưu]
    CheckChunkMode -->|Không| SelectBest[⭐ Chọn video tốt nhất<br/>trong mỗi group]
    
    CrossChunkCheck --> CheckExist{Video ID exists<br/>in other chunk<br/>with smaller job_id?}
    
    CheckExist -->|Có| RemoveCrossChunk[🗑️ Loại bỏ toàn bộ group<br/>Đã tồn tại ở chunk khác]
    CheckExist -->|Không| SelectBest
    
    RemoveCrossChunk --> Pass1
    
    SelectBest --> ExtractResolution[📐 Extract Resolution từ URL<br/>✨ Hỗ trợ nhiều patterns:<br/>- FlashTalking: _width_height_bitrate_fps<br/>- Adnxs: _width_height_bitratek<br/>- VZ CDN: play_1080p<br/>- Query params: ?width=1920&height=1080]
    
    ExtractResolution --> CalculateScore["📊 Tính resolution_score<br/>score = width x height<br/>Hoặc itag nếu không có resolution"]
    
    CalculateScore --> SortByQuality["🔢 Sort videos:<br/>1. resolution_score DESC<br/>2. itag DESC<br/>3. job_id ASC"]
    
    SortByQuality --> SelectBestQuality[✅ Chọn video có resolution cao nhất<br/>Giữ lại trong group]
    
    SelectBestQuality --> RemoveDupURL[🗑️ Xóa URL duplicates<br/>Giữ 1 video/group<br/>Loại bỏ các video còn lại]
    
    RemoveDupURL --> Pass1
    SkipPreFilter --> Pass1[🔍 PASS 1: Tìm Duplicate Pairs]
    
    Pass1 --> BatchVideos[📦 Chia videos thành batches<br/>batch_size = 10 max<br/>Zilliz limit]
    
    BatchVideos --> ParallelSearch[⚡ Parallel Search với threads<br/>num_threads threads<br/>Search trên Milvus/Zilliz]
    
    ParallelSearch --> CollectPairs[📋 Thu thập duplicate pairs<br/>job_id1, job_id2, similarity<br/>Thread-safe collection]
    
    CollectPairs --> FilterThreshold["✅ Lọc pairs:<br/>similarity >= threshold<br/>Validate score: 0.0 to 1.0"]
    
    FilterThreshold --> NormalizePairs["🔄 Normalize pairs<br/>Sort job_ids để tránh trùng<br/>Remove duplicate pairs"]
    
    NormalizePairs --> SeparatePairs["📊 Tách pairs:<br/>- within-chunk pairs<br/>- cross-chunk pairs"]
    
    SeparatePairs --> Pass2[🔗 PASS 2: Clustering & Chọn Originals]
    
    Pass2 --> ProcessCrossChunk[🌐 Xử lý Cross-chunk Duplicates]
    
    ProcessCrossChunk --> CheckCrossThreshold{"similarity >=<br/>cross_chunk_threshold?<br/>default: 0.98"}
    
    CheckCrossThreshold -->|Có| CheckOriginal{"Original ở<br/>chunk khác?"}
    CheckCrossThreshold -->|Không| SkipCrossDup[⏭️ Bỏ qua pair này<br/>Similarity quá thấp]
    
    CheckOriginal -->|Có| MarkCrossDup[🏷️ Đánh dấu duplicate<br/>nếu original ở chunk khác<br/>Thêm vào cross_chunk_duplicates]
    CheckOriginal -->|Không| SkipCrossDup
    
    MarkCrossDup --> BuildGraph
    SkipCrossDup --> BuildGraph["🕸️ Xây dựng Graph<br/>job_id to neighbors<br/>Chỉ within-chunk pairs"]
    
    BuildGraph --> BuildSimilarityDict["📚 Xây dựng similarity lookup<br/>job_id1, job_id2 to similarity<br/>Dict để tối ưu lookup"]
    
    BuildSimilarityDict --> DFS["🔍 DFS với Path Validation<br/>Max path length = 2<br/>Path similarity >= threshold x 0.95<br/>Tránh transitive closure"]
    
    DFS --> FindClusters["📊 Tìm Connected Components<br/>Mỗi cluster = 1 nhóm duplicates<br/>Iterative DFS để tránh recursion limit"]
    
    FindClusters --> ProcessClusters[🔄 Xử lý từng Cluster]
    
    ProcessClusters --> ExtractResCluster[📐 Extract Resolution cho mỗi video<br/>trong cluster]
    
    ExtractResCluster --> SortByQualityCluster[🔢 Sort videos trong cluster:<br/>1. resolution_score DESC<br/>2. job_id ASC]
    
    SortByQualityCluster --> SelectOriginal[✅ Chọn original<br/>Video có resolution cao nhất<br/>Thêm vào unique_videos]
    
    SelectOriginal --> AddDuplicates[📝 Thêm duplicates vào<br/>duplicates list<br/>Với similarity từ lookup dict]
    
    AddDuplicates --> CheckStandalone{"Còn videos<br/>standalone?<br/>Không trong cluster"}
    
    CheckStandalone -->|Có| AddStandalone[➕ Thêm standalone videos<br/>vào unique_videos<br/>Không có duplicates]
    CheckStandalone -->|Không| AddCrossDup
    
    AddStandalone --> AddCrossDup["🌐 Thêm cross-chunk duplicates<br/>vào duplicates list<br/>Mark CROSS-CHUNK trong original_url"]
    
    AddCrossDup --> CheckAutoClean{"--auto_clean?"}
    
    CheckAutoClean -->|Có| ValidateURL["🧼 Kiểm tra URL hợp lệ<br/>Loại bỏ PNG/images<br/>Loại bỏ URLs lỗi<br/>Kiểm tra domain, extension"]
    CheckAutoClean -->|Không| WriteResults
    
    ValidateURL --> SeparateValid["📊 Tách valid/invalid URLs<br/>valid_videos vs invalid_urls"]
    
    SeparateValid --> WriteResults[💾 Ghi kết quả]
    
    WriteResults --> WriteUnique[📄 Ghi unique_csv<br/>Danh sách URLs unique<br/>decoded_url]
    
    WriteUnique --> WriteReport[📋 Ghi report_csv<br/>Duplicates với original mapping<br/>duplicate_url, original_url, similarity]
    
    WriteReport --> CheckInvalid{Invalid URLs?}
    
    CheckInvalid -->|Có| WriteInvalid[📄 Ghi invalid_csv<br/>Invalid URLs report<br/>url, job_id, reason]
    CheckInvalid -->|Không| PerformanceReport
    
    WriteInvalid --> PerformanceReport[📊 Performance Report<br/>Timing, RAM, CPU<br/>Phase breakdown]
    
    PerformanceReport --> End([✅ Kết thúc])
    
    style Start fill:#90EE90
    style End fill:#FFB6C1
    style PreFilter fill:#FFE4B5
    style Pass1 fill:#ADD8E6
    style Pass2 fill:#DDA0DD
    style WriteResults fill:#F0E68C
    style ExtractVideoID fill:#E6E6FA
    style ExtractResolution fill:#E6E6FA
    style DFS fill:#FFB6C1
```

---

## Flowchart Chi Tiết - Các Phase

```mermaid
flowchart LR
    subgraph Phase1["PHASE 1: LOAD DATA"]
        A1[🔌 Connect Milvus] --> A2[📦 Load Collection]
        A2 --> A3{Chunk Mode?}
        A3 -->|Có| A4[Query by job_id range<br/>Batch query 100 videos/batch]
        A3 -->|Không| A5[Query all videos<br/>offset/limit + ID range]
        A4 --> A6[📥 Load Embeddings + URLs]
        A5 --> A6
        A6 --> A7[🔧 Build video_info map]
    end
    
    subgraph Phase2["PHASE 2: PRE-FILTER"]
        B1{--skip_url_dedup?} -->|Có| B2[⏭️ Skip Pre-filtering]
        B1 -->|Không| B3[📋 Extract Video ID<br/>8 CDN patterns]
        B3 --> B4[📊 Group by Video ID]
        B4 --> B5{Chunk Mode?}
        B5 -->|Có| B6[🔗 Check cross-chunk<br/>Batch query other chunks]
        B5 -->|Không| B7[⭐ Select Best Quality]
        B6 --> B8{Exists in<br/>other chunk?}
        B8 -->|Có| B9[🗑️ Remove Group]
        B8 -->|Không| B7
        B7 --> B10[📐 Extract Resolution<br/>Multiple patterns]
        B10 --> B11[📊 Calculate Score<br/>width × height]
        B11 --> B12[🔢 Sort & Select<br/>Highest resolution]
        B12 --> B13[🗑️ Remove Duplicates]
        B9 --> B13
        B2 --> B13
    end
    
    subgraph Phase3["PHASE 3: PASS 1 - FIND PAIRS"]
        C1[📦 Batch Videos<br/>max 10/batch] --> C2[⚡ Parallel Search<br/>num_threads threads]
        C2 --> C3[📋 Collect Pairs<br/>Thread-safe]
        C3 --> C4[✅ Filter Threshold<br/>similarity >= threshold]
        C4 --> C5[🔄 Normalize Pairs<br/>Remove duplicates]
        C5 --> C6[📊 Separate Pairs<br/>within-chunk vs cross-chunk]
    end
    
    subgraph Phase4["PHASE 4: PASS 2 - CLUSTER"]
        D1{--skip_cross_chunk?} -->|Có| D2[⏭️ Skip Cross-chunk]
        D1 -->|Không| D3[🌐 Process Cross-chunk<br/>Check threshold 0.98]
        D3 --> D4[🏷️ Mark Cross-chunk Duplicates]
        D2 --> D5[🕸️ Build Graph<br/>job_id → neighbors]
        D4 --> D5
        D5 --> D6[📚 Build Similarity Dict<br/>Optimize lookup]
        D6 --> D7[🔍 DFS with Validation<br/>Max path = 2<br/>Path sim >= threshold × 0.95]
        D7 --> D8[📊 Find Clusters<br/>Connected components]
        D8 --> D9[🔄 Process Clusters]
        D9 --> D10[📐 Extract Resolution]
        D10 --> D11[🔢 Sort by Quality<br/>resolution_score DESC]
        D11 --> D12[✅ Select Original<br/>Highest resolution]
        D12 --> D13[📝 Add Duplicates]
        D13 --> D14[➕ Add Standalone]
        D14 --> D15[🌐 Add Cross-chunk]
    end
    
    subgraph Phase5["PHASE 5: OUTPUT"]
        E1{--auto_clean?} -->|Có| E2[🧼 Validate URLs<br/>Remove PNG/images]
        E1 -->|Không| E3[💾 Write CSV]
        E2 --> E4[📊 Separate Valid/Invalid]
        E4 --> E3
        E3 --> E5[📄 unique_csv]
        E5 --> E6[📋 report_csv]
        E6 --> E7{Invalid URLs?}
        E7 -->|Có| E8[📄 invalid_csv]
        E7 -->|Không| E9[📊 Performance Report]
        E8 --> E9
    end
    
    Phase1 --> Phase2
    Phase2 --> Phase3
    Phase3 --> Phase4
    Phase4 --> Phase5
```

---

## Decision Points Chi Tiết

```mermaid
flowchart TD
    subgraph Decisions["Các Điểm Quyết Định"]
        D1{📦 Chunk Mode?}
        D2{⏭️ --skip_url_dedup?}
        D3{🔗 Video ID exists<br/>in other chunk<br/>with smaller job_id?}
        D4{🌐 similarity >=<br/>cross_chunk_threshold?<br/>default: 0.98}
        D5{"🔍 Path similarity >=<br/>threshold x 0.95?<br/>Max path length = 2"}
        D6{🧼 --auto_clean?}
        D7{✅ URL valid?<br/>Not PNG/image<br/>Has video indicator}
        D8{📊 Video in cluster?}
    end
    
    D1 -->|Có| QueryByJobID[Query by job_id range<br/>Batch query 100/batch<br/>Handle message size limit]
    D1 -->|Không| QueryAll[Query all videos<br/>offset/limit + ID range]
    
    D2 -->|Có| SkipPreFilter[⏭️ Skip Pre-filtering<br/>Giữ tất cả videos]
    D2 -->|Không| DoPreFilter[🔍 Do Pre-filtering<br/>Extract Video ID<br/>Group & Select Best]
    
    D3 -->|Có| RemoveGroup[🗑️ Remove entire group<br/>Đã tồn tại ở chunk khác]
    D3 -->|Không| KeepGroup[✅ Keep group<br/>Select best quality]
    
    D4 -->|Có| MarkAsDup[🏷️ Mark as duplicate<br/>nếu original ở chunk khác]
    D4 -->|Không| SkipPair[⏭️ Skip pair<br/>Similarity quá thấp]
    
    D5 -->|Có| AddToCluster[✅ Add to cluster<br/>Path similarity OK]
    D5 -->|Không| StopPath[⏹️ Stop path<br/>Similarity dropped]
    
    D6 -->|Có| ValidateURLs[🧼 Validate URLs<br/>Remove invalid]
    D6 -->|Không| WriteAll[💾 Write all results]
    
    D7 -->|Có| KeepURL[✅ Keep URL<br/>Add to valid_videos]
    D7 -->|Không| RemoveURL[🗑️ Remove URL<br/>Add to invalid_urls]
    
    D8 -->|Có| ProcessCluster[🔄 Process in cluster<br/>Select original]
    D8 -->|Không| AddStandalone[➕ Add as standalone<br/>No duplicates]
```

---

## Data Flow

```mermaid
flowchart LR
    subgraph Input["📥 INPUT"]
        I1[(Milvus Collection<br/>job_id, url, embedding)]
        I2[Parameters:<br/>threshold, batch_size,<br/>chunk_start, chunk_end,<br/>skip_url_dedup, skip_cross_chunk]
    end
    
    subgraph Processing["⚙️ PROCESSING"]
        P1[all_data:<br/>List of {job_id, url, embedding}]
        P2["video_info:<br/>Dict: job_id to {url, embedding}"]
        P3["video_id_groups:<br/>Dict: video_id to list of job_ids"]
        P4["duplicate_pairs:<br/>List of (job_id1, job_id2, similarity)"]
        P5["chunk_duplicate_pairs:<br/>Within-chunk pairs"]
        P6["cross_chunk_pairs:<br/>Cross-chunk pairs"]
        P7["similarity_lookup:<br/>Dict: (job_id1, job_id2) to similarity"]
        P8["graph:<br/>Dict: job_id to Set of neighbors"]
        P9["clusters:<br/>List of Set of job_ids"]
        P10[originals:<br/>Set of job_ids]
        P11[unique_videos:<br/>List of {url, job_id}]
        P12[duplicates:<br/>List of {duplicate_url, original_url, similarity}]
    end
    
    subgraph Output["📤 OUTPUT"]
        O1[(unique_csv:<br/>Unique URLs)]
        O2[(report_csv:<br/>Duplicates report)]
        O3[(invalid_csv:<br/>Invalid URLs<br/>if --auto_clean)]
    end
    
    I1 --> P1
    I2 --> P1
    P1 --> P2
    P2 --> P3
    P3 --> P2
    P2 --> P4
    P4 --> P5
    P4 --> P6
    P5 --> P7
    P7 --> P8
    P8 --> P9
    P9 --> P10
    P10 --> P11
    P9 --> P12
    P6 --> P12
    P11 --> O1
    P12 --> O2
    P2 --> O3
```

---

## Critical Logic Flow - Chi Tiết

```mermaid
flowchart TD
    Start --> Load[📥 Load Videos from Milvus]
    Load --> PreFilter{🔍 Pre-filter?<br/>--skip_url_dedup?}
    
    PreFilter -->|No| ExtractID[📋 Extract Video ID<br/>✨ 8 CDN patterns:<br/>- Google CDN: gcdn_id_XXX<br/>- YouTube: youtube_XXX<br/>- FlashTalking: flashtalking_account_base<br/>- FPT Play: fptplay_id<br/>- Adnxs: adnxs_creative_uuid<br/>- VZ CDN: vzcdn_uuid<br/>- UpPremium: upremium_filename<br/>- BlueAdss: blueadss_path_filename<br/>- Adsrvr: adsrvr_filename<br/>- AIActiv: aiactiv_base]
    
    ExtractID --> Group[📊 Group by Video ID<br/>video_id_groups]
    
    Group --> CheckChunk{📦 Chunk Mode?}
    
    CheckChunk -->|Yes| CheckExist{🔗 Video ID exists<br/>in other chunk<br/>with smaller job_id?}
    CheckChunk -->|No| SelectBest
    
    CheckExist -->|Yes| Remove[🗑️ Remove Entire Group<br/>Đã tồn tại ở chunk khác]
    CheckExist -->|No| SelectBest[⭐ Select Best Video]
    
    Remove --> Search
    
    SelectBest --> ExtractRes[📐 Extract Resolution<br/>✨ Multiple patterns:<br/>- FlashTalking: _width_height_bitrate_fps<br/>- Adnxs: _width_height_bitratek<br/>- VZ CDN: play_1080p<br/>- Query params: ?width=1920&height=1080<br/>- Standard: 1920x1080, 1080p]
    
    ExtractRes --> CalcScore["📊 Calculate Score<br/>resolution_score = width x height<br/>Fallback: itag"]
    
    CalcScore --> Sort[🔢 Sort by:<br/>1. resolution_score DESC<br/>2. itag DESC<br/>3. job_id ASC]
    
    Sort --> KeepBest[✅ Keep Best Quality<br/>Remove others in group]
    
    KeepBest --> Search
    PreFilter -->|Yes| Search[🔍 Search Duplicates<br/>Vector Similarity]
    
    Search --> FindPairs[📋 Find Duplicate Pairs<br/>Batch parallel search<br/>top_k results per video]
    
    FindPairs --> Separate{📊 Within-chunk<br/>or Cross-chunk?}
    
    Separate -->|Cross-chunk| CheckSim{🌐 Sim >=<br/>cross_chunk_threshold?<br/>default: 0.98}
    CheckSim -->|Yes| CheckOrig{Original ở<br/>chunk khác?}
    CheckSim -->|No| SkipCross
    CheckOrig -->|Yes| MarkDup[🏷️ Mark as Duplicate<br/>Add to cross_chunk_duplicates]
    CheckOrig -->|No| SkipCross[⏭️ Skip]
    MarkDup --> Cluster
    
    Separate -->|Within-chunk| Cluster[🕸️ Build Graph & Cluster]
    
    Cluster --> BuildDict[📚 Build Similarity Dict<br/>Optimize lookup]
    
    BuildDict --> DFS["🔍 DFS with Validation<br/>Max path length = 2<br/>Path similarity >= threshold x 0.95<br/>Prevent transitive closure"]
    
    DFS --> FindClusters[📊 Find Clusters<br/>Connected components]
    
    FindClusters --> ProcessCluster[🔄 Process Each Cluster]
    
    ProcessCluster --> ExtractResCluster[📐 Extract Resolution<br/>for each video in cluster]
    
    ExtractResCluster --> CalcScoreCluster[📊 Calculate Score<br/>for each video]
    
    CalcScoreCluster --> SortCluster[🔢 Sort by Quality<br/>resolution_score DESC<br/>job_id ASC]
    
    SortCluster --> SelectOrig[✅ Select Original<br/>Highest resolution<br/>Add to unique_videos]
    
    SelectOrig --> AddDups[📝 Add Duplicates<br/>to duplicates list]
    
    AddDups --> CheckStandalone{📊 Standalone<br/>videos?}
    
    CheckStandalone -->|Yes| AddStandalone[➕ Add Standalone<br/>to unique_videos]
    CheckStandalone -->|No| AddCross
    
    AddStandalone --> AddCross[🌐 Add Cross-chunk<br/>to duplicates list]
    
    AddCross --> Clean{🧼 Auto-clean?<br/>--auto_clean?}
    
    Clean -->|Yes| Validate[✅ Validate URLs<br/>Remove PNG/images<br/>Remove invalid URLs]
    Validate --> Write
    Clean -->|No| Write[💾 Write Results]
    
    Write --> WriteUnique[📄 Write unique_csv]
    WriteUnique --> WriteReport[📋 Write report_csv]
    WriteReport --> CheckInvalid{Invalid URLs?}
    CheckInvalid -->|Yes| WriteInvalid[📄 Write invalid_csv]
    CheckInvalid -->|No| PerfReport
    WriteInvalid --> PerfReport[📊 Performance Report]
    
    PerfReport --> End
    
    style Start fill:#90EE90
    style End fill:#FFB6C1
    style PreFilter fill:#FFE4B5
    style Search fill:#ADD8E6
    style Cluster fill:#DDA0DD
    style Write fill:#F0E68C
    style ExtractID fill:#E6E6FA
    style ExtractRes fill:#E6E6FA
    style DFS fill:#FFB6C1
```

---

## Video ID Extraction Patterns

```mermaid
flowchart TD
    URL[Input URL] --> Check1{Google CDN?}
    Check1 -->|Yes| G1[Extract: gcdn_id_XXXXX<br/>Pattern: /videoplayback/id/HEX_ID/]
    Check1 -->|No| Check2{YouTube?}
    
    Check2 -->|Yes| Y1[Extract: youtube_XXXXX<br/>Pattern: v=XXXXX or youtu.be/XXXXX]
    Check2 -->|No| Check3{FlashTalking?}
    
    Check3 -->|Yes| F1[Extract: flashtalking_account_base<br/>Pattern: cdn.flashtalking.com/account/filename<br/>Remove: _width_height_bitrate_fps]
    Check3 -->|No| Check4{FPT Play?}
    
    Check4 -->|Yes| FP1[Extract: fptplay_id<br/>Pattern: static/banner/date/id.mp4<br/>ID = part after underscore]
    Check4 -->|No| Check5{Adnxs?}
    
    Check5 -->|Yes| A1[Extract: adnxs_creative_uuid<br/>Pattern: creative_id/uuid]
    Check5 -->|No| Check6{VZ CDN?}
    
    Check6 -->|Yes| V1[Extract: vzcdn_uuid<br/>Pattern: vz-XXX.b-cdn.net/uuid/]
    Check6 -->|No| Check7{UpPremium?}
    
    Check7 -->|Yes| U1[Extract: upremium_filename<br/>Remove timestamp prefix]
    Check7 -->|No| Check8{BlueAdss?}
    
    Check8 -->|Yes| B1[Extract: blueadss_path_filename<br/>Combine path + filename]
    Check8 -->|No| Check9{Adsrvr?}
    
    Check9 -->|Yes| AD1[Extract: adsrvr_filename<br/>Pattern: v.adsrvr.org/.../filename]
    Check9 -->|No| Check10{AIActiv?}
    
    Check10 -->|Yes| AI1[Extract: aiactiv_base<br/>Remove numeric suffix]
    Check10 -->|No| Check11{FPT VOD?}
    
    Check11 -->|Yes| FV1[Extract: fptplay_vod_hash<br/>Pattern: vod/transcoded/HASH/]
    Check11 -->|No| Empty[Return: ""<br/>No Video ID found]
    
    G1 --> Return[Return Video ID]
    Y1 --> Return
    F1 --> Return
    FP1 --> Return
    A1 --> Return
    V1 --> Return
    U1 --> Return
    B1 --> Return
    AD1 --> Return
    AI1 --> Return
    FV1 --> Return
    Empty --> Return
```

---

## Resolution Extraction Patterns

```mermaid
flowchart TD
    URL[Input URL] --> Check1{Has itag?<br/>Google CDN}
    Check1 -->|Yes| I1[Map itag to resolution<br/>348→4K, 37→1080p, 22→720p, etc.]
    Check1 -->|No| Check2{Has pattern<br/>1920x1080 or 1920_1080?}
    
    Check2 -->|Yes| P1[Extract: width x height<br/>Validate: 100-7680 x 100-4320]
    Check2 -->|No| Check3{FlashTalking pattern?<br/>_width_height_bitrate_fps}
    
    Check3 -->|Yes| F1[Extract: width_height<br/>Pattern: _1920_1080_2500_3000]
    Check3 -->|No| Check4{Adnxs pattern?<br/>uuid_width_height_bitratek}
    
    Check4 -->|Yes| A1[Extract: width_height<br/>Pattern: uuid_1280_720_600k]
    Check4 -->|No| Check5{VZ CDN pattern?<br/>play_1080p}
    
    Check5 -->|Yes| V1[Map: play_1080p → 1920x1080<br/>play_720p → 1280x720<br/>Calculate 16:9 if needed]
    Check5 -->|No| Check6{Query params?<br/>?width=1920&height=1080}
    
    Check6 -->|Yes| Q1[Extract from params<br/>width & height or w & h]
    Check6 -->|No| Check7{Resolution keywords?<br/>1080p, 720p, 4k}
    
    Check7 -->|Yes| K1[Map keywords to resolution<br/>1080p→1920x1080, 720p→1280x720]
    Check7 -->|No| Zero[Return: 0, 0<br/>No resolution found]
    
    I1 --> Return[Return width, height]
    P1 --> Return
    F1 --> Return
    A1 --> Return
    V1 --> Return
    Q1 --> Return
    K1 --> Return
    Zero --> Return
```

---

## Pre-filtering Logic Flow

```mermaid
flowchart TD
    Start[Start Pre-filtering] --> Extract[Extract Video ID<br/>for each video]
    
    Extract --> Group[Group videos by Video ID<br/>video_id_groups]
    
    Group --> Stats[📊 Statistics:<br/>- Videos with ID<br/>- Videos without ID<br/>- Unique video IDs]
    
    Stats --> CheckChunk{Chunk Mode?}
    
    CheckChunk -->|Yes| QueryOther[Query other chunks<br/>Batch query 5k/batch<br/>Find existing video IDs]
    CheckChunk -->|No| ProcessGroups
    
    QueryOther --> FindExisting[Find video IDs<br/>with smaller job_id<br/>in other chunks]
    
    FindExisting --> MarkSkip[Mark video_ids_to_skip<br/>Remove entire groups]
    
    MarkSkip --> ProcessGroups[Process each group]
    
    ProcessGroups --> CheckSkip{Video ID<br/>in skip list?}
    
    CheckSkip -->|Yes| RemoveAll[🗑️ Remove all videos<br/>in this group<br/>Already exists in other chunk]
    CheckSkip -->|No| CheckMultiple{Group has<br/>> 1 video?}
    
    CheckMultiple -->|No| KeepSingle[✅ Keep single video<br/>No duplicates in group]
    CheckMultiple -->|Yes| ExtractRes[📐 Extract Resolution<br/>for each video in group]
    
    ExtractRes --> CalcScores["📊 Calculate resolution_score<br/>for each video<br/>score = width x height"]
    
    CalcScores --> Sort[🔢 Sort by:<br/>1. resolution_score DESC<br/>2. itag DESC<br/>3. job_id ASC]
    
    Sort --> SelectBest[✅ Select best quality<br/>Highest resolution]
    
    SelectBest --> RemoveOthers[🗑️ Remove other videos<br/>in group]
    
    RemoveAll --> Summary
    KeepSingle --> Summary
    RemoveOthers --> Summary[📊 Summary:<br/>- URL duplicates removed<br/>- Videos filtered]
    
    Summary --> End[End Pre-filtering]
```

---

## Clustering Logic Flow

```mermaid
flowchart TD
    Start[Start Clustering] --> BuildDict["📚 Build Similarity Dict<br/>job_id1, job_id2 to similarity<br/>Normalize: sort job_ids"]
    
    BuildDict --> BuildGraph["🕸️ Build Graph<br/>job_id to Set of neighbors<br/>Exclude cross-chunk duplicates"]
    
    BuildGraph --> InitDFS["🔍 Initialize DFS<br/>visited = set<br/>clusters = list"]
    
    InitDFS --> Iterate[Iterate through<br/>all job_ids<br/>Sorted by job_id number]
    
    Iterate --> CheckVisited{Already<br/>visited?}
    
    CheckVisited -->|Yes| Next[Next job_id]
    CheckVisited -->|No| StartDFS[Start DFS<br/>from this node]
    
    StartDFS --> DFSStack[DFS Stack:<br/>node, path, min_similarity]
    
    DFSStack --> CheckPath{Path length<br/>> max_path_length?<br/>default: 2}
    
    CheckPath -->|Yes| StopPath[⏹️ Stop this path<br/>Too long]
    CheckPath -->|No| CheckSim{"Path similarity<br/>< threshold x 0.95?"}
    
    CheckSim -->|Yes| StopPath
    CheckSim -->|No| AddNode[✅ Add node to cluster<br/>Mark as visited]
    
    AddNode --> GetNeighbors[Get neighbors<br/>from graph<br/>Sorted by job_id]
    
    GetNeighbors --> CheckNeighbor{Neighbor<br/>visited or in path?}
    
    CheckNeighbor -->|Yes| NextNeighbor[Next neighbor]
    CheckNeighbor -->|No| AddToStack[Add to DFS stack<br/>Update path & min_similarity]
    
    AddToStack --> DFSStack
    NextNeighbor --> CheckMore{More<br/>neighbors?}
    CheckMore -->|Yes| GetNeighbors
    CheckMore -->|No| CheckStack{Stack<br/>empty?}
    
    CheckStack -->|No| DFSStack
    CheckStack -->|Yes| SaveCluster[💾 Save cluster<br/>Add to clusters list]
    
    StopPath --> CheckStack
    SaveCluster --> Next
    Next --> CheckMoreNodes{More<br/>job_ids?}
    
    CheckMoreNodes -->|Yes| Iterate
    CheckMoreNodes -->|No| End[✅ End Clustering<br/>Return clusters]
```

---

## Best Quality Selection Logic

```mermaid
flowchart TD
    Start[Videos in group/cluster] --> ExtractRes[📐 Extract Resolution<br/>for each video]
    
    ExtractRes --> CheckRes{Resolution<br/>found?}
    
    CheckRes -->|Yes| CalcScore["📊 Calculate score<br/>resolution_score = width x height<br/>Example: 1920x1080 = 2,073,600"]
    CheckRes -->|No| CheckItag{Has itag?}
    
    CheckItag -->|Yes| UseItag[Use itag as score<br/>Higher itag = better quality]
    CheckItag -->|No| ScoreZero[Score = 0<br/>No quality info]
    
    CalcScore --> Sort[🔢 Sort videos by:<br/>1. resolution_score DESC<br/>2. itag DESC<br/>3. job_id ASC]
    
    UseItag --> Sort
    ScoreZero --> Sort
    
    Sort --> Select[✅ Select first video<br/>Highest resolution<br/>or smallest job_id]
    
    Select --> Log[📝 Log selection:<br/>- Resolution info<br/>- Score<br/>- Reason]
    
    Log --> End[Return best video]
```

---

## Key Improvements Summary

### 1. Video ID Extraction (8 CDN mới)
- **FlashTalking**: `flashtalking_{account_id}_{base_filename}`
- **FPT Play**: `fptplay_{id}` hoặc `fptplay_vod_{hash}`
- **Adnxs**: `adnxs_{creative_id}_{uuid}`
- **VZ CDN**: `vzcdn_{uuid}`
- **UpPremium**: `upremium_{base_filename}`
- **BlueAdss**: `blueadss_{path}_{filename}`
- **Adsrvr**: `adsrvr_{filename}`
- **AIActiv**: `aiactiv_{base_filename}`

### 2. Resolution Extraction (4 patterns mới)
- **FlashTalking**: `_width_height_bitrate_fps.mp4`
- **Adnxs**: `uuid_width_height_bitratek.ext`
- **VZ CDN**: `play_1080p.mp4` → map to 1920x1080
- **Query params**: `?width=1920&height=1080`

### 3. Best Quality Selection
- **Dùng resolution_score** (width × height) thay vì chỉ itag
- **Sort order**: resolution_score DESC → itag DESC → job_id ASC
- **Đảm bảo** chọn video có resolution cao nhất

### 4. Cross-chunk Detection
- **Threshold riêng**: `cross_chunk_threshold` (default: 0.98)
- **Batch querying**: Query 5k videos/batch để tối ưu
- **Chỉ mark duplicate** nếu original ở chunk khác

### 5. Path Validation trong DFS
- **Max path length**: 2 (tránh transitive closure)
- **Path similarity**: >= threshold × 0.95
- **Prevent**: A-B-C-D where A and D are not similar

---

## Performance Optimizations

1. **Batch Querying**: Query 100 videos/batch để tránh message size limit
2. **Parallel Processing**: num_threads threads cho batch search
3. **Similarity Lookup Dict**: O(1) lookup thay vì O(n) search
4. **Memory Cleanup**: Clear all_data sau khi không cần
5. **Deterministic Sorting**: Sort by job_id number để đảm bảo kết quả nhất quán

---

## Output Files

1. **unique_csv**: Danh sách URLs unique (decoded_url)
2. **report_csv**: Duplicates report với mapping (duplicate_url, original_url, similarity)
3. **invalid_csv**: Invalid URLs report (nếu --auto_clean, gồm url, job_id, reason)

---

## Command Line Options

- `--skip_url_dedup`: Tắt pre-filtering (giữ tất cả videos)
- `--skip_cross_chunk`: Tắt cross-chunk duplicate removal
- `--cross_chunk_threshold`: Threshold cho cross-chunk (default: 0.98)
- `--auto_clean`: Tự động loại bỏ invalid URLs
- `--fast_mode`: Dùng search params tối ưu (nhanh hơn 2-4x)
- `--batch_size`: Batch size cho search (max 10)
- `--num_threads`: Số threads cho parallel search
