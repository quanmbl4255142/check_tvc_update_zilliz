# FLOWCHART: Luồng Lọc Trùng - search_duplicates_aggregated.py

## Mermaid Flowchart

```mermaid
flowchart TD
    Start([Bắt đầu]) --> Connect[Kết nối Milvus<br/>Load Collection]
    
    Connect --> CheckChunk{Chunk Mode?}
    
    CheckChunk -->|Có| QueryChunk[Query theo job_id range<br/>url_XXXX format]
    CheckChunk -->|Không| QueryAll[Query tất cả videos]
    
    QueryChunk --> LoadData[Load embeddings + URLs<br/>all_data = job_id, url, embedding]
    QueryAll --> LoadData
    
    LoadData --> BuildVideoInfo[Xây dựng video_info map<br/>job_id → url, embedding]
    
    BuildVideoInfo --> CheckSkipURL{--skip_url_dedup?}
    
    CheckSkipURL -->|Có| SkipPreFilter[Bỏ qua Pre-filtering<br/>Giữ tất cả videos]
    CheckSkipURL -->|Không| PreFilter[PRE-FILTERING]
    
    PreFilter --> ExtractVideoID[Extract Video ID từ URL<br/>Google CDN: gcdn_id_XXX<br/>YouTube: youtube_XXX]
    
    ExtractVideoID --> GroupByID[Nhóm videos theo Video ID<br/>video_id_groups]
    
    GroupByID --> CheckChunkMode{Chunk Mode?}
    
    CheckChunkMode -->|Có| CrossChunkCheck[Kiểm tra Video ID<br/>trong chunks khác]
    CheckChunkMode -->|Không| SelectBest[Chọn video tốt nhất<br/>trong mỗi group]
    
    CrossChunkCheck --> RemoveCrossChunk[Loại bỏ groups<br/>đã tồn tại ở chunk khác]
    RemoveCrossChunk --> SelectBest
    
    SelectBest --> SelectByItag[Ưu tiên: itag cao nhất<br/>Nếu không: job_id nhỏ nhất]
    
    SelectByItag --> RemoveDupURL[Xóa URL duplicates<br/>Giữ 1 video/group]
    
    RemoveDupURL --> Pass1
    SkipPreFilter --> Pass1[PASS 1: Tìm Duplicate Pairs]
    
    Pass1 --> BatchVideos[Chia videos thành batches<br/>batch_size = 10 max]
    
    BatchVideos --> ParallelSearch[Parallel Search với threads<br/>Search trên Milvus]
    
    ParallelSearch --> CollectPairs[Thu thập duplicate pairs<br/>job_id1, job_id2, similarity]
    
    CollectPairs --> FilterThreshold[Lọc pairs:<br/>similarity >= threshold]
    
    FilterThreshold --> NormalizePairs[Normalize pairs<br/>Sort job_ids để tránh trùng]
    
    NormalizePairs --> SeparatePairs[Tách pairs:<br/>- within-chunk<br/>- cross-chunk]
    
    SeparatePairs --> Pass2[PASS 2: Clustering & Chọn Originals]
    
    Pass2 --> ProcessCrossChunk[Xử lý Cross-chunk Duplicates]
    
    ProcessCrossChunk --> CheckCrossThreshold{similarity >=<br/>cross_chunk_threshold?}
    
    CheckCrossThreshold -->|Có| MarkCrossDup[Đánh dấu duplicate<br/>nếu original ở chunk khác]
    CheckCrossThreshold -->|Không| SkipCrossDup[Bỏ qua pair này]
    
    MarkCrossDup --> BuildGraph
    SkipCrossDup --> BuildGraph[Xây dựng Graph<br/>job_id → neighbors]
    
    BuildGraph --> BuildSimilarityDict[Xây dựng similarity lookup<br/>job_id1, job_id2 → similarity]
    
    BuildSimilarityDict --> DFS[DFS với Path Validation<br/>Max path length = 3<br/>Path similarity >= threshold × 0.9]
    
    DFS --> FindClusters[Tìm Connected Components<br/>Mỗi cluster = 1 nhóm duplicates]
    
    FindClusters --> ProcessClusters[Xử lý từng Cluster]
    
    ProcessClusters --> SortByJobID[Sort job_ids theo số<br/>job_id nhỏ nhất = original]
    
    SortByJobID --> SelectOriginal[Chọn original<br/>Thêm vào unique_videos]
    
    SelectOriginal --> AddDuplicates[Thêm duplicates vào<br/>duplicates list]
    
    AddDuplicates --> CheckStandalone{Còn videos<br/>standalone?}
    
    CheckStandalone -->|Có| AddStandalone[Thêm standalone videos<br/>vào unique_videos]
    CheckStandalone -->|Không| AddCrossDup
    
    AddStandalone --> AddCrossDup[Thêm cross-chunk duplicates<br/>vào duplicates list]
    
    AddCrossDup --> CheckAutoClean{--auto_clean?}
    
    CheckAutoClean -->|Có| ValidateURL[Kiểm tra URL hợp lệ<br/>Loại bỏ PNG/images<br/>Loại bỏ URLs lỗi]
    CheckAutoClean -->|Không| WriteResults
    
    ValidateURL --> SeparateValid[Tách valid/invalid URLs]
    SeparateValid --> WriteResults[Ghi kết quả]
    
    WriteResults --> WriteUnique[Ghi unique_csv<br/>Danh sách URLs unique]
    WriteUnique --> WriteReport[Ghi report_csv<br/>Duplicates với original mapping]
    
    WriteReport --> CheckInvalid{Invalid URLs?}
    
    CheckInvalid -->|Có| WriteInvalid[Ghi invalid_csv<br/>Invalid URLs report]
    CheckInvalid -->|Không| End
    
    WriteInvalid --> End([Kết thúc])
    
    style Start fill:#90EE90
    style End fill:#FFB6C1
    style PreFilter fill:#FFE4B5
    style Pass1 fill:#ADD8E6
    style Pass2 fill:#DDA0DD
    style WriteResults fill:#F0E68C
```

## Flowchart Chi Tiết - Các Bước Quan Trọng

```mermaid
flowchart LR
    subgraph Phase1["PHASE 1: LOAD DATA"]
        A1[Connect Milvus] --> A2[Query Videos]
        A2 --> A3[Load Embeddings]
    end
    
    subgraph Phase2["PHASE 2: PRE-FILTER"]
        B1[Extract Video ID] --> B2[Group by ID]
        B2 --> B3[Check Cross-chunk]
        B3 --> B4[Select Best]
        B4 --> B5[Remove Dups]
    end
    
    subgraph Phase3["PHASE 3: PASS 1"]
        C1[Batch Videos] --> C2[Parallel Search]
        C2 --> C3[Collect Pairs]
        C3 --> C4[Filter Threshold]
        C4 --> C5[Separate Pairs]
    end
    
    subgraph Phase4["PHASE 4: PASS 2"]
        D1[Process Cross-chunk] --> D2[Build Graph]
        D2 --> D3[DFS Clustering]
        D3 --> D4[Select Originals]
        D4 --> D5[Add Standalone]
    end
    
    subgraph Phase5["PHASE 5: OUTPUT"]
        E1[Auto-clean] --> E2[Write CSV]
    end
    
    Phase1 --> Phase2
    Phase2 --> Phase3
    Phase3 --> Phase4
    Phase4 --> Phase5
```

## Decision Points Chi Tiết

```mermaid
flowchart TD
    subgraph Decisions["Các Điểm Quyết Định"]
        D1{Chunk Mode?}
        D2{--skip_url_dedup?}
        D3{Video ID exists<br/>in other chunk?}
        D4{similarity >=<br/>cross_chunk_threshold?}
        D5{Path similarity >=<br/>threshold × 0.9?}
        D6{--auto_clean?}
        D7{URL valid?}
    end
    
    D1 -->|Có| QueryByJobID[Query by job_id range]
    D1 -->|Không| QueryAll[Query all]
    
    D2 -->|Có| SkipPreFilter
    D2 -->|Không| DoPreFilter
    
    D3 -->|Có| RemoveGroup[Remove entire group]
    D3 -->|Không| KeepGroup[Keep group]
    
    D4 -->|Có| MarkAsDup[Mark as duplicate]
    D4 -->|Không| SkipPair[Skip pair]
    
    D5 -->|Có| AddToCluster[Add to cluster]
    D5 -->|Không| StopPath[Stop path]
    
    D6 -->|Có| ValidateURLs
    D6 -->|Không| WriteAll
    
    D7 -->|Có| KeepURL
    D7 -->|Không| RemoveURL
```

## Data Flow

```mermaid
flowchart LR
    subgraph Input["INPUT"]
        I1[(Milvus Collection)]
        I2[Parameters:<br/>threshold, batch_size,<br/>chunk_start, chunk_end]
    end
    
    subgraph Processing["PROCESSING"]
        P1[all_data:<br/>job_id, url, embedding]
        P2[video_info:<br/>job_id → url, embedding]
        P3[duplicate_pairs:<br/>job_id1, job_id2, sim]
        P4[clusters:<br/>Set of job_ids]
        P5[originals:<br/>Set of job_ids]
    end
    
    subgraph Output["OUTPUT"]
        O1[(unique_csv:<br/>Unique URLs)]
        O2[(report_csv:<br/>Duplicates report)]
        O3[(invalid_csv:<br/>Invalid URLs)]
    end
    
    I1 --> P1
    I2 --> P1
    P1 --> P2
    P2 --> P3
    P3 --> P4
    P4 --> P5
    P5 --> O1
    P5 --> O2
    P2 --> O3
```

## Critical Logic Flow

```mermaid
flowchart TD
    Start --> Load[Load Videos]
    Load --> PreFilter{Pre-filter?}
    
    PreFilter -->|Yes| ExtractID[Extract Video ID]
    ExtractID --> Group[Group by Video ID]
    Group --> CheckExist{Exists in<br/>other chunk?}
    CheckExist -->|Yes| Remove[Remove Group]
    CheckExist -->|No| SelectBest[Select Best Video<br/>Highest itag or<br/>Smallest job_id]
    SelectBest --> Search
    Remove --> Search
    PreFilter -->|No| Search[Search Duplicates]
    
    Search --> FindPairs[Find Duplicate Pairs]
    FindPairs --> Separate{Within-chunk<br/>or Cross-chunk?}
    
    Separate -->|Cross-chunk| CheckSim{Sim >=<br/>0.98?}
    CheckSim -->|Yes| MarkDup[Mark as Duplicate]
    CheckSim -->|No| Skip
    MarkDup --> Cluster
    
    Separate -->|Within-chunk| Cluster[Build Graph & Cluster]
    Cluster --> DFS[DFS with Validation]
    DFS --> SelectOrig[Select Original<br/>Smallest job_id]
    SelectOrig --> AddUnique[Add to Unique]
    
    AddUnique --> Clean{Auto-clean?}
    Clean -->|Yes| Validate[Validate URLs]
    Validate --> Write
    Clean -->|No| Write[Write Results]
    
    Write --> End
    
    style Start fill:#90EE90
    style End fill:#FFB6C1
    style PreFilter fill:#FFE4B5
    style Search fill:#ADD8E6
    style Cluster fill:#DDA0DD
    style Write fill:#F0E68C
```

