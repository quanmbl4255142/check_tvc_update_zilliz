@echo off
python run_all_chunks_append.py --collection video_dedup_tri_quan --cosine_thresh 0.85 --chunk_size 5000 --unique_csv FINAL_RESULT.csv --report_csv duplicates.csv --auto_clean --batch_size 10 --num_threads 4 --top_k 50 --fast_mode
pause


