import argparse
import csv
import glob
import hashlib
import os
import shutil
import sys
from typing import Dict, List, Tuple

import numpy as np


def list_jobs(root_dir: str) -> List[str]:
    pattern = os.path.join(root_dir, "url_*")
    jobs = [p for p in glob.glob(pattern) if os.path.isdir(p)]
    jobs.sort()
    return jobs


def load_url(job_dir: str) -> str:
    url_path = os.path.join(job_dir, "url.txt")
    try:
        with open(url_path, "r", encoding="utf-8") as f:
            return f.read().strip()
    except Exception:
        return ""


def vector_hash(npy_path: str, round_decimals: int = 6) -> Tuple[str, Tuple[int, ...], str]:
    arr = np.load(npy_path)
    # Normalize dtype/precision for hashing stability
    if arr.dtype != np.float32:
        arr = arr.astype(np.float32)
    if round_decimals is not None and round_decimals >= 0:
        arr = np.round(arr, round_decimals)
    shape = tuple(arr.shape)
    h = hashlib.md5(arr.tobytes()).hexdigest()
    return h, shape, arr.dtype.name


def load_normalized_vector(npy_path: str) -> np.ndarray:
    v = np.load(npy_path).astype(np.float32)
    # L2 normalize defensively
    norm = float(np.linalg.norm(v)) or 1.0
    return v / norm


def load_all_frame_vectors(job_dir: str) -> List[np.ndarray]:
    """Load all frame embeddings from a job directory (first, middle, last if available)"""
    vectors = []
    frame_names = ["first_frame.npy", "middle_frame.npy", "last_frame.npy"]
    for name in frame_names:
        npy_path = os.path.join(job_dir, name)
        if os.path.isfile(npy_path):
            try:
                v = load_normalized_vector(npy_path)
                vectors.append(v)
            except Exception:
                pass
    return vectors


def compute_max_similarity(vectors_a: List[np.ndarray], vectors_b: List[np.ndarray]) -> float:
    """
    Compute max cosine similarity between all pairs of frames from two videos.
    If ANY pair of frames is similar, videos are considered similar.
    This helps detect videos with watermarks/crops where some frames differ.
    """
    if not vectors_a or not vectors_b:
        return 0.0
    
    max_sim = 0.0
    for v_a in vectors_a:
        for v_b in vectors_b:
            sim = float(np.dot(v_a, v_b))  # Cosine similarity (already L2-normalized)
            max_sim = max(max_sim, sim)
    return max_sim


def dedupe_vectors(
    root_dir: str,
    unique_csv: str,
    report_csv: str,
    round_decimals: int,
    delete: bool,
    method: str = "cosine",
    cosine_thresh: float = 0.95,
) -> Tuple[int, int]:
    jobs = list_jobs(root_dir)
    seen: Dict[str, str] = {}  # hash -> job_dir (for method=hash)
    unique_rows: List[List[str]] = []
    duplicates: List[List[str]] = []
    reps_vecs_multi: List[List[np.ndarray]] = []  # representatives - multiple frames per video (for method=cosine)
    reps_dirs: List[str] = []

    for job_dir in jobs:
        url = load_url(job_dir)
        job_id = os.path.basename(job_dir)
        
        # Load all available frame vectors
        frame_vectors = load_all_frame_vectors(job_dir)
        if not frame_vectors:
            continue  # Skip if no valid embeddings found

        if method == "hash":
            # For hash method, use only first frame (backward compatibility)
            npy_path = os.path.join(job_dir, "first_frame.npy")
            if not os.path.isfile(npy_path):
                continue
            try:
                vhash, shape, dtype = vector_hash(npy_path, round_decimals=round_decimals)
            except Exception:
                continue
            if vhash not in seen:
                seen[vhash] = job_dir
                unique_rows.append([url])
            else:
                original_dir = seen[vhash]
                duplicates.append([url, job_id, os.path.basename(original_dir), "exact_hash"])
                if delete:
                    try:
                        shutil.rmtree(job_dir)
                    except Exception:
                        pass
        else:
            # cosine-based dedupe with multi-frame comparison
            is_duplicate = False
            max_sim_val = 0.0
            best_match_idx = -1
            
            if reps_vecs_multi:
                # Compare with all representatives
                for rep_idx, rep_vectors in enumerate(reps_vecs_multi):
                    sim = compute_max_similarity(frame_vectors, rep_vectors)
                    if sim > max_sim_val:
                        max_sim_val = sim
                        best_match_idx = rep_idx
                
                if max_sim_val >= cosine_thresh:
                    # duplicate of reps_dirs[best_match_idx]
                    is_duplicate = True
                    original_dir = reps_dirs[best_match_idx]
                    duplicates.append([url, job_id, os.path.basename(original_dir), f"{max_sim_val:.6f}"])
                    if delete:
                        try:
                            shutil.rmtree(job_dir)
                        except Exception:
                            pass
            
            if not is_duplicate:
                reps_vecs_multi.append(frame_vectors)
                reps_dirs.append(job_dir)
                unique_rows.append([url])

    # Write unique URLs
    os.makedirs(os.path.dirname(unique_csv) or ".", exist_ok=True)
    with open(unique_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["decoded_url"])  # single column
        writer.writerows(unique_rows)

    # Write duplicate report
    os.makedirs(os.path.dirname(report_csv) or ".", exist_ok=True)
    with open(report_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["duplicate_url", "job_id", "duplicate_of_job_id", "max_similarity"])  # add cosine for transparency
        writer.writerows(duplicates)

    return len(unique_rows), len(duplicates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Detect and remove duplicate embedding vectors; export unique URL list")
    parser.add_argument("--root", default="batch_outputs", help="Root directory containing url_XXXX folders (default: batch_outputs)")
    parser.add_argument("--unique_csv", default="unique_urls.csv", help="Output CSV of unique URLs (default: unique_urls.csv)")
    parser.add_argument("--report_csv", default="duplicate_vectors.csv", help="Report CSV of removed duplicates (default: duplicate_vectors.csv)")
    parser.add_argument("--round", dest="round_decimals", type=int, default=6, help="Round decimals before hashing to merge near-identical vectors (default: 6)")
    parser.add_argument("--delete", action="store_true", help="Actually delete duplicate folders")
    parser.add_argument("--method", choices=["hash", "cosine"], default="cosine", help="Dedupe method: exact hash or cosine threshold (default: cosine)")
    parser.add_argument("--cosine_thresh", type=float, default=0.995, help="Cosine similarity threshold for duplicates (default: 0.995)")
    args = parser.parse_args()

    # Validate root directory exists
    if not os.path.isdir(args.root):
        print(f"❌ ERROR: Root directory not found: {args.root}", file=sys.stderr)
        print(f"Please run batch_extract_from_urls.py first to generate embeddings.", file=sys.stderr)
        sys.exit(1)

    # Check if there are any job folders
    jobs = list_jobs(args.root)
    if not jobs:
        print(f"⚠️  WARNING: No url_* folders found in {args.root}", file=sys.stderr)
        print(f"Please make sure batch_extract_from_urls.py has completed successfully.", file=sys.stderr)
        sys.exit(1)

    # Validate cosine threshold
    if args.method == "cosine" and not (0.0 <= args.cosine_thresh <= 1.0):
        print(f"❌ ERROR: Cosine threshold must be between 0.0 and 1.0 (got {args.cosine_thresh})", file=sys.stderr)
        sys.exit(1)

    # Display processing mode
    if args.method == "cosine":
        print(f"🎯 Detection mode: Multi-frame comparison (cosine similarity >= {args.cosine_thresh})")
        print(f"   This will detect duplicates even with watermarks/crops by comparing all frame pairs.")
    else:
        print(f"🎯 Detection mode: Exact hash matching")
    
    unique_count, dup_count = dedupe_vectors(
        root_dir=args.root,
        unique_csv=args.unique_csv,
        report_csv=args.report_csv,
        round_decimals=args.round_decimals,
        delete=args.delete,
        method=args.method,
        cosine_thresh=args.cosine_thresh,
    )

    print(f"\n✅ Results: {unique_count} unique video(s), {dup_count} duplicate(s) {'removed' if args.delete else 'detected'}")
    print(f"→ Unique URLs: {args.unique_csv}")
    print(f"→ Duplicates report: {args.report_csv}")


if __name__ == "__main__":
    main()


