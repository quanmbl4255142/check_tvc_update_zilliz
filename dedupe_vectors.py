import argparse
import csv
import glob
import hashlib
import os
import shutil
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
    reps_vecs: List[np.ndarray] = []  # representatives (for method=cosine)
    reps_dirs: List[str] = []

    for job_dir in jobs:
        npy_path = os.path.join(job_dir, "first_frame.npy")
        if not os.path.isfile(npy_path):
            continue
        url = load_url(job_dir)
        job_id = os.path.basename(job_dir)

        if method == "hash":
            try:
                vhash, shape, dtype = vector_hash(npy_path, round_decimals=round_decimals)
            except Exception:
                continue
            if vhash not in seen:
                seen[vhash] = job_dir
                unique_rows.append([url])
            else:
                original_dir = seen[vhash]
                duplicates.append([url, job_id, os.path.basename(original_dir)])
                if delete:
                    try:
                        shutil.rmtree(job_dir)
                    except Exception:
                        pass
        else:
            # cosine-based dedupe with threshold
            try:
                v = load_normalized_vector(npy_path)
            except Exception:
                continue
            is_duplicate = False
            max_sim_val = None
            if reps_vecs:
                # Compute cosine similarity to all reps (dot since L2-normalized)
                reps = np.stack(reps_vecs, axis=0)  # (k, d)
                sims = reps @ v  # (k,)
                max_idx = int(np.argmax(sims))
                max_sim_val = float(sims[max_idx])
                if max_sim_val >= cosine_thresh:
                    # duplicate of reps_dirs[max_idx]
                    is_duplicate = True
                    original_dir = reps_dirs[max_idx]
                    duplicates.append([url, job_id, os.path.basename(original_dir), f"{max_sim_val:.6f}"])
                    if delete:
                        try:
                            shutil.rmtree(job_dir)
                        except Exception:
                            pass
            if not is_duplicate:
                reps_vecs.append(v)
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

    unique_count, dup_count = dedupe_vectors(
        root_dir=args.root,
        unique_csv=args.unique_csv,
        report_csv=args.report_csv,
        round_decimals=args.round_decimals,
        delete=args.delete,
        method=args.method,
        cosine_thresh=args.cosine_thresh,
    )

    print(f"Unique URLs: {unique_count}; Duplicates removed: {dup_count}")


if __name__ == "__main__":
    main()


