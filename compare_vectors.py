import argparse
import os
import sys
import numpy as np


def load_normalized(npy_path: str) -> np.ndarray:
    v = np.load(npy_path).astype(np.float32)
    norm = float(np.linalg.norm(v)) or 1.0
    return v / norm


def resolve_job_path(root: str, job_id: str) -> str:
    # Accept either a full path to .npy or a job folder name like url_0000
    if job_id.lower().endswith(".npy") and os.path.isfile(job_id):
        return job_id
    if os.path.isdir(job_id):
        cand = os.path.join(job_id, "first_frame.npy")
        if os.path.isfile(cand):
            return cand
    cand = os.path.join(root, job_id, "first_frame.npy")
    if os.path.isfile(cand):
        return cand
    raise FileNotFoundError(f"Cannot resolve vector for '{job_id}'. Provide job folder or .npy path.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare cosine similarity between two vectors (as percentage)")
    parser.add_argument("job_a", help="First job id (e.g., url_0000) or path to .npy")
    parser.add_argument("job_b", help="Second job id (e.g., url_0003) or path to .npy")
    parser.add_argument("--root", default="batch_outputs", help="Root folder for jobs (default: batch_outputs)")
    args = parser.parse_args()

    try:
        path_a = resolve_job_path(args.root, args.job_a)
        path_b = resolve_job_path(args.root, args.job_b)
    except Exception as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)

    va = load_normalized(path_a)
    vb = load_normalized(path_b)
    sim = float(np.dot(va, vb))
    sim = max(min(sim, 1.0), -1.0)
    pct = sim * 100.0
    print(f"Cosine similarity: {sim:.6f} ({pct:.2f}%)")
    print(f"A: {path_a}")
    print(f"B: {path_b}")


if __name__ == "__main__":
    main()


