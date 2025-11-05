import argparse
import glob
import os
import shutil
from typing import List


def list_job_dirs(root: str) -> List[str]:
    pattern = os.path.join(root, "url_*")
    return sorted([p for p in glob.glob(pattern) if os.path.isdir(p)])


def is_empty_or_url_only(job_dir: str) -> bool:
    try:
        entries = [name for name in os.listdir(job_dir) if not name.startswith(".")]
    except Exception:
        return False
    if not entries:
        return True  # empty
    # If contains any .npy, keep it
    for name in entries:
        if name.lower().endswith(".npy"):
            return False
    # If only url.txt (case-insensitive) or other trivial files, and no npy → delete
    normalized = {name.lower() for name in entries}
    return normalized == {"url.txt"} or normalized == {"url.txt", "readme.txt"}


def clean(root: str, dry_run: bool) -> int:
    removed = 0
    for job_dir in list_job_dirs(root):
        if is_empty_or_url_only(job_dir):
            if dry_run:
                print(f"DRY-RUN would remove: {job_dir}")
            else:
                try:
                    shutil.rmtree(job_dir)
                    print(f"Removed: {job_dir}")
                    removed += 1
                except Exception as e:
                    print(f"Failed to remove {job_dir}: {e}")
    return removed


def main() -> None:
    parser = argparse.ArgumentParser(description="Remove job folders that are empty or only contain url.txt (no npy)")
    parser.add_argument("--root", default="batch_outputs", help="Root directory (default: batch_outputs)")
    parser.add_argument("--dry_run", action="store_true", help="List folders that would be removed without deleting")
    args = parser.parse_args()

    # Validate root directory exists
    if not os.path.isdir(args.root):
        print(f"❌ ERROR: Root directory not found: {args.root}", file=sys.stderr)
        print(f"Please run batch_extract_from_urls.py first to generate job folders.", file=sys.stderr)
        sys.exit(1)

    removed = clean(args.root, args.dry_run)
    if args.dry_run:
        print(f"\n🔍 DRY RUN: Would remove {removed} folder(s)")
        print(f"Run without --dry_run to actually delete them.")
    else:
        if removed > 0:
            print(f"✅ Total removed: {removed} folder(s)")
        else:
            print(f"✅ No empty folders found. All jobs look good!")


if __name__ == "__main__":
    main()


