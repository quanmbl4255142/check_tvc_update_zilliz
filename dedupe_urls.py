import argparse
import csv
import os
import sys
from urllib.parse import urlparse


def normalize_url(u: str) -> str:
    s = (u or "").strip().strip('"').strip("'")
    # Basic normalization: collapse scheme/host case, drop trailing slash (path only), keep query intact
    try:
        p = urlparse(s)
        scheme = (p.scheme or "").lower()
        netloc = (p.netloc or "").lower()
        path = p.path or ""
        if len(path) > 1 and path.endswith("/"):
            path = path[:-1]
        # Keep params, query, fragment as-is to avoid over-merging distinct assets
        rebuilt = f"{scheme}://{netloc}{path}"
        if p.params:
            rebuilt += f";{p.params}"
        if p.query:
            rebuilt += f"?{p.query}"
        if p.fragment:
            rebuilt += f"#{p.fragment}"
        return rebuilt or s
    except Exception:
        return s


def dedupe_file(input_path: str, output_path: str, report_path: str) -> tuple[int, int]:
    total = 0
    kept = 0
    seen = {}
    duplicates = []  # list of duplicate url strings

    with open(input_path, "r", encoding="utf-8", newline="") as fin:
        reader = csv.reader(fin)
        rows = list(reader)

    # detect header if present
    start_idx = 0
    header = None
    if rows:
        first = (rows[0][0] if rows[0] else "").strip().lower()
        if first in {"decoded_url", "url", "tvc", "links"}:
            header = rows[0]
            start_idx = 1

    uniques: list[list[str]] = []
    index_map: dict[str, int] = {}
    for i in range(start_idx, len(rows)):
        total += 1
        cell = (rows[i][0] if rows[i] else "").strip()
        norm = normalize_url(cell)
        if norm not in seen:
            seen[norm] = 1
            index_map[norm] = i
            uniques.append([norm])
            kept += 1
        else:
            duplicates.append(norm)

    # write uniques
    with open(output_path, "w", encoding="utf-8", newline="") as fout:
        writer = csv.writer(fout)
        writer.writerow(["decoded_url"])
        writer.writerows(uniques)

    # write report: single column of duplicate URLs only
    with open(report_path, "w", encoding="utf-8", newline="") as frep:
        writer = csv.writer(frep)
        writer.writerow(["duplicate_url"])  # header as single column
        for url in duplicates:
            writer.writerow([url])

    return kept, len(duplicates)


def main() -> None:
    parser = argparse.ArgumentParser(description="Deduplicate URLs in a CSV and report removed duplicates")
    parser.add_argument("--input", dest="input_path", default="url-tvc.decoded.csv", help="Input CSV path (default: url-tvc.decoded.csv)")
    parser.add_argument("--output", dest="output_path", default="url-tvc.unique.csv", help="Output unique CSV path (default: url-tvc.unique.csv)")
    parser.add_argument("--report", dest="report_path", default="url-tvc.duplicates.csv", help="Duplicates report CSV path (default: url-tvc.duplicates.csv)")
    args = parser.parse_args()

    if not os.path.isfile(args.input_path):
        print(f"❌ ERROR: Input file not found: {args.input_path}", file=sys.stderr)
        print(f"Please run decode_urls.py first to generate the decoded URL file.", file=sys.stderr)
        sys.exit(1)

    # Check if output files would overwrite input
    if os.path.abspath(args.input_path) in [os.path.abspath(args.output_path), os.path.abspath(args.report_path)]:
        print(f"❌ ERROR: Output files cannot be the same as input file!", file=sys.stderr)
        sys.exit(1)

    kept, removed = dedupe_file(args.input_path, args.output_path, args.report_path)
    print(f"✅ Successfully processed: kept {kept} unique URL(s), removed {removed} duplicate(s).")
    print(f"→ Unique: {args.output_path}\n→ Report: {args.report_path}")


if __name__ == "__main__":
    main()


