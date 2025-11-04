import argparse
import csv
import os
import sys
from urllib.parse import unquote, urlparse


def smart_unquote(value: str) -> str:
    if value is None:
        return ""
    s = value.strip().strip('"').strip("'")
    # Some entries may be quoted full URLs already; decode percent-encodings.
    # Decode twice to handle cases like %253D (encoded '%3D').
    try:
        once = unquote(s)
        twice = unquote(once)
        decoded = twice
    except Exception:
        decoded = s

    # Fix protocol-relative URLs if any
    if decoded.startswith("//"):
        decoded = "https:" + decoded

    return decoded


def looks_like_url(s: str) -> bool:
    try:
        p = urlparse(s)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def decode_file(input_path: str, output_path: str) -> int:
    count = 0
    with open(input_path, "r", encoding="utf-8", newline="") as fin, open(
        output_path, "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        writer.writerow(["decoded_url"])  # single-column output

        first_row = True
        for row in reader:
            if not row:
                continue
            cell = row[0].strip()
            if first_row and cell.lower() in {"tvc", "url", "links"}:
                first_row = False
                continue
            first_row = False

            decoded = smart_unquote(cell)
            if looks_like_url(decoded):
                writer.writerow([decoded])
                count += 1
            else:
                # Keep original as fallback if decoding doesn't look like a URL
                writer.writerow([decoded or cell])
                count += 1
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Decode percent-encoded URLs in a CSV (single column)")
    parser.add_argument("--input", dest="input_path", default="url-tvc.csv", help="Input CSV path (default: url-tvc.csv)")
    parser.add_argument("--output", dest="output_path", default="url-tvc.decoded.csv", help="Output CSV path (default: url-tvc.decoded.csv)")
    args = parser.parse_args()

    if not os.path.isfile(args.input_path):
        print(f"Input file not found: {args.input_path}", file=sys.stderr)
        sys.exit(1)

    total = decode_file(args.input_path, args.output_path)
    print(f"Decoded {total} row(s) → {args.output_path}")


if __name__ == "__main__":
    main()


