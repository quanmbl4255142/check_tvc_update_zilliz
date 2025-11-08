import argparse
import csv
import os
import sys

# thư viện urllib.parse dùng để phân tích các url thành các thành phần như scheme, netloc, path, query, fragment.
from urllib.parse import unquote, urlparse

# smart_unquote dùng để giải mã url bằng cách decode percent-encoding.
def smart_unquote(value: str) -> str:
    # nếu value là None, trả về chuỗi rỗng.
    if value is None:
        return ""
    # s nhằm loại bỏ các khoảng trắng, dấu ngoặc kép và dấu ngoặc đơn.
    s = value.strip().strip('"').strip("'")
    
    try:
        # unquote này dùng để giải mã url bằng cách decode percent-encoding.
        once = unquote(s)
        twice = unquote(once)
        decoded = twice
    except Exception:
        decoded = s

    # if này dùng để fix protocol-relative URLs nếu có.
    # protocol-relative là url không có scheme.
    # ví dụ: //www.google.com thì sẽ được fix thành https://www.google.com.
    # startswith là phương thức kiểm tra xem một chuỗi có bắt đầu với một chuỗi cụ thể không.
    if decoded.startswith("//"):
        decoded = "https:" + decoded

    return decoded

# looks_like_url phương thức này dùng để kiểm tra xem một chuỗi có phải là một url hợp lệ không.
def looks_like_url(s: str) -> bool:
    try:
        # urlparse phân tích chuỗi url thành các thành phần như scheme, netloc, path, query, fragment.
        p = urlparse(s)
        return p.scheme in ("http", "https") and bool(p.netloc)
    except Exception:
        return False


def decode_file(input_path: str, output_path: str) -> int:
    # biến count dùng để đếm số lượng url đã được giải mã.
    count = 0
    
    # with open dùng để đọc và đặt là biến fin và viết đặt là biến fout.
    with open(input_path, "r", encoding="utf-8", newline="") as fin, open(
        output_path, "w", encoding="utf-8", newline=""
    ) as fout:
        reader = csv.reader(fin)
        writer = csv.writer(fout)
        writer.writerow(["decoded_url"])  # single-column output

        # first_row là biến kiểm tra xem có phải là dòng đầu tiên không.
        first_row = True
        for row in reader:
            if not row:
                continue
            # cell là biến lấy giá trị của cột đầu tiên. nghĩa dữ liệu đầu tiên của file csv.
            cell = row[0].strip()
            # if này dùng để kiểm tra xem có phải là dòng đầu tiên không.
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
    # phương thức ArgumentParser dùng để xử lý các tham số dòng lệnh nghĩa là nhập vào các tham số từ dòng lệnh.
    parser = argparse.ArgumentParser(description="Decode percent-encoded URLs in a CSV (single column)")
    # phương thức add_argument dùng để thêm các tham số vào parser.
    parser.add_argument("--input", dest="input_path", default="url-tvc.csv", help="Input CSV path (default: url-tvc.csv)")
    parser.add_argument("--output", dest="output_path", default="url-tvc.decoded.csv", help="Output CSV path (default: url-tvc.decoded.csv)")
    # phương thức parse_args dùng để phân tích các tham số dòng lệnh và trả về một đối tượng Namespace chứa các tham số.
    args = parser.parse_args()

    # phương thức isfile dùng để kiểm tra xem một file có tồn tại không.
    if not os.path.isfile(args.input_path):
        print(f"❌ ERROR: Input file not found: {args.input_path}", file=sys.stderr)
        print(f"Please provide a valid CSV file containing URLs.", file=sys.stderr)
        sys.exit(1)

    # Check if output file would overwrite input
    # abspath dùng để lấy đường dẫn tuyệt đối của file.
    if os.path.abspath(args.input_path) == os.path.abspath(args.output_path):
        print(f"❌ ERROR: Output file cannot be the same as input file!", file=sys.stderr)
        sys.exit(1)

    total = decode_file(args.input_path, args.output_path)
    print(f"✅ Successfully decoded {total} row(s) → {args.output_path}")


if __name__ == "__main__":
    main()


