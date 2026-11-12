
# -*- coding: utf-8 -*-

import argparse
import json
import sys
from pathlib import Path, PurePosixPath, PureWindowsPath

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingest_paysim import compute_sha256

DEFAULT_MANIFEST_PATH = "metadata/manifest.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description="Kiểm tra và xác thực manifest dữ liệu raw."
    )
    parser.add_argument(
        "--manifest",
        default=DEFAULT_MANIFEST_PATH,
        help="Đường dẫn tới file manifest.json cần kiểm tra (mặc định: metadata/manifest.json).",
    )
    return parser.parse_args()


def resolve_repo_relative(value, field):
    """Manifest phải lưu đường dẫn tương đối so với gốc repo (dạng data/raw/...)."""
    if not isinstance(value, str) or not value:
        raise ValueError(f"Manifest thiếu trường {field}.")
    if PureWindowsPath(value).is_absolute() or PurePosixPath(value).is_absolute():
        raise ValueError(f"Trường {field} là đường dẫn tuyệt đối ({value}); manifest phải dùng đường dẫn tương đối.")
    return PROJECT_ROOT / value


def main():
    args = parse_args()
    manifest_path = Path(args.manifest)
    if not manifest_path.is_absolute():
        manifest_path = PROJECT_ROOT / manifest_path

    if not manifest_path.is_file():
        print(f"[ERROR] Không tìm thấy manifest: {args.manifest}")
        return 1

    try:
        with manifest_path.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[ERROR] Không thể đọc manifest: {e}")
        return 1

    if manifest.get("status") != "SUCCESS":
        print("[ERROR] Manifest không có trạng thái SUCCESS.")
        return 1

    try:
        raw_path = resolve_repo_relative(manifest.get("destination"), "destination")
        log_path = resolve_repo_relative(manifest.get("log_file"), "log_file")
    except ValueError as e:
        print(f"[ERROR] {e}")
        return 1

    if not raw_path.is_file():
        print(f"[ERROR] Không tìm thấy file raw: {manifest['destination']}")
        return 1

    if not log_path.is_file():
        print(f"[ERROR] Không tìm thấy file log của run: {manifest['log_file']}")
        return 1

    # Kiểm tra kích thước file
    expected_bytes = manifest.get("bytes")
    actual_bytes = raw_path.stat().st_size

    if (
        not isinstance(expected_bytes, int)
        or isinstance(expected_bytes, bool)
        or actual_bytes <= 0
        or actual_bytes != expected_bytes
    ):
        print(
            f"[ERROR] Dung lượng không khớp: "
            f"thực tế={actual_bytes}, manifest={expected_bytes}"
        )
        return 1

    # Kiểm tra SHA-256
    expected_hash = manifest.get("sha256")

    if not isinstance(expected_hash, str) or not expected_hash:
        print("[ERROR] Manifest thiếu mã SHA-256.")
        return 1

    print(f"Đang kiểm tra file: {manifest['destination']}")
    actual_hash = compute_sha256(raw_path)

    if actual_hash.lower() != expected_hash.lower():
        print("[ERROR] SHA-256 không khớp!")
        print(f"Thực tế : {actual_hash}")
        print(f"Manifest: {expected_hash}")
        return 1

    print("=" * 50)
    print(f"Run ID    : {manifest.get('run_id')}")
    print(f"File      : {manifest['destination']}")
    print(f"Dung lượng: {actual_bytes:,} bytes [KHỚP]")
    print(f"SHA-256   : {actual_hash} [KHỚP]")
    print(f"Log file  : {manifest['log_file']} [TỒN TẠI]")
    print("XÁC THỰC MANIFEST: PASS")
    print("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
