
# -*- coding: utf-8 -*-

import json
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingest_paysim import compute_sha256

# Đường dẫn manifest cần kiểm tra
MANIFEST_PATH = PROJECT_ROOT / "metadata" / "manifest.json"


def main():
    if not MANIFEST_PATH.is_file():
        print(f"[ERROR] Không tìm thấy manifest: {MANIFEST_PATH}")
        return 1

    try:
        with MANIFEST_PATH.open("r", encoding="utf-8") as f:
            manifest = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        print(f"[ERROR] Không thể đọc manifest: {e}")
        return 1

    if manifest.get("status") != "SUCCESS":
        print("[ERROR] Manifest không có trạng thái SUCCESS.")
        return 1

    destination = manifest.get("destination")
    if not destination:
        print("[ERROR] Manifest thiếu trường destination.")
        return 1

    raw_path = Path(destination).resolve()

    if not raw_path.is_file():
        print(f"[ERROR] Không tìm thấy file raw: {raw_path}")
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
    expected_hash = manifest.get("sha256") or manifest.get("hash")

    if not isinstance(expected_hash, str) or not expected_hash:
        print("[ERROR] Manifest thiếu mã SHA-256.")
        return 1

    print(f"Đang kiểm tra file: {raw_path}")
    actual_hash = compute_sha256(raw_path)

    if actual_hash.lower() != expected_hash.lower():
        print("[ERROR] SHA-256 không khớp!")
        print(f"Thực tế : {actual_hash}")
        print(f"Manifest: {expected_hash}")
        return 1

    print("=" * 50)
    print("XÁC THỰC MANIFEST: PASS")
    print(f"File     : {raw_path}")
    print(f"Dung lượng: {actual_bytes:,} bytes")
    print(f"SHA-256  : {actual_hash}")
    print("=" * 50)

    return 0


if __name__ == "__main__":
    sys.exit(main())
