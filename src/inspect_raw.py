#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script đọc và kiểm tra Raw Data PaySim bằng PySpark theo Checkpoint 2.
Có thể gọi trực tiếp với tham số: python src/inspect_raw.py --manifest "<path/to/manifest.json>"
"""

import argparse
import sys
from pathlib import Path

# Đảm bảo import được từ src
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT / "src") not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT / "src"))

from ingest_paysim import inspect_raw_with_spark


def parse_args():
    parser = argparse.ArgumentParser(
        description="Đọc và kiểm tra raw data PaySim bằng PySpark thông qua manifest hoặc đường dẫn file."
    )
    parser.add_argument(
        "--manifest",
        required=True,
        help="Đường dẫn tới file manifest.json hoặc đường dẫn tệp CSV raw.",
    )
    parser.add_argument(
        "--spark-master",
        default=None,
        help="Spark master URL (mặc định lấy từ biến môi trường SPARK_MASTER hoặc 'local[*]').",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    try:
        inspect_raw_with_spark(
            raw_path=args.manifest,
            master=args.spark_master,
        )
        return 0
    except Exception as e:
        print(f"\n[ERROR] Kiểm tra Spark thất bại: {e}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
