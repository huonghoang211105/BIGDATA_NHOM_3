#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Ingestion Nạp Dữ Liệu PaySim vào Raw Storage
Tuân thủ đầy đủ các yêu cầu Checkpoint 2 (Buổi 6):
1. Nạp tự động từ Local Path hoặc URL HTTP(S).
2. Đảm bảo tính bất biến (Immutability): Copy binary nguyên vẹn, không chỉnh sửa hay ETL.
3. Cơ chế Rerun (Idempotency): Mỗi lần chạy tạo một run_id và thư mục riêng biệt theo timestamp.
4. Bắt lỗi đầy đủ: Kiểm tra file tồn tại, file rỗng, timeout mạng, header PaySim.
5. Tạo Manifest và Log truy vết: Ghi nhận source, ingest_time, bytes, sha256 hash, destination, status.
"""

import argparse
import csv
import hashlib
import json
import logging
import os
import shutil
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

# Cấu hình UTF-8 cho console Windows
if hasattr(sys.stdout, "reconfigure"):
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass
if hasattr(sys.stderr, "reconfigure"):
    try:
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent

# Danh sách 11 cột chuẩn của bộ dữ liệu PaySim
PAYSIM_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud"
]


def setup_logger(log_path):
    """Cấu hình logger ghi đồng thời ra console và file log."""
    log_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger(f"ingest_{log_path.stem}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    file_handler = logging.FileHandler(log_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    return logger


def extract_filename(source):
    """Xác định tên tệp từ đường dẫn local hoặc URL."""
    parsed = urlparse(source)
    if parsed.scheme in ("http", "https"):
        name = Path(unquote(parsed.path)).name
    else:
        name = Path(source).name

    if not name or name in (".", ".."):
        name = "paysim_dataset.csv"
    return name


def compute_sha256(filepath, chunk_size=1024 * 1024):
    """Tính mã băm SHA-256 theo từng khối để tiết kiệm bộ nhớ."""
    sha = hashlib.sha256()
    with open(filepath, "rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            sha.update(chunk)
    return sha.hexdigest()


def validate_csv_source(source_path, expected_columns=None):
    """Kiểm tra tính hợp lệ của file CSV nguồn: không rỗng, đúng header PaySim."""
    if not source_path.is_file():
        raise FileNotFoundError(f"File nguồn không tồn tại: {source_path}")

    file_size = source_path.stat().st_size
    if file_size == 0:
        raise ValueError(f"File nguồn rỗng (0 bytes): {source_path}")

    with source_path.open(encoding="utf-8-sig", newline="", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError(f"File CSV rỗng, không có dòng tiêu đề: {source_path}")

        cleaned_header = [col.strip() for col in header]
        if expected_columns and cleaned_header != expected_columns:
            raise ValueError(
                f"Header CSV không khớp cấu trúc PaySim 11 cột.\n"
                f"Thực tế: {cleaned_header}\n"
                f"Kỳ vọng: {expected_columns}"
            )

        # Kiểm tra tối thiểu 1 dòng dữ liệu
        first_row = next(reader, None)
        if not first_row:
            raise ValueError(f"File CSV chỉ có header, không có dòng dữ liệu nào: {source_path}")


def copy_source(source, temp_destination, logger):
    """Tải từ URL hoặc sao chép từ Local Path vào đường dẫn tạm (.part)."""
    parsed = urlparse(source)

    if parsed.scheme in ("http", "https"):
        logger.info(f"Đang tải dữ liệu từ URL: {source}")
        req = Request(source, headers={"User-Agent": "BigData-PaySim-Ingestion/1.0"})
        try:
            with urlopen(req, timeout=60) as response, temp_destination.open("wb") as out_file:
                shutil.copyfileobj(response, out_file, length=1024 * 1024)
        except (HTTPError, URLError, TimeoutError) as e:
            raise ConnectionError(f"Mất kết nối hoặc lỗi khi tải từ URL '{source}': {e}")
    else:
        source_path = Path(source).expanduser().resolve()
        if not source_path.exists():
            raise FileNotFoundError(f"Đường dẫn nguồn local không tồn tại: {source_path}")
        if not source_path.is_file():
            raise ValueError(f"Nguồn dữ liệu không phải là một tệp hợp lệ: {source_path}")
        if source_path.stat().st_size == 0:
            raise ValueError(f"Tệp nguồn rỗng (0 bytes): {source_path}")

        logger.info(f"Đang sao chép binary từ file local: {source_path}")
        with source_path.open("rb") as in_file, temp_destination.open("wb") as out_file:
            shutil.copyfileobj(in_file, out_file, length=1024 * 1024)


def write_manifest(manifest_path, manifest_data):
    """Ghi dữ liệu manifest an toàn thông qua file tạm."""
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = manifest_path.with_name(manifest_path.name + ".tmp")
    try:
        with temp_path.open("w", encoding="utf-8") as f:
            json.dump(manifest_data, f, ensure_ascii=False, indent=2)
            f.write("\n")
        temp_path.replace(manifest_path)
    finally:
        if temp_path.exists():
            temp_path.unlink()


def find_default_source():
    """Tự động tìm kiếm file PaySim có sẵn trong repo hoặc Kaggle."""
    candidates = [
        PROJECT_ROOT / "data" / "raw" / "paysim_data.csv",
        PROJECT_ROOT / "data" / "paysim_data.csv",
    ]
    for c in candidates:
        if c.is_file() and c.stat().st_size > 0:
            return str(c)

    # Kiểm tra mount của Kaggle
    kaggle_input = Path("/kaggle/input")
    if kaggle_input.exists():
        for csv_file in kaggle_input.rglob("*.csv"):
            if "paysim" in csv_file.name.lower() or "ps_" in csv_file.name.lower():
                return str(csv_file)

    # Tìm file csv bất kỳ nằm trực tiếp dưới data/raw/
    raw_dir = PROJECT_ROOT / "data" / "raw"
    if raw_dir.exists():
        for f in raw_dir.glob("*.csv"):
            if f.is_file() and f.stat().st_size > 0:
                return str(f)

    return None


def ingest(source, dataset="paysim", raw_root=None, logger=None):
    """
    Hàm thực thi Ingestion:
    - Tạo run folder theo ngày và timestamp.
    - Copy binary nguyên bản không chỉnh sửa.
    - Tính SHA256 và kích thước bytes.
    - Xuất file manifest.json tại cả run folder và thư mục metadata/.
    """
    copy_source(source, temp_destination, logger)

    file_size = temp_destination.stat().st_size
    if file_size == 0:
        raise ValueError("File tải về rỗng (0 bytes).")

    # Validate nội dung cho MỌI loại nguồn (URL/local) trước khi công bố
    validate_csv_source(temp_destination, PAYSIM_COLUMNS)

    temp_destination.replace(destination_path)
    if raw_root is None:
        raw_root = PROJECT_ROOT / "data" / "raw"
    else:
        raw_root = Path(raw_root).resolve()

    now = datetime.now(timezone.utc)
    timestamp = now.strftime("%Y%m%dT%H%M%SZ")
    run_id = f"{timestamp}_{uuid.uuid4().hex[:8]}"
    date_str = now.strftime("%Y-%m-%d")

    # Đường dẫn thư mục đích theo quy ước phân cấp chuẩn
    destination_dir = raw_root / dataset / f"ingest_date={date_str}" / f"run_{run_id}"
    filename = extract_filename(source)
    destination_path = destination_dir / filename
    temp_destination = destination_dir / (filename + ".part")

    manifest = {
        "source": str(source),
        "dataset": dataset,
        "run_id": run_id,
        "ingest_time": now.isoformat(),
        "bytes": 0,
        "hash": None,
        "sha256": None,
        "destination": str(destination_path.resolve()),
        "status": "FAILED",
        "rerun_policy": "Tạo run_id mới theo timestamp mỗi lần chạy; cùng snapshot cho ra cùng mã hash.",
    }

    manifest_central = PROJECT_ROOT / "metadata" / "manifest.json"
    manifest_run = destination_dir / "manifest.json"

    logger.info("=" * 60)
    logger.info("BẮT ĐẦU JOB INGESTION DỮ LIỆU PAYSIM")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Nguồn: {source}")
    logger.info(f"Đích lưu trữ: {destination_path}")
    logger.info("=" * 60)

    try:
        # Nếu nguồn là file local, kiểm tra tính hợp lệ trước khi copy
        if not urlparse(source).scheme in ("http", "https"):
            validate_csv_source(Path(source).expanduser().resolve(), PAYSIM_COLUMNS)

        # Tạo thư mục đích
        destination_dir.mkdir(parents=True, exist_ok=False)

        # Thực hiện copy vào file tạm .part
        copy_source(source, temp_destination, logger)

        # Kiểm tra kích thước file tải về
        file_size = temp_destination.stat().st_size
        if file_size == 0:
            raise ValueError("File tải về rỗng (0 bytes).")

        # Đổi tên file tạm thành file chính thức (atomic move)
        temp_destination.replace(destination_path)
        logger.info(f"Đã lưu file raw thành công ({file_size:,} bytes).")

        # Tính mã băm SHA-256 trên file đã lưu
        logger.info("Đang tính mã băm SHA-256 để kiểm tra tính toàn vẹn...")
        file_hash = compute_sha256(destination_path)
        logger.info(f"Mã SHA-256: {file_hash}")

        # Cập nhật thông tin manifest
        manifest["bytes"] = file_size
        manifest["hash"] = file_hash
        manifest["sha256"] = file_hash
        manifest["status"] = "SUCCESS"

        # Ghi manifest tại thư mục run và metadata/
        write_manifest(manifest_run, manifest)
        write_manifest(manifest_central, manifest)

        logger.info(f"Đã lưu Manifest tại: {manifest_run}")
        logger.info(f"Đã cập nhật Manifest tổng: {manifest_central}")
        logger.info("=" * 60)
        logger.info(f"INGESTION SUCCESS: {destination_path}")
        logger.info("=" * 60)
        return manifest

    except Exception as e:
        logger.exception(f"LỖI INGESTION: {e}")

        # Dọn file tạm và thư mục run rỗng
        if temp_destination.exists():
            temp_destination.unlink(missing_ok=True)
        try:
            if destination_dir.exists() and not any(destination_dir.iterdir()):
                destination_dir.rmdir()
        except OSError:
            pass

        # Ghi lỗi ra file riêng, KHÔNG đè manifest của lần chạy thành công
        manifest["error"] = str(e)
        manifest["status"] = "FAILED"
        try:
            write_manifest(PROJECT_ROOT / "metadata" / "last_failed_run.json", manifest)
        except Exception:
            pass
        raise


def parse_args():
    parser = argparse.ArgumentParser(
        description="Script nạp dữ liệu PaySim vào Raw Storage tuân thủ Checkpoint 2."
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Đường dẫn file local hoặc URL HTTP(S). Nếu bỏ trống, script tự tìm file có sẵn.",
    )
    parser.add_argument(
        "--dataset",
        default="paysim",
        help="Tên định danh dataset (mặc định: paysim).",
    )
    parser.add_argument(
        "--raw-root",
        default=str(PROJECT_ROOT / "data" / "raw"),
        help="Thư mục gốc lưu trữ raw data (mặc định: data/raw).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_uuid = uuid.uuid4().hex[:8]
    log_file = PROJECT_ROOT / "logs" / f"ingest_{now_str}_{run_uuid}.txt"

    logger = setup_logger(log_file)

    source = args.source
    if not source:
        source = find_default_source()
        if not source:
            logger.error("Không tìm thấy nguồn dữ liệu mặc định.")
            logger.error("Vui lòng chỉ định tham số: --source <đường_dẫn_file_hoặc_URL>")
            return 1
        logger.info(f"Tự động chọn nguồn dữ liệu tìm thấy: {source}")

    try:
        manifest = ingest(
            source=source,
            dataset=args.dataset,
            raw_root=args.raw_root,
            logger=logger,
        )
        print(f"\nSUCCESS manifest={manifest['destination']}")
        return 0
    except Exception:
        print("\nINGESTION FAILED. Xem chi tiết trong file log.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
