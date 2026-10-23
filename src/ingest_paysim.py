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
6. Kiểm tra Raw Data bằng PySpark: Đọc trực tiếp file raw đã nạp, kiểm tra schema, đếm số dòng (count), phân vùng (partitions), trích xuất sample và lưu báo cáo spark_read_<run_id>.json.
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


def get_spark_session(app_name="paysim-raw-inspect", master=None):
    """Khởi tạo hoặc lấy SparkSession hiện tại."""
    try:
        from pyspark.sql import SparkSession
    except ImportError as e:
        raise ImportError(
            "Chưa cài đặt PySpark. Vui lòng cài đặt bằng lệnh: pip install pyspark>=3.5.0"
        ) from e

    master_mode = master or os.environ.get("SPARK_MASTER", "local[*]")
    spark = (
        SparkSession.builder
        .master(master_mode)
        .appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def inspect_raw_with_spark(
    raw_path,
    run_id=None,
    logger=None,
    master=None,
    report_dir=None,
):
    """
    Sử dụng PySpark để đọc, kiểm tra cấu trúc (schema), đếm số dòng (count),
    kiểm tra tính toàn vẹn (mapPartitions) và hiển thị dữ liệu mẫu từ tệp raw data CSV.
    """
    raw_path = Path(raw_path).resolve()

    # Nếu truyền vào file manifest.json, tự động trích xuất destination path và run_id
    if raw_path.suffix.lower() == ".json":
        with raw_path.open("r", encoding="utf-8") as f:
            manifest_info = json.load(f)
        if "destination" in manifest_info:
            if not run_id:
                run_id = manifest_info.get("run_id")
            raw_path = Path(manifest_info["destination"]).resolve()

    if not raw_path.is_file():
        raise FileNotFoundError(f"File raw không tồn tại để kiểm tra Spark: {raw_path}")

    log_fn = logger.info if logger else print

    log_fn("=" * 60)
    log_fn("KIỂM TRA RAW DATA BẰNG PYSPARK (SPARK RAW INSPECTION)")
    log_fn(f"Đường dẫn file raw: {raw_path}")
    log_fn("=" * 60)

    spark = get_spark_session(
        app_name=f"paysim-raw-inspect-{run_id or 'manual'}",
        master=master,
    )

    try:
        log_fn(f"Spark Version   : {spark.version} (Master: {spark.sparkContext.master})")
        log_fn("Đang đọc raw CSV với chế độ FAILFAST...")

        df = spark.read.option("header", True).option("mode", "FAILFAST").csv(str(raw_path))

        # Kiểm tra cấu trúc 11 cột PaySim
        if df.columns != PAYSIM_COLUMNS:
            raise ValueError(
                f"Cột trong DataFrame không khớp chuẩn PaySim 11 cột.\n"
                f"Thực tế ({len(df.columns)}): {df.columns}\n"
                f"Kỳ vọng ({len(PAYSIM_COLUMNS)}): {PAYSIM_COLUMNS}"
            )
        log_fn(f"Cấu trúc cột    : {len(df.columns)} cột [KHỚP CHUẨN PAYSIM]")

        partitions = df.rdd.getNumPartitions()
        log_fn(f"Số partition    : {partitions}")

        log_fn("Đang đếm tổng số dòng (count)...")
        rows = df.count()
        if rows == 0:
            raise ValueError("File raw data không có dòng dữ liệu nào theo Spark count().")
        log_fn(f"Tổng số dòng    : {rows:,} dòng [THANH CONG]")

        # Đảm bảo Spark đọc và parse đầy đủ tất cả các trường trên từng partition
        log_fn("Đang kiểm tra tính toàn vẹn (mapPartitions)...")
        def consume_partition(records):
            n = 0
            for row in records:
                tuple(row)
                n += 1
            yield n

        parsed_rows = df.rdd.mapPartitions(consume_partition).sum()
        if parsed_rows != rows:
            raise ValueError(f"Số dòng parse được ({parsed_rows:,}) không khớp với count ({rows:,}).")
        log_fn(f"Parsed rows     : {parsed_rows:,} dòng [THANH CONG]")

        # Lấy schema và dữ liệu mẫu 5 dòng
        schema_tree = df._jdf.schema().treeString() if hasattr(df, "_jdf") else str(df.schema)
        sample_table = df._jdf.showString(5, 20, False) if hasattr(df, "_jdf") else ""
        sample_dicts = [row.asDict() for row in df.limit(5).collect()]

        log_fn("\n--- SCHEMA DỮ LIỆU ---")
        for line in schema_tree.strip().splitlines():
            log_fn(line)

        log_fn("\n--- 5 DÒNG DỮ LIỆU MẪU ---")
        for line in sample_table.strip().splitlines():
            log_fn(line)

        spark_meta = {
            "run_id": run_id,
            "destination": str(raw_path),
            "spark_version": spark.version,
            "master": spark.sparkContext.master,
            "rows": rows,
            "parsed_rows": parsed_rows,
            "partitions": partitions,
            "columns": df.columns,
            "sample_records": sample_dicts,
            "status": "PASS",
            "inspected_at_utc": datetime.now(timezone.utc).isoformat(),
        }

        # Lưu báo cáo JSON
        target_report_dir = report_dir or (PROJECT_ROOT / "metadata")
        target_report_dir.mkdir(parents=True, exist_ok=True)
        report_filename = f"spark_read_{run_id}.json" if run_id else "spark_read_latest.json"
        report_file = target_report_dir / report_filename
        with report_file.open("w", encoding="utf-8") as f:
            json.dump(spark_meta, f, ensure_ascii=False, indent=2)

        log_fn(f"\nĐã lưu báo cáo Spark tại: {report_file}")
        log_fn("KẾT QUẢ SPARK RAW: PASS")
        return spark_meta

    finally:
        try:
            spark.stop()
        except Exception:
            pass


def ingest(
    source,
    dataset="paysim",
    raw_root=None,
    logger=None,
    spark_check=True,
    spark_master=None,
):
    """Nạp dữ liệu vào raw storage, sinh manifest và kiểm tra bằng Spark (nếu bật)."""
    raw_root = Path(raw_root).resolve() if raw_root else PROJECT_ROOT / "data" / "raw"

    now = datetime.now(timezone.utc)
    run_id = f"{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"
    date_str = now.strftime("%Y-%m-%d")

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
        # Fail-fast với nguồn local trước khi tạo thư mục
        if urlparse(source).scheme not in ("http", "https"):
            validate_csv_source(Path(source).expanduser().resolve(), PAYSIM_COLUMNS)

        destination_dir.mkdir(parents=True, exist_ok=False)

        copy_source(source, temp_destination, logger)

        file_size = temp_destination.stat().st_size
        if file_size == 0:
            raise ValueError("File tải về rỗng (0 bytes).")

        # Validate nội dung cho MỌI loại nguồn (URL/local) trước khi công bố
        validate_csv_source(temp_destination, PAYSIM_COLUMNS)

        temp_destination.replace(destination_path)
        logger.info(f"Đã lưu file raw thành công ({file_size:,} bytes).")

        logger.info("Đang tính mã băm SHA-256...")
        file_hash = compute_sha256(destination_path)
        logger.info(f"Mã SHA-256: {file_hash}")

        manifest.update(bytes=file_size, hash=file_hash, sha256=file_hash, status="SUCCESS")

        # Kiểm tra raw data bằng PySpark (nếu được kích hoạt)
        if spark_check:
            logger.info("")
            spark_meta = inspect_raw_with_spark(
                raw_path=destination_path,
                run_id=run_id,
                logger=logger,
                master=spark_master,
                report_dir=PROJECT_ROOT / "metadata",
            )
            # Lưu thêm bản sao báo cáo Spark trong chính thư mục run raw
            run_spark_report = destination_dir / f"spark_read_{run_id}.json"
            with run_spark_report.open("w", encoding="utf-8") as f:
                json.dump(spark_meta, f, ensure_ascii=False, indent=2)

            manifest.update(
                spark_verified=True,
                spark_rows=spark_meta["rows"],
                spark_version=spark_meta["spark_version"],
                spark_report=str(run_spark_report.resolve()),
            )

        write_manifest(manifest_run, manifest)
        write_manifest(manifest_central, manifest)

        logger.info(f"Đã lưu Manifest tại: {manifest_run}")
        logger.info(f"Đã cập nhật Manifest tổng: {manifest_central}")
        logger.info(f"INGESTION SUCCESS: {destination_path}")
        return manifest

    except Exception as e:
        logger.exception(f"LỖI INGESTION: {e}")

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
        description="Script nạp dữ liệu PaySim vào Raw Storage và kiểm tra bằng PySpark tuân thủ Checkpoint 2."
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
    parser.add_argument(
        "--with-spark",
        dest="with_spark",
        action="store_true",
        default=True,
        help="Tự động kiểm tra raw data bằng PySpark sau khi ingest (mặc định: Bật).",
    )
    parser.add_argument(
        "--no-spark",
        dest="with_spark",
        action="store_false",
        help="Bỏ qua bước kiểm tra raw data bằng PySpark.",
    )
    parser.add_argument(
        "--spark-master",
        default=None,
        help="Spark master URL (mặc định: lấy từ biến môi trường SPARK_MASTER hoặc 'local[*]').",
    )
    parser.add_argument(
        "--inspect-only",
        default=None,
        help="Chỉ thực hiện đọc và kiểm tra file raw data hoặc manifest bằng Spark (không sao chép dữ liệu mới).",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_uuid = uuid.uuid4().hex[:8]
    log_file = PROJECT_ROOT / "logs" / f"ingest_{now_str}_{run_uuid}.txt"

    logger = setup_logger(log_file)

    # Chế độ chỉ kiểm tra file raw data hoặc manifest bằng PySpark
    if args.inspect_only:
        try:
            inspect_raw_with_spark(
                raw_path=args.inspect_only,
                logger=logger,
                master=args.spark_master,
            )
            print("\nSPARK RAW INSPECTION SUCCESS.")
            return 0
        except Exception:
            logger.exception("SPARK RAW INSPECTION FAILED.")
            print("\nSPARK RAW INSPECTION FAILED. Xem chi tiết trong file log.", file=sys.stderr)
            return 1

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
            spark_check=args.with_spark,
            spark_master=args.spark_master,
        )
        print(f"\nSUCCESS manifest={manifest['destination']}")
        return 0
    except Exception:
        print("\nINGESTION FAILED. Xem chi tiết trong file log.", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())

