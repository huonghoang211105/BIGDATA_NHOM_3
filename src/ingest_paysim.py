#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script Ingestion Nạp Dữ Liệu PaySim vào Raw Storage
Tuân thủ đầy đủ các yêu cầu Checkpoint 2 (Buổi 6):
1. Nạp tự động từ Local Path hoặc URL HTTP(S).
2. Đảm bảo tính bất biến (Immutability): Copy binary nguyên vẹn, không chỉnh sửa hay ETL.
3. Rerun idempotent theo SHA-256: nếu đã có run SUCCESS cùng sha256 thì SKIPPED_DUPLICATE
   (không copy); cờ --force để cố ý tạo run mới.
4. Quality gate trước khi công bố raw: tồn tại, bytes > 0, header 11 cột PaySim,
   sha256 khớp snapshot_sha256 trong metadata/source.json.
5. Manifest + log truy vết: log logs/ingest_<run_id>.txt khớp run_id trong manifest;
   mọi lần chạy append 1 dòng vào metadata/ingest_history.jsonl.
   Mọi đường dẫn ghi ra là đường dẫn tương đối so với gốc repo.
6. Kiểm tra Raw Data bằng PySpark: schema, count, partitions, sample, lưu
   spark_read_<run_id>.json trong chính thư mục run.
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
SOURCE_META_FILE = PROJECT_ROOT / "metadata" / "source.json"
HISTORY_FILE = PROJECT_ROOT / "metadata" / "ingest_history.jsonl"
CENTRAL_MANIFEST = PROJECT_ROOT / "metadata" / "manifest.json"

# Danh sách 11 cột chuẩn của bộ dữ liệu PaySim
PAYSIM_COLUMNS = [
    "step", "type", "amount", "nameOrig", "oldbalanceOrg",
    "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest",
    "isFraud", "isFlaggedFraud"
]

RERUN_POLICY = (
    "Idempotent theo SHA-256: nếu data/raw/<dataset>/ đã có manifest SUCCESS cùng sha256 "
    "thì SKIPPED_DUPLICATE (không copy, không tạo thư mục mới); dùng --force để cố ý tạo run mới."
)


def to_repo_rel(path):
    """Đường dẫn tương đối so với gốc repo, dạng data/raw/... (POSIX)."""
    p = Path(path).resolve()
    try:
        return p.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        # Nằm ngoài repo: không thể tương đối hoá
        return p.as_posix()


def from_repo_rel(path_str):
    """Ngược lại của to_repo_rel: đường dẫn tương đối được tính từ gốc repo."""
    p = Path(path_str)
    return p if p.is_absolute() else PROJECT_ROOT / p


def new_run_id(now):
    return f"{now.strftime('%Y%m%dT%H%M%SZ')}_{uuid.uuid4().hex[:8]}"


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
    shown = to_repo_rel(source_path)
    if not source_path.is_file():
        raise FileNotFoundError(f"File nguồn không tồn tại: {shown}")

    file_size = source_path.stat().st_size
    if file_size == 0:
        raise ValueError(f"File nguồn rỗng (0 bytes): {shown}")

    with source_path.open(encoding="utf-8-sig", newline="", errors="replace") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if not header:
            raise ValueError(f"File CSV rỗng, không có dòng tiêu đề: {shown}")

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
            raise ValueError(f"File CSV chỉ có header, không có dòng dữ liệu nào: {shown}")


def load_snapshot_sha256(dataset):
    """Đọc snapshot_sha256 của dataset từ metadata/source.json."""
    if not SOURCE_META_FILE.is_file():
        raise FileNotFoundError("Thiếu metadata/source.json (cần snapshot_sha256 cho quality gate).")
    with SOURCE_META_FILE.open(encoding="utf-8") as f:
        meta = json.load(f)
    if meta.get("dataset") != dataset or not meta.get("snapshot_sha256"):
        raise ValueError(f"metadata/source.json không có snapshot_sha256 cho dataset '{dataset}'.")
    return meta["snapshot_sha256"].lower()


def quality_gate(file_path, expected_sha, logger):
    """Tồn tại, bytes > 0, header đủ 11 cột PaySim, sha256 khớp snapshot. Trả về (bytes, sha256)."""
    validate_csv_source(file_path, PAYSIM_COLUMNS)
    size = file_path.stat().st_size
    logger.info(f"  - Tồn tại, {size:,} bytes > 0, header 11 cột PaySim: OK")
    sha = compute_sha256(file_path)
    if sha != expected_sha:
        raise ValueError(
            f"SHA-256 không khớp snapshot_sha256 trong metadata/source.json.\n"
            f"Thực tế : {sha}\nKỳ vọng: {expected_sha}"
        )
    logger.info(f"  - SHA-256 {sha} khớp snapshot_sha256: OK")
    return size, sha


def find_duplicate_run(dataset_dir, sha256):
    """
    Tìm run SUCCESS cũ nhất cùng sha256 trong data/raw/<dataset>/, với điều kiện file raw
    của run đó vẫn tồn tại và kích thước file == bytes ghi trong manifest cũ.
    """
    if not dataset_dir.is_dir():
        return None
    for manifest_path in sorted(dataset_dir.glob("ingest_date=*/run_*/manifest.json")):
        try:
            with manifest_path.open(encoding="utf-8") as f:
                m = json.load(f)
        except (OSError, json.JSONDecodeError):
            continue
        if m.get("status") != "SUCCESS" or m.get("sha256") != sha256 or not m.get("destination"):
            continue
        raw_file = from_repo_rel(m["destination"])
        if raw_file.is_file() and raw_file.stat().st_size == m.get("bytes"):
            return m
    return None


def append_history(record):
    """Mỗi lần chạy append đúng 1 dòng JSON vào metadata/ingest_history.jsonl."""
    HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    with HISTORY_FILE.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")


def cleanup_run_dir(destination_dir, dataset_dir):
    """Xoá thư mục run dở dang và các thư mục cha nếu đã rỗng (không để lại thư mục raw rỗng)."""
    shutil.rmtree(destination_dir, ignore_errors=True)
    for d in (destination_dir.parent, dataset_dir):
        try:
            d.rmdir()  # chỉ xoá được khi rỗng
        except OSError:
            pass


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
        shown = to_repo_rel(source_path)
        if not source_path.exists():
            raise FileNotFoundError(f"Đường dẫn nguồn local không tồn tại: {shown}")
        if not source_path.is_file():
            raise ValueError(f"Nguồn dữ liệu không phải là một tệp hợp lệ: {shown}")
        if source_path.stat().st_size == 0:
            raise ValueError(f"Tệp nguồn rỗng (0 bytes): {shown}")

        logger.info(f"Đang sao chép binary từ file local: {shown}")
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
            "Chưa cài đặt PySpark. Vui lòng cài đặt bằng lệnh: pip install -r requirements.txt"
        ) from e

    master_mode = master or os.environ.get("SPARK_MASTER", "local[*]")
    # Local workers must use the same Python as the driver. On Windows,
    # Spark's default "python3" may resolve to a Microsoft Store alias.
    is_local = master_mode == "local" or master_mode.startswith("local[")
    if is_local:
        os.environ["PYSPARK_PYTHON"] = sys.executable
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    spark = (
        SparkSession.builder
        .master(master_mode)
        .appName(app_name)
        .config("spark.ui.showConsoleProgress", "false")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    if is_local:
        # Also covers an existing SparkContext created before this function.
        spark.sparkContext.pythonExec = sys.executable
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
    Đường dẫn tương đối được tính từ gốc repo. Chỉ ghi spark_read_<run_id>.json khi có report_dir
    (chế độ chỉ đọc qua inspect_raw.py không ghi gì vào raw zone hay metadata/).
    """
    raw_path = from_repo_rel(raw_path).resolve()

    # Nếu truyền vào file manifest.json, tự động trích xuất destination path và run_id
    if raw_path.suffix.lower() == ".json":
        if not raw_path.is_file():
            raise FileNotFoundError(f"Không tìm thấy manifest: {to_repo_rel(raw_path)}")
        with raw_path.open("r", encoding="utf-8") as f:
            manifest_info = json.load(f)
        if "destination" in manifest_info:
            if not run_id:
                run_id = manifest_info.get("run_id")
            raw_path = from_repo_rel(manifest_info["destination"]).resolve()

    raw_rel = to_repo_rel(raw_path)
    if not raw_path.is_file():
        raise FileNotFoundError(f"File raw không tồn tại để kiểm tra Spark: {raw_rel}")

    log_fn = logger.info if logger else print

    log_fn("=" * 60)
    log_fn("KIỂM TRA RAW DATA BẰNG PYSPARK (SPARK RAW INSPECTION)")
    log_fn(f"Run ID          : {run_id}")
    log_fn(f"Đường dẫn file raw: {raw_rel}")
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
            "destination": raw_rel,
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

        if report_dir is not None:
            report_dir = Path(report_dir)
            report_dir.mkdir(parents=True, exist_ok=True)
            report_filename = f"spark_read_{run_id}.json" if run_id else "spark_read_latest.json"
            report_file = report_dir / report_filename
            with report_file.open("w", encoding="utf-8") as f:
                json.dump(spark_meta, f, ensure_ascii=False, indent=2)
            spark_meta["report_file"] = to_repo_rel(report_file)
            log_fn(f"\nĐã lưu báo cáo Spark tại: {spark_meta['report_file']}")

        log_fn("KẾT QUẢ SPARK RAW: PASS")
        return spark_meta

    finally:
        try:
            spark.stop()
        except Exception:
            pass


def ingest(
    source,
    run_id,
    now,
    log_file,
    logger,
    dataset="paysim",
    raw_root=None,
    spark_check=True,
    spark_master=None,
    force=False,
):
    """
    Nạp dữ liệu vào raw storage. Không ném lỗi ra ngoài: luôn trả về bản ghi lịch sử
    với status SUCCESS / SKIPPED_DUPLICATE / FAILED.
    """
    raw_root = from_repo_rel(raw_root).resolve() if raw_root else PROJECT_ROOT / "data" / "raw"
    is_url = urlparse(source).scheme in ("http", "https")
    if not is_url:
        # --source local tương đối được tính từ gốc repo (giống --raw-root), không theo CWD
        source = str(from_repo_rel(Path(source).expanduser()).resolve())
    source_label = source if is_url else to_repo_rel(source)

    dataset_dir = raw_root / dataset
    date_str = now.strftime("%Y-%m-%d")
    destination_dir = dataset_dir / f"ingest_date={date_str}" / f"run_{run_id}"
    filename = extract_filename(source)
    destination_path = destination_dir / filename
    temp_destination = destination_dir / (filename + ".part")

    record = {
        "run_id": run_id,
        "time_utc": now.isoformat(),
        "dataset": dataset,
        "source": source_label,
        "sha256": None,
        "bytes": None,
        "status": "FAILED",
        # destination chỉ trỏ tới file thật (SUCCESS / SKIPPED_DUPLICATE); FAILED giữ null
        "destination": None,
        "planned_destination": to_repo_rel(destination_path),
        "log_file": to_repo_rel(log_file),
        "forced": force,
    }

    logger.info("=" * 60)
    logger.info("BẮT ĐẦU JOB INGESTION DỮ LIỆU PAYSIM")
    logger.info(f"Run ID: {run_id}")
    logger.info(f"Nguồn: {source_label}")
    logger.info(f"Đích dự kiến: {record['planned_destination']}")
    logger.info(f"Log: {record['log_file']}")
    logger.info("=" * 60)

    created_dir = False
    try:
        expected_sha = load_snapshot_sha256(dataset)
        logger.info(f"snapshot_sha256 kỳ vọng (metadata/source.json): {expected_sha}")

        if not is_url:
            # Quality gate + hash trên nguồn TRƯỚC khi copy (fail-fast, chưa tạo thư mục)
            logger.info("Quality gate trên file nguồn:")
            src_bytes, src_sha = quality_gate(Path(source), expected_sha, logger)
            record.update(bytes=src_bytes, sha256=src_sha)

            if force:
                logger.info("--force: bỏ qua kiểm tra trùng snapshot, tạo run mới.")
            else:
                dup = find_duplicate_run(dataset_dir, src_sha)
                if dup:
                    record.pop("planned_destination")
                    record.update(
                        status="SKIPPED_DUPLICATE",
                        destination=dup["destination"],
                        duplicate_of=dup["run_id"],
                    )
                    logger.info(f"SKIPPED_DUPLICATE: trùng snapshot với run {dup['run_id']}")
                    logger.info(f"Raw hiện có: {dup['destination']} (không copy, không tạo thư mục mới)")
                    return record

        destination_dir.mkdir(parents=True, exist_ok=False)
        created_dir = True

        copy_source(source, temp_destination, logger)

        # Quality gate trên bản sao trước khi công bố
        logger.info("Quality gate trên bản sao (.part) trước khi công bố:")
        dst_bytes, dst_sha = quality_gate(temp_destination, expected_sha, logger)
        if not is_url and (dst_bytes, dst_sha) != (record["bytes"], record["sha256"]):
            raise ValueError("Bản sao không khớp bytes/sha256 của nguồn (copy lỗi).")
        record.update(bytes=dst_bytes, sha256=dst_sha)

        if is_url and not force:
            dup = find_duplicate_run(dataset_dir, dst_sha)
            if dup:
                cleanup_run_dir(destination_dir, dataset_dir)
                created_dir = False
                record.pop("planned_destination")
                record.update(status="SKIPPED_DUPLICATE", destination=dup["destination"], duplicate_of=dup["run_id"])
                logger.info(f"SKIPPED_DUPLICATE: trùng snapshot với run {dup['run_id']}")
                return record

        temp_destination.replace(destination_path)
        published_rel = to_repo_rel(destination_path)
        logger.info(f"Đã lưu file raw: {published_rel} ({dst_bytes:,} bytes)")

        manifest = {
            "dataset": dataset,
            "run_id": run_id,
            "ingest_time": now.isoformat(),
            "source": source_label,
            "bytes": dst_bytes,
            "sha256": dst_sha,
            "snapshot_sha256": expected_sha,
            "quality_gate": "PASS",
            "destination": published_rel,
            "run_dir": to_repo_rel(destination_dir),
            "log_file": record["log_file"],
            "status": "SUCCESS",
            "rerun_policy": RERUN_POLICY,
        }

        if spark_check:
            logger.info("")
            spark_meta = inspect_raw_with_spark(
                raw_path=destination_path,
                run_id=run_id,
                logger=logger,
                master=spark_master,
                report_dir=destination_dir,
            )
            manifest.update(
                spark_verified=True,
                spark_rows=spark_meta["rows"],
                spark_version=spark_meta["spark_version"],
                spark_report=spark_meta["report_file"],
            )
        else:
            manifest["spark_verified"] = False

        manifest_run = destination_dir / "manifest.json"
        write_manifest(manifest_run, manifest)
        # metadata/manifest.json là con trỏ tới run SUCCESS mới nhất
        write_manifest(CENTRAL_MANIFEST, {**manifest, "run_manifest": to_repo_rel(manifest_run)})

        # Chỉ khi manifest đã ghi xong mới coi là công bố: destination trỏ tới file thật
        record.pop("planned_destination")
        record.update(status="SUCCESS", destination=published_rel)
        logger.info(f"Đã lưu Manifest run: {to_repo_rel(manifest_run)}")
        logger.info("Đã cập nhật con trỏ: metadata/manifest.json")
        logger.info(f"INGESTION SUCCESS: {record['destination']}")
        return record

    except Exception as e:
        if isinstance(e, (FileNotFoundError, ValueError, ConnectionError)):
            logger.error(f"LỖI INGESTION ({type(e).__name__}): {e}")
        else:
            logger.exception(f"LỖI INGESTION ({type(e).__name__}): {e}")

        if created_dir:
            cleanup_run_dir(destination_dir, dataset_dir)
            logger.info(f"Đã xoá thư mục run dở dang: {to_repo_rel(destination_dir)}")

        record.update(status="FAILED", destination=None, error=f"{type(e).__name__}: {e}")
        return record


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
        default="data/raw",
        help="Thư mục gốc lưu trữ raw data, tương đối so với gốc repo (mặc định: data/raw).",
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
        "--force",
        action="store_true",
        help="Cố ý tạo run mới kể cả khi đã có run SUCCESS cùng sha256.",
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
    now = datetime.now(timezone.utc)
    run_id = new_run_id(now)

    # Chế độ chỉ kiểm tra file raw data hoặc manifest bằng PySpark
    if args.inspect_only:
        logger = setup_logger(PROJECT_ROOT / "logs" / f"inspect_{run_id}.txt")
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

    # Log của lần ingest mang đúng run_id của lần đó
    log_file = PROJECT_ROOT / "logs" / f"ingest_{run_id}.txt"
    logger = setup_logger(log_file)

    source = args.source
    if not source:
        source = find_default_source()
        if source:
            logger.info(f"Tự động chọn nguồn dữ liệu tìm thấy: {to_repo_rel(source)}")

    if not source:
        logger.error("Không tìm thấy nguồn dữ liệu mặc định. Chỉ định: --source <đường_dẫn_file_hoặc_URL>")
        record = {
            "run_id": run_id, "time_utc": now.isoformat(), "dataset": args.dataset,
            "source": None, "sha256": None, "bytes": None, "status": "FAILED",
            "destination": None, "log_file": to_repo_rel(log_file), "forced": args.force,
            "error": "Không có --source và không tìm thấy nguồn mặc định",
        }
    else:
        record = ingest(
            source=source,
            run_id=run_id,
            now=now,
            log_file=log_file,
            logger=logger,
            dataset=args.dataset,
            raw_root=args.raw_root,
            spark_check=args.with_spark,
            spark_master=args.spark_master,
            force=args.force,
        )

    append_history(record)
    logger.info("Đã append lịch sử: metadata/ingest_history.jsonl")
    logger.info(f"KẾT QUẢ: {record['status']} (run_id={run_id})")

    if record["status"] in ("SUCCESS", "SKIPPED_DUPLICATE"):
        print(f"\n{record['status']} run_id={run_id} destination={record['destination']}")
        return 0
    print(f"\nINGESTION FAILED run_id={run_id}: {record.get('error')}", file=sys.stderr)
    print(f"Xem log: {record['log_file']}", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
