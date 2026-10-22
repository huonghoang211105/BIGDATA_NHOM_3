import hashlib
import json
import os
import platform
import subprocess
import sys
import traceback
from datetime import datetime, timezone
from pathlib import Path

# Đảm bảo mã hóa UTF-8 trên mọi nền tảng (Windows / Linux / Kaggle)
for _stream in (sys.stdout, sys.stderr):
    if hasattr(_stream, "reconfigure"):
        try:
            _stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass

PROJECT_ROOT = Path(__file__).resolve().parent.parent
LOG_DIR = PROJECT_ROOT / "logs"
LOG_FILE = LOG_DIR / "env_check.txt"
DATA_SOURCE_FILE = LOG_DIR / "data_source.json"


PAYSIM_SOURCE = {
    "name": "PaySim - Synthetic Financial Datasets For Fraud Detection",
    "source_type": "Kaggle",
    "source_url": "https://www.kaggle.com/datasets/ealaxi/paysim1",
    "original_filename": "PS_20174392719_1491204439457_log.csv",
    "version_snapshot": "paysim1 - dien version/ngay tai ve tu trang Kaggle",
    "local_path": str(PROJECT_ROOT / "data" / "raw" / "PS_20174392719_1491204439457_log.csv"),
    "license": "CC BY-SA 4.0",
}


class DualLogger:
    """Ghi đồng thời ra console và file log logs/env_check.txt."""

    def __init__(self, filepath):
        self.terminal = sys.stdout
        self.filepath = filepath
        self.log_file = None

    def __enter__(self):
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self.log_file = open(self.filepath, "w", encoding="utf-8")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.log_file:
            self.log_file.close()

    def log(self, message=""):
        try:
            self.terminal.write(message + "\n")
            self.terminal.flush()
        except Exception:
            self.terminal.write(message.encode("ascii", "replace").decode("ascii") + "\n")
            self.terminal.flush()
        if self.log_file:
            self.log_file.write(message + "\n")
            self.log_file.flush()


def get_java_version():
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
        raw = (proc.stdout + proc.stderr).strip()
        return raw.splitlines()[0] if raw else "Unknown Java version"
    except Exception as e:
        return f"Java check failed: {e}"


def human_size(num_bytes):
    size = float(num_bytes)
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024
    return f"{size:.2f} PB"


def sha256_of_file(path, chunk_size=1024 * 1024):
    h = hashlib.sha256()
    with open(path, "rb") as f:
        while chunk := f.read(chunk_size):
            h.update(chunk)
    return h.hexdigest()


def record_data_source(logger):
    """Muc 2: ghi nhan nguon goc, dung luong, phien ban cua PaySim."""
    logger.log("=" * 60)
    logger.log("NGUON DU LIEU PAYSIM (DATA PROVENANCE)")
    logger.log("=" * 60)

    meta = dict(PAYSIM_SOURCE)
    meta["checked_at"] = datetime.now(timezone.utc).isoformat()
    path = Path(meta["local_path"])

    if path.exists():
        size = path.stat().st_size
        meta.update(
            file_exists=True,
            file_size_bytes=size,
            file_size_human=human_size(size),
            sha256=sha256_of_file(path),
        )
    else:
        meta.update(file_exists=False, file_size_bytes=None, file_size_human=None, sha256=None)
        logger.log(f"[WARN] Chua tim thay file du lieu tai: {path}")

    with open(DATA_SOURCE_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    logger.log(f"Ten dataset    : {meta['name']}")
    logger.log(f"Nguon          : {meta['source_type']} - {meta['source_url']}")
    logger.log(f"Phien ban      : {meta['version_snapshot']}")
    logger.log(f"Giay phep      : {meta['license']}")
    logger.log(f"Duong dan local: {meta['local_path']}")
    logger.log(f"Dung luong     : {meta['file_size_human']} ({meta['file_size_bytes']} bytes)")
    logger.log(f"SHA-256        : {meta['sha256']}")
    logger.log(f"Metadata luu tai: {DATA_SOURCE_FILE}")


def run_smoke_test(logger):
    logger.log("=" * 60)
    logger.log("SMOKE TEST KIEM TRA MOI TRUONG PYSPARK")
    logger.log(f"Thoi gian kiem tra: {datetime.now(timezone.utc).isoformat()}")
    logger.log("=" * 60)

    logger.log(f"[1] Python Version: {platform.python_version()}")
    logger.log(f"[2] He dieu hanh: {platform.system()} {platform.release()} ({platform.machine()})")
    logger.log(f"[3] Java Version: {get_java_version()}")

    master_mode = os.environ.get("SPARK_MASTER", "local[*]")
    logger.log(f"[4] Khoi tao SparkSession voi Master: '{master_mode}'...")

    from pyspark.sql import SparkSession  # loi import se duoc main() bat va ghi log

    spark = (
        SparkSession.builder
        .master(master_mode)
        .appName("env-check")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.log(f"[5] Spark Version: {spark.version}")
        logger.log(f"    - App Name: {spark.sparkContext.appName}")
        logger.log(f"    - Master  : {spark.sparkContext.master}")

        n = 1_000_000
        logger.log(f"[6] Thuc hien phep tinh phan tan tren DataFrame {n:,} dong...")
        df = spark.range(n)
        logger.log(f"    - So partition: {df.rdd.getNumPartitions()}")

        count_result = df.count()
        if count_result != n:
            raise RuntimeError(f"Count sai: {count_result} != {n}")
        logger.log(f"    - Count : {count_result:,} dong [THANH CONG]")

        even = df.filter("id % 2 == 0").count()
        if even != n // 2:
            raise RuntimeError(f"Filter sai: {even} != {n // 2}")
        logger.log(f"    - Filter (chan): {even:,} dong [THANH CONG]")

        logger.log("KET QUA SMOKE TEST: ENV PASS")
        return True
    finally:
        spark.stop()


def main():
    with DualLogger(LOG_FILE) as logger:
        ok = False
        try:
            ok = run_smoke_test(logger)
        except Exception as e:
            logger.log(f"\n[ERROR] Smoke test that bai: {e}")
            logger.log(traceback.format_exc())
            logger.log("KET QUA SMOKE TEST: ENV FAILED")

        # Muc 2 chay doc lap, smoke test loi van ghi duoc metadata
        try:
            logger.log("")
            record_data_source(logger)
        except Exception as e:
            logger.log(f"[ERROR] Ghi nhan nguon du lieu that bai: {e}")
            ok = False

        return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())