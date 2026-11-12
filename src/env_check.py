import hashlib
import json
import os
import platform
import re
import shutil
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
SOURCE_META_FILE = PROJECT_ROOT / "metadata" / "source.json"
REQUIREMENTS_FILE = PROJECT_ROOT / "requirements.txt"

# Đường dẫn tương đối so với gốc repo (không ghi ổ đĩa C:\ hay D:\ vào metadata)
PAYSIM_LOCAL_PATH = "data/raw/paysim_data.csv"

PAYSIM_SOURCE = {
    "name": "PaySim - Synthetic Financial Datasets For Fraud Detection",
    "source_type": "Kaggle",
    "source_url": "https://www.kaggle.com/datasets/ealaxi/paysim1",
    "original_filename": "PS_20174392719_1491204439457_log.csv",
    "version_snapshot": "paysim1 - version 2/18-10-2026",
    "local_path": PAYSIM_LOCAL_PATH,
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
    """Trả về (ok, dòng version đầu tiên của `java -version`)."""
    try:
        proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=True)
        raw = (proc.stdout + proc.stderr).strip()
        return True, (raw.splitlines()[0] if raw else "Unknown Java version")
    except Exception as e:
        return False, f"Java check failed: {e}"


def pinned_pyspark_version():
    """Đọc version pyspark được ghim (==) trong requirements.txt."""
    if not REQUIREMENTS_FILE.is_file():
        return None
    for line in REQUIREMENTS_FILE.read_text(encoding="utf-8").splitlines():
        m = re.match(r"^\s*pyspark\s*==\s*([^\s#]+)", line, re.IGNORECASE)
        if m:
            return m.group(1)
    return None


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


def check_environment(logger, failures):
    """Muc 1: ghi nhan Python / OS / Java / JAVA_HOME / HADOOP_HOME / PySpark."""
    logger.log("=" * 60)
    logger.log("KIEM TRA MOI TRUONG (OS -> JAVA -> PYTHON -> PYSPARK)")
    logger.log(f"Thoi gian kiem tra: {datetime.now(timezone.utc).isoformat()}")
    logger.log("=" * 60)

    is_windows = platform.system() == "Windows"
    logger.log(f"[1] He dieu hanh : {platform.system()} {platform.release()} ({platform.machine()})")

    # Java
    java_ok, java_version = get_java_version()
    logger.log(f"[2] Java Version : {java_version}")
    logger.log(f"    java tren PATH: {shutil.which('java') or '(khong tim thay)'}")
    if not java_ok:
        failures.append("khong chay duoc 'java -version'")

    java_home = os.environ.get("JAVA_HOME")
    logger.log(f"    JAVA_HOME     : {java_home or '(chua dat)'}")
    if java_home:
        java_bin = Path(java_home) / "bin" / ("java.exe" if is_windows else "java")
        if not java_bin.is_file():
            failures.append(f"JAVA_HOME khong chua bin/{java_bin.name}")
    else:
        logger.log("    [WARN] JAVA_HOME chua dat, Spark se dung java tren PATH.")

    # Hadoop / winutils (bat buoc tren Windows)
    hadoop_home = os.environ.get("HADOOP_HOME")
    logger.log(f"    HADOOP_HOME   : {hadoop_home or '(chua dat)'}")
    if is_windows:
        winutils = Path(hadoop_home) / "bin" / "winutils.exe" if hadoop_home else None
        logger.log(f"    winutils.exe  : {'CO' if winutils and winutils.is_file() else 'KHONG CO'}")
        if not hadoop_home:
            failures.append("HADOOP_HOME chua dat (Windows can winutils.exe)")
        elif not winutils.is_file():
            failures.append("khong thay HADOOP_HOME\\bin\\winutils.exe")

    # Python
    logger.log(f"[3] Python Version: {platform.python_version()}")
    logger.log(f"    Python path   : {sys.executable}")

    # PySpark
    pinned = pinned_pyspark_version()
    try:
        import pyspark

        installed = pyspark.__version__
        logger.log(f"[4] PySpark Version: {installed} (requirements.txt ghim: {pinned or 'khong ghim'})")
        if pinned and installed != pinned:
            failures.append(f"pyspark dang cai {installed} khac requirements.txt {pinned}")
    except ImportError as e:
        logger.log(f"[4] PySpark: KHONG IMPORT DUOC ({e})")
        failures.append("chua cai pyspark")


def run_smoke_test(logger):
    logger.log("=" * 60)
    logger.log("SMOKE TEST SPARK JOB")
    logger.log("=" * 60)

    master_mode = os.environ.get("SPARK_MASTER", "local[*]")
    logger.log(f"[5] Khoi tao SparkSession voi Master: '{master_mode}'...")

    from pyspark.sql import SparkSession  # loi import se duoc main() bat va ghi log

    # Python worker phai dung cung interpreter voi driver (tranh alias 'python3' cua Microsoft Store)
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    logger.log(f"    PYSPARK_PYTHON = PYSPARK_DRIVER_PYTHON = {sys.executable}")

    spark = (
        SparkSession.builder
        .master(master_mode)
        .appName("env-check")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        logger.log(f"    - Spark Version: {spark.version}")
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

        # Smoke test Python worker: RDD map chay code Python tren worker process
        logger.log("[7] Smoke test Python worker (RDD parallelize -> map -> sum)...")
        worker_sum = spark.sparkContext.parallelize(range(1000), 4).map(lambda x: x * 2).sum()
        if worker_sum != 999000:
            raise RuntimeError(f"Python worker sum sai: {worker_sum} != 999000")
        logger.log(f"    - sum(x*2, x<1000) = {worker_sum:,} [THANH CONG]")

        logger.log("KET QUA SMOKE TEST: PASS")
        return True
    finally:
        spark.stop()


def record_data_source(logger):
    """Muc 2: ghi nhan nguon goc, dung luong, phien ban cua PaySim."""
    logger.log("=" * 60)
    logger.log("NGUON DU LIEU PAYSIM (DATA PROVENANCE)")
    logger.log("=" * 60)

    meta = dict(PAYSIM_SOURCE)
    meta["checked_at"] = datetime.now(timezone.utc).isoformat()
    path = PROJECT_ROOT / meta["local_path"]

    snapshot_sha = None
    if SOURCE_META_FILE.is_file():
        with SOURCE_META_FILE.open(encoding="utf-8") as f:
            snapshot_sha = json.load(f).get("snapshot_sha256")

    if path.exists():
        size = path.stat().st_size
        sha = sha256_of_file(path)
        meta.update(
            file_exists=True,
            file_size_bytes=size,
            file_size_human=human_size(size),
            sha256=sha,
            matches_snapshot_sha256=(sha == snapshot_sha) if snapshot_sha else None,
        )
    else:
        meta.update(file_exists=False, file_size_bytes=None, file_size_human=None, sha256=None,
                    matches_snapshot_sha256=None)
        logger.log(f"[WARN] Chua tim thay file du lieu tai: {meta['local_path']}")

    with open(DATA_SOURCE_FILE, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    logger.log(f"Ten dataset    : {meta['name']}")
    logger.log(f"Nguon          : {meta['source_type']} - {meta['source_url']}")
    logger.log(f"Phien ban      : {meta['version_snapshot']}")
    logger.log(f"Giay phep      : {meta['license']}")
    logger.log(f"Duong dan local: {meta['local_path']}")
    logger.log(f"Dung luong     : {meta['file_size_human']} ({meta['file_size_bytes']} bytes)")
    logger.log(f"SHA-256        : {meta['sha256']}")
    logger.log(f"Khop snapshot_sha256 (metadata/source.json): {meta['matches_snapshot_sha256']}")
    logger.log("Metadata luu tai: logs/data_source.json")


def main():
    with DualLogger(LOG_FILE) as logger:
        failures = []

        try:
            check_environment(logger, failures)
        except Exception as e:
            logger.log(traceback.format_exc())
            failures.append(f"kiem tra bien moi truong loi: {e}")

        try:
            logger.log("")
            run_smoke_test(logger)
        except Exception as e:
            logger.log(f"\n[ERROR] Smoke test that bai: {e}")
            logger.log(traceback.format_exc())
            failures.append(f"Spark smoke test that bai: {e}")

        # Muc 2 chay doc lap, smoke test loi van ghi duoc metadata
        try:
            logger.log("")
            record_data_source(logger)
        except Exception as e:
            logger.log(f"[ERROR] Ghi nhan nguon du lieu that bai: {e}")
            failures.append(f"ghi nhan nguon du lieu that bai: {e}")

        logger.log("")
        if failures:
            logger.log("ENV FAIL: " + "; ".join(failures))
            return 1
        logger.log("ENV PASS")
        return 0


if __name__ == "__main__":
    sys.exit(main())
