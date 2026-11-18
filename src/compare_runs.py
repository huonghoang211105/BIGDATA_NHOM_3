"""So sánh kết quả nghiệp vụ đã lưu (Parquet daily_type_summary) của hai run SUCCESS, bỏ qua bố cục file.

Exit code: 0 = RERUN PASS (schema giống hệt, mọi dòng giống nhau theo cả hai chiều), 1 = khác / không so được.
Thư mục run có thể là tương đối theo gốc repo (output/snapshot_.../<run_id>) hoặc tuyệt đối. Chỉ đọc
input_sha256, dimension_sha256 và run_id trong metrics.json: hai run phải cùng snapshot input và dimension.
"""
import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def resolve_run(value):
    """Đường dẫn run tương đối theo gốc repo hoặc tuyệt đối -> Path tuyệt đối."""
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def rel(path):
    """Đường dẫn hiển thị tương đối theo gốc repo nếu được."""
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def fail(message):
    """In "RERUN FAIL: ..." và trả exit code 1."""
    print(f'RERUN FAIL: {message}')
    return 1


def main():
    """So hai run: cùng snapshot input/dimension, cùng schema, và exceptAll hai chiều = 0 dòng.
    Trả về exit code 0 (RERUN PASS) hoặc 1.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument('first', help='Run dir A (repo-relative or absolute)')
    parser.add_argument('second', help='Run dir B (repo-relative or absolute)')
    args = parser.parse_args()
    runs = [resolve_run(args.first), resolve_run(args.second)]
    for folder in runs:
        if not (folder / '_SUCCESS.json').is_file():
            return fail(f'Not a successful run: {rel(folder)}')
    meta = [json.loads((p / 'metrics.json').read_text(encoding='utf-8')) for p in runs]
    for label, folder, m in zip('AB', runs, meta):
        print(f'Run {label}: run_id={m.get("run_id", folder.name)} dir={rel(folder)}')
    if meta[0]['input_sha256'] != meta[1]['input_sha256']:
        return fail('Different input snapshots')
    if meta[0]['dimension_sha256'] != meta[1]['dimension_sha256']:
        return fail('Different dimension snapshots')
    print(f'input_sha256={meta[0]["input_sha256"]} dimension_sha256={meta[0]["dimension_sha256"]} (same)')
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    if 'HADOOP_HOME' not in os.environ:
        for hadoop_cand in [Path('D:/hadoop'), Path('C:/hadoop'), ROOT / 'hadoop']:
            if hadoop_cand.is_dir():
                os.environ['HADOOP_HOME'] = str(hadoop_cand.resolve())
                os.environ['PATH'] = f"{str((hadoop_cand / 'bin').resolve())};{os.environ.get('PATH', '')}"
                break
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local[*]').appName('cp3-rerun-check').getOrCreate()
    spark.sparkContext.setLogLevel('WARN')
    try:
        a, b = [spark.read.parquet((p / 'daily_type_summary').as_posix()) for p in runs]
        print(f'Rows: A={a.count()} B={b.count()}')
        if a.schema != b.schema:
            print(f'Schema A: {a.schema.simpleString()}')
            print(f'Schema B: {b.schema.simpleString()}')
            return fail('Output schemas differ')
        b = b.select(*a.columns)
        only_a, only_b = a.exceptAll(b).count(), b.exceptAll(a).count()
        print(f'Rows only in A={only_a}, only in B={only_b}')
        if only_a or only_b:
            return fail('Business output differs')
        print('RERUN PASS: identical schema and business rows in both directions')
        return 0
    finally:
        spark.stop()


if __name__ == '__main__':
    sys.exit(main())
