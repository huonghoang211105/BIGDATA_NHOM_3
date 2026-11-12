"""Compare persisted business results of two successful runs, ignoring file layout.

Exit code: 0 = RERUN PASS (identical schema and rows both ways), 1 = different / not comparable.
Run dirs may be repo-relative (output/snapshot_.../<run_id>) or absolute. Only input_sha256,
dimension_sha256 and run_id are read from metrics.json, so older runs that stored absolute
paths in metrics remain comparable.
"""
import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def resolve_run(value):
    path = Path(value)
    return (path if path.is_absolute() else ROOT / path).resolve()


def rel(path):
    try:
        return path.relative_to(ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def fail(message):
    print(f'RERUN FAIL: {message}')
    return 1


def main():
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
