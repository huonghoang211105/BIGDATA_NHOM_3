"""Compare persisted business results of two successful runs, ignoring file layout."""
import argparse
import json
import os
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('first', type=Path)
    parser.add_argument('second', type=Path)
    args = parser.parse_args()
    for folder in [args.first, args.second]:
        if not (folder / '_SUCCESS.json').is_file():
            raise RuntimeError(f'Not a successful run: {folder}')
    meta = [json.loads((p / 'metrics.json').read_text(encoding='utf-8'))
            for p in [args.first, args.second]]
    if meta[0]['input_sha256'] != meta[1]['input_sha256']:
        raise RuntimeError('Different input snapshots')
    if meta[0]['dimension_sha256'] != meta[1]['dimension_sha256']:
        raise RuntimeError('Different dimension snapshots')
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
    try:
        a, b = [spark.read.parquet((p / 'daily_type_summary').resolve().as_posix())
                for p in [args.first, args.second]]
        if a.schema != b.schema:
            raise RuntimeError('Output schemas differ')
        b = b.select(*a.columns)
        if a.exceptAll(b).limit(1).count() or b.exceptAll(a).limit(1).count():
            raise RuntimeError('Business output differs')
        print('RERUN PASS: identical schema and business rows in both directions')
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
