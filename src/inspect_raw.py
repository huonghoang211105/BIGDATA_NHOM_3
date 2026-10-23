#!/usr/bin/env python3
"""Read an ingested PaySim CSV with Spark and report its schema and contents."""

import argparse
import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession

PROJECT_ROOT = Path(__file__).resolve().parent.parent


def parse_args():
    parser = argparse.ArgumentParser(
        description="Read an ingested raw CSV with Spark and show its schema, count, and sample."
    )
    parser.add_argument(
        "--manifest",
        default=str(PROJECT_ROOT / "metadata" / "manifest.json"),
        help="Path to a successful ingestion manifest.",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    manifest_path = Path(args.manifest).expanduser().resolve()

    with manifest_path.open(encoding="utf-8") as manifest_file:
        manifest = json.load(manifest_file)

    if manifest.get("status") != "SUCCESS":
        raise ValueError(f"Manifest is not successful: {manifest_path}")

    raw_path = Path(manifest["destination"])
    if not raw_path.is_file():
        raise FileNotFoundError(f"Raw CSV does not exist: {raw_path}")

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("inspect-paysim-raw")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(str(raw_path))
        )
        print(f"Manifest: {manifest_path}")
        print(f"Raw file: {raw_path}")
        print("\nSchema:")
        df.printSchema()
        print(f"Row count: {df.count():,}")
        print("\nSample (5 rows):")
        df.show(5, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    try:
        sys.exit(main() or 0)
    except Exception as error:
        print(f"ERROR: {error}", file=sys.stderr)
        sys.exit(1)
