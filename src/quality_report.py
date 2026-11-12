"""Exact DataFrame checks for CP3; no UDF, cache, or tuning."""
import hashlib
import json
from decimal import Decimal
from pathlib import Path
from pyspark.sql import functions as F


def sha256(path):
    """SHA-256 of a file, streamed in 8 MiB blocks (moved here from the removed prepare_raw.py)."""
    h = hashlib.sha256()
    with Path(path).open('rb') as stream:
        for block in iter(lambda: stream.read(8 * 1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()


def save_json(path, value):
    def convert(item):
        if isinstance(item, Decimal):
            return str(item)
        raise TypeError(type(item).__name__)
    Path(path).write_text(json.dumps(value, ensure_ascii=False, indent=2,
                                     default=convert), encoding='utf-8')


def require(condition, message):
    if not condition:
        raise RuntimeError(message)


def profile(df):
    expressions = [F.count('*').alias('rows')]
    for name in df.columns:
        expressions.append(F.sum(F.when(F.col(name).isNull(), 1).otherwise(0))
                           .alias('null_' + name))
    expressions += [F.min('step').alias('step_min'),
                    F.max('step').alias('step_max'),
                    F.countDistinct('step').alias('distinct_steps'),
                    F.min('amount').alias('amount_min'),
                    F.max('amount').alias('amount_max'),
                    F.sum('amount').alias('total_amount'),
                    F.sum('isFraud').alias('fraud_rows'),
                    F.sum(F.when(F.col('amount') == 0, 1).otherwise(0))
                    .alias('zero_amount_rows')]
    metrics = df.agg(*expressions).first().asDict()
    duplicates = df.groupBy(*df.columns).count().filter(F.col('count') > 1)
    metrics.update(duplicates.agg(F.count('*').alias('duplicate_groups'),
                    F.coalesce(F.sum(F.col('count') - 1), F.lit(0))
                    .alias('duplicate_extra_rows')).first().asDict())
    return metrics
