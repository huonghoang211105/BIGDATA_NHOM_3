"""CP4 correctness test: xxhash64-prefilter duplicate check == old exact groupBy on all columns.

Run from the repo root (no pytest needed; pytest also collects the test_* functions):
    python tests/test_dup_check.py
Exit code 0 = all PASS, 1 = a case failed.
"""
from decimal import Decimal
import os
from pathlib import Path
import sys
import traceback

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
if 'HADOOP_HOME' not in os.environ:
    for cand in [Path('D:/hadoop'), Path('C:/hadoop'), ROOT / 'hadoop']:
        if cand.is_dir():
            os.environ['HADOOP_HOME'] = str(cand.resolve())
            os.environ['PATH'] = f"{(cand / 'bin').resolve()};{os.environ.get('PATH', '')}"
            break

from pyspark.sql import SparkSession, functions as F, types as T  # noqa: E402
from quality_report import duplicate_candidates, duplicate_profile  # noqa: E402

MONEY = T.DecimalType(20, 2)
SCHEMA = T.StructType([T.StructField(n, t, True) for n, t in [
    ('step', T.IntegerType()), ('type', T.StringType()), ('amount', MONEY), ('nameOrig', T.StringType()),
    ('oldbalanceOrg', MONEY), ('newbalanceOrig', MONEY), ('nameDest', T.StringType()),
    ('oldbalanceDest', MONEY), ('newbalanceDest', MONEY), ('isFraud', T.IntegerType()),
    ('isFlaggedFraud', T.IntegerType())]])
_spark = None


def spark():
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder.master('local[2]').appName('cp4-test-dup-check')
                  .config('spark.ui.enabled', 'false').config('spark.ui.showConsoleProgress', 'false')
                  .config('spark.sql.shuffle.partitions', '4')
                  .getOrCreate())
        _spark.sparkContext.setLogLevel('ERROR')
    return _spark


def old_method(df):
    """Baseline duplicate check, copied verbatim from tag checkpoint-4-baseline (quality_report.py)."""
    duplicates = df.groupBy(*df.columns).count().filter(F.col('count') > 1)
    return duplicates.agg(F.count('*').alias('duplicate_groups'),
                          F.coalesce(F.sum(F.col('count') - 1), F.lit(0))
                          .alias('duplicate_extra_rows')).first().asDict()


def new_method(df):
    details = {}
    result = duplicate_profile(df, details)
    return result, details


def row(i, **over):
    base = dict(step=1 + i % 5, type=['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN'][i % 5],
                amount=Decimal(f'{1000 + i * 37}.{i % 100:02d}'), nameOrig=f'C{100000 + i}',
                oldbalanceOrg=Decimal(f'{5000 + i}.00'), newbalanceOrig=Decimal(f'{4000 + i}.50'),
                nameDest=f'M{200000 + i}', oldbalanceDest=Decimal('0.00'), newbalanceDest=Decimal('0.00'),
                isFraud=0, isFlaggedFraud=0)
    base.update(over)
    return tuple(base[f.name] for f in SCHEMA.fields)


def unique_rows(n):
    return [row(i) for i in range(n)]


def check(name, df, expected_groups, expected_extra, expect_step2=None):
    old = old_method(df)
    new, details = new_method(df)
    ok = (old == new == {'duplicate_groups': expected_groups, 'duplicate_extra_rows': expected_extra})
    if expect_step2 is not None:
        ok = ok and details['exact_step_ran'] == expect_step2
    print(f'[{"PASS" if ok else "FAIL"}] {name}: old={old} new={new} expected=({expected_groups}, '
          f'{expected_extra}) hash_candidates={details["hash_candidates"]} '
          f'exact_step_ran={details["exact_step_ran"]}')
    assert ok, name


def test_mixed_duplicates():
    """~20 rows: 1 exact pair, 1 pair differing in one column, 1 row present 3 times."""
    rows = unique_rows(16)
    rows.append(rows[3])                                    # exact pair -> 1 group, 1 extra
    rows.append(row(5, newbalanceDest=Decimal('0.01')))     # differs from rows[5] in 1 column -> no dup
    rows += [rows[9], rows[9]]                              # rows[9] x3 -> 1 group, 2 extra
    df = spark().createDataFrame(rows, SCHEMA)
    assert df.count() == 20
    check('mixed: 1 pair + 1 near-pair + 1 triple', df, 2, 3, expect_step2=True)


def test_no_duplicates():
    df = spark().createDataFrame(unique_rows(20), SCHEMA)
    check('no duplicates', df, 0, 0, expect_step2=False)


def test_hash_collision_is_verified():
    """Spark's hash skips nulls, so (isFraud=null, isFlaggedFraud=1) and (isFraud=1, isFlaggedFraud=null)
    with all other columns equal share one xxhash64 but are different rows: step 1 flags a candidate,
    step 2 must still report 0 duplicates."""
    rows = unique_rows(18)
    rows.append(row(100, isFraud=None, isFlaggedFraud=1))
    rows.append(row(100, isFraud=1, isFlaggedFraud=None))
    df = spark().createDataFrame(rows, SCHEMA)
    assert duplicate_candidates(df).count() == 1, 'expected a real xxhash64 collision in this fixture'
    check('hash collision, no real duplicate', df, 0, 0, expect_step2=True)


def test_null_rows_duplicate():
    """Identical rows containing nulls are duplicates for groupBy and share a hash."""
    rows = unique_rows(18)
    rows += [row(50, nameDest=None, amount=None)] * 2
    df = spark().createDataFrame(rows, SCHEMA)
    check('duplicate rows with nulls', df, 1, 1, expect_step2=True)


def test_sample_csv_with_injected_duplicate():
    """Real PaySim sample (1.000 rows, same reader options as pipeline) + 1 injected copy of one row."""
    sample = (spark().read.schema(SCHEMA).option('header', True).option('enforceSchema', False)
              .option('mode', 'FAILFAST').csv((ROOT / 'data/sample/paysim_sample_1000.csv').as_posix()))
    check('sample 1000, as is', sample, 0, 0, expect_step2=False)
    injected = sample.unionByName(sample.orderBy('step', 'nameOrig').limit(1))
    assert injected.count() == 1001
    check('sample 1000 + 1 injected duplicate', injected, 1, 1, expect_step2=True)


def main():
    tests = [test_mixed_duplicates, test_no_duplicates, test_hash_collision_is_verified,
             test_null_rows_duplicate, test_sample_csv_with_injected_duplicate]
    failed = 0
    try:
        for test in tests:
            try:
                test()
            except Exception:  # report every case, then fail the run
                failed += 1
                traceback.print_exc()
    finally:
        if _spark is not None:
            _spark.stop()
    print(f'\nRESULT: {"PASS" if not failed else "FAIL"} ({len(tests) - failed}/{len(tests)} tests passed)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
