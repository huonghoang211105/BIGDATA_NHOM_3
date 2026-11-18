"""Test correctness CP4: kiểm trùng bằng hash-prefilter (xxhash64) phải cho ĐÚNG kết quả của cách cũ
(groupBy toàn bộ 11 cột) trên các bộ dữ liệu nhỏ dựng sẵn, gồm cả ca va chạm hash thật và ca chèn dòng trùng.

Chạy từ gốc repo (không cần pytest; nếu có pytest thì nó cũng tự nhận các hàm test_*):
    python tests/test_dup_check.py
Exit code 0 = tất cả PASS, 1 = có ca FAIL.
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
    """SparkSession local[2] dùng chung cho mọi test (tạo một lần, tắt UI cho nhanh)."""
    global _spark
    if _spark is None:
        _spark = (SparkSession.builder.master('local[2]').appName('cp4-test-dup-check')
                  .config('spark.ui.enabled', 'false').config('spark.ui.showConsoleProgress', 'false')
                  .config('spark.sql.shuffle.partitions', '4')
                  .getOrCreate())
        _spark.sparkContext.setLogLevel('ERROR')
    return _spark


def old_method(df):
    """Cách kiểm trùng CŨ, chép nguyên văn từ tag checkpoint-4-baseline (quality_report.py): làm chuẩn so sánh."""
    duplicates = df.groupBy(*df.columns).count().filter(F.col('count') > 1)
    return duplicates.agg(F.count('*').alias('duplicate_groups'),
                          F.coalesce(F.sum(F.col('count') - 1), F.lit(0))
                          .alias('duplicate_extra_rows')).first().asDict()


def new_method(df):
    """Cách MỚI (quality_report.duplicate_profile). Trả về (kết quả, details: hash_candidates, exact_step_ran)."""
    details = {}
    result = duplicate_profile(df, details)
    return result, details


def row(i, **over):
    """Một dòng PaySim giả, khác nhau theo i; over=... ghi đè từng cột (để tạo dòng gần trùng / có null)."""
    base = dict(step=1 + i % 5, type=['PAYMENT', 'TRANSFER', 'CASH_OUT', 'DEBIT', 'CASH_IN'][i % 5],
                amount=Decimal(f'{1000 + i * 37}.{i % 100:02d}'), nameOrig=f'C{100000 + i}',
                oldbalanceOrg=Decimal(f'{5000 + i}.00'), newbalanceOrig=Decimal(f'{4000 + i}.50'),
                nameDest=f'M{200000 + i}', oldbalanceDest=Decimal('0.00'), newbalanceDest=Decimal('0.00'),
                isFraud=0, isFlaggedFraud=0)
    base.update(over)
    return tuple(base[f.name] for f in SCHEMA.fields)


def unique_rows(n):
    """n dòng đôi một khác nhau (không có trùng)."""
    return [row(i) for i in range(n)]


def check(name, df, expected_groups, expected_extra, expect_step2=None):
    """So cách cũ, cách mới và giá trị kỳ vọng; expect_step2 kiểm bước 2 (kiểm chính xác) có chạy hay không.
    In một dòng [PASS]/[FAIL] và assert."""
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
    """20 dòng: 1 cặp trùng toàn dòng, 1 cặp chỉ khác 1 cột, 1 dòng xuất hiện 3 lần -> (2 nhóm, 3 dòng thừa)."""
    rows = unique_rows(16)
    rows.append(rows[3])                                    # cặp trùng -> 1 nhóm, 1 dòng thừa
    rows.append(row(5, newbalanceDest=Decimal('0.01')))     # khác rows[5] đúng 1 cột -> KHÔNG trùng
    rows += [rows[9], rows[9]]                              # rows[9] x3 -> 1 nhóm, 2 dòng thừa
    df = spark().createDataFrame(rows, SCHEMA)
    assert df.count() == 20
    check('mixed: 1 pair + 1 near-pair + 1 triple', df, 2, 3, expect_step2=True)


def test_no_duplicates():
    """Không có trùng -> (0, 0) và bước 2 không chạy (không có hash ứng viên)."""
    df =spark().createDataFrame(unique_rows(20), SCHEMA)
    check('no duplicates', df, 0, 0, expect_step2=False)


def test_hash_collision_is_verified():
    """Va chạm hash THẬT: hàm hash của Spark bỏ qua giá trị null, nên (isFraud=null, isFlaggedFraud=1) và
    (isFraud=1, isFlaggedFraud=null), các cột khác bằng nhau, có cùng xxhash64 dù là hai dòng khác nhau.
    Bước 1 báo 1 hash ứng viên; bước 2 (groupBy đủ cột) vẫn phải trả 0 dòng trùng."""
    rows = unique_rows(18)
    rows.append(row(100, isFraud=None, isFlaggedFraud=1))
    rows.append(row(100, isFraud=1, isFlaggedFraud=None))
    df = spark().createDataFrame(rows, SCHEMA)
    assert duplicate_candidates(df).count() == 1, 'expected a real xxhash64 collision in this fixture'
    check('hash collision, no real duplicate', df, 0, 0, expect_step2=True)


def test_null_rows_duplicate():
    """Hai dòng giống hệt nhau có chứa null: groupBy coi là trùng, và chúng cùng hash -> (1, 1)."""
    rows = unique_rows(18)
    rows += [row(50, nameDest=None, amount=None)] * 2
    df = spark().createDataFrame(rows, SCHEMA)
    check('duplicate rows with nulls', df, 1, 1, expect_step2=True)


def test_sample_csv_with_injected_duplicate():
    """Mẫu PaySim thật (1.000 dòng, cùng tùy chọn đọc như pipeline): nguyên bản -> (0, 0);
    cố ý chèn thêm 1 bản sao của một dòng -> (1, 1), giống cách cũ."""
    sample = (spark().read.schema(SCHEMA).option('header', True).option('enforceSchema', False)
              .option('mode', 'FAILFAST').csv((ROOT / 'data/sample/paysim_sample_1000.csv').as_posix()))
    check('sample 1000, as is', sample, 0, 0, expect_step2=False)
    injected = sample.unionByName(sample.orderBy('step', 'nameOrig').limit(1))
    assert injected.count() == 1001
    check('sample 1000 + 1 injected duplicate', injected, 1, 1, expect_step2=True)


def main():
    """Chạy lần lượt mọi test, báo đủ từng ca rồi mới kết luận; trả về exit code 0 (PASS) / 1 (FAIL)."""
    tests = [test_mixed_duplicates, test_no_duplicates, test_hash_collision_is_verified,
             test_null_rows_duplicate, test_sample_csv_with_injected_duplicate]
    failed = 0
    try:
        for test in tests:
            try:
                test()
            except Exception:  # báo mọi ca lỗi, cuối cùng mới FAIL cả lượt chạy
                failed += 1
                traceback.print_exc()
    finally:
        if _spark is not None:
            _spark.stop()
    print(f'\nRESULT: {"PASS" if not failed else "FAIL"} ({len(tests) - failed}/{len(tests)} tests passed)')
    return 1 if failed else 0


if __name__ == '__main__':
    sys.exit(main())
