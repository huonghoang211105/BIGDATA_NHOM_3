"""Cổng correctness CP4: so output bản tối ưu với baseline cho một session benchmark.

    python src/check_correctness.py --session exp1
    -> evidence/checkpoint4/<session>/correctness.json, in PASS/FAIL từng kiểm tra, exit 0 = tất cả PASS.

Kiểm (mọi run của session, kể cả warm-up, cả hai nhãn):
  * bất biến trong metrics.json: rows raw = valid = joined = expected_rows, unmatched 0, invalid 0, null 0,
    duplicate_groups 0, duplicate_extra_rows 0, 16 dòng amount = 0, fraud, tổng tiền, 152 nhóm;
  * Parquet output đã ghi: schema = baseline (10 cột, cùng nội dung schema_output.txt), khóa (day_index, type)
    duy nhất, và TỪNG giá trị bằng run tham chiếu sau khi sort theo (day_index, type) + checksum SHA-256
    của các dòng đã sort + exceptAll hai chiều;
  * tests/test_dup_check.py (kiểm trùng cũ vs mới, chèn dòng trùng) trả exit 0.
Run tham chiếu = run baseline đo đầu tiên của session.
"""
import argparse
import csv
from decimal import Decimal
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
EXPECTED_SCHEMA_COLS = 10
EXPECTED_ZERO_AMOUNT = 16


def spark_session():
    """Khởi động SparkSession local[*] (đặt PYSPARK_PYTHON, HADOOP_HOME như pipeline)."""
    os.environ['PYSPARK_PYTHON'] = sys.executable
    os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
    if 'HADOOP_HOME' not in os.environ:
        for cand in [Path('D:/hadoop'), Path('C:/hadoop'), ROOT / 'hadoop']:
            if cand.is_dir():
                os.environ['HADOOP_HOME'] = str(cand.resolve())
                os.environ['PATH'] = f"{(cand / 'bin').resolve()};{os.environ.get('PATH', '')}"
                break
    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master('local[*]').appName('cp4-check-correctness')
             .config('spark.ui.showConsoleProgress', 'false').getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark


class Report:
    def __init__(self):
        """Danh sách các kiểm tra đã thực hiện (mỗi phần tử là một dict)."""
        self.checks = []

    def add(self, scope, name, expected, actual, ok=None):
        """Ghi một kiểm tra và in một dòng PASS/FAIL.

        ok mặc định = (expected == actual); truyền ok để dùng điều kiện khác (ví dụ >=). Trả về: bool.
        """
        ok = (expected == actual) if ok is None else ok
        self.checks.append({'scope': scope, 'check': name, 'expected': str(expected),
                            'actual': str(actual), 'status': 'PASS' if ok else 'FAIL'})
        print(f'[{"PASS" if ok else "FAIL"}] {scope:<34} {name:<38} expected={expected} actual={actual}')
        return ok


def session_runs(csv_path, session):
    """Các dòng benchmark.csv thuộc session (cả warm-up); không có thì dừng."""
    with csv_path.open(encoding='utf-8', newline='') as stream:
        rows = [r for r in csv.DictReader(stream) if r.get('session') == session]
    if not rows:
        raise SystemExit(f'No rows for session {session!r} in {csv_path}')
    return rows


def output_dir(row, metrics):
    """Thư mục Parquet output nằm trong worktree đã chạy commit đó: .bench/<commit[:12]>/<run_dir>."""
    return ROOT / '.bench' / row['git_commit'][:12] / metrics['run_dir'] / 'daily_type_summary'


def canonical(rows):
    """Chuỗi xác định của các dòng output đã sort (Decimal giữ chính xác), dùng để tính checksum."""
    def fmt(v):
        return format(v, 'f') if isinstance(v, Decimal) else repr(v)
    return '\n'.join('|'.join(fmt(v) for v in r) for r in rows)


def main():
    """Chạy toàn bộ kiểm tra cho session, ghi correctness.json. Trả về exit code 0 (PASS) / 1 (FAIL)."""
    parser = argparse.ArgumentParser()
    parser.add_argument('--session', default='exp1')
    parser.add_argument('--csv', type=Path, default=ROOT / 'benchmark.csv')
    parser.add_argument('--config', type=Path, default=ROOT / 'config' / 'config.json')
    parser.add_argument('--out', type=Path, default=None)
    parser.add_argument('--skip-tests', action='store_true')
    args = parser.parse_args()
    out = args.out or ROOT / 'evidence' / 'checkpoint4' / args.session / 'correctness.json'
    cfg = json.loads(args.config.read_text(encoding='utf-8-sig'))
    runs = session_runs(args.csv, args.session)
    rep = Report()
    labels = sorted({r['label'] for r in runs})
    ref_row = next(r for r in runs if r['label'] == 'baseline' and r['warm_or_cold'] != 'warmup')
    for label in labels:
        n_warm = sum(1 for r in runs if r['label'] == label and r['warm_or_cold'] == 'warmup')
        n_meas = sum(1 for r in runs if r['label'] == label and r['warm_or_cold'] != 'warmup')
        rep.add(f'session/{label}', 'measured runs >= 5', '>=5', n_meas, n_meas >= 5)
        rep.add(f'session/{label}', 'warm-up runs', 1, n_warm)
        rep.add(f'session/{label}', 'single commit', 1, len({r['git_commit'] for r in runs if r['label'] == label}))
    metrics = {r['run_id']: json.loads((ROOT / r['evidence_dir'] / 'metrics.json').read_text(encoding='utf-8'))
               for r in runs}
    exp = cfg['expected_rows']
    for r in runs:
        m = metrics[r['run_id']]
        scope = f'{r["label"]}#{r["run_no"]} {r["run_id"][-8:]}'
        p, rc, j, t = m['input_profile'], m['row_counts'], m['join'], m['output']['totals']
        rep.add(scope, 'status', 'SUCCESS', m['status'])
        rep.add(scope, 'rows input', exp, p['rows'])
        rep.add(scope, 'rows valid', exp, rc['valid'])
        rep.add(scope, 'rows after join', exp, j['after_rows'])
        rep.add(scope, 'unmatched', 0, j['unmatched_rows'])
        rep.add(scope, 'invalid', 0, rc['invalid'])
        rep.add(scope, 'null (sum of 11 columns)', 0, sum(v for k, v in p.items() if k.startswith('null_')))
        rep.add(scope, 'duplicate_groups', 0, p['duplicate_groups'])
        rep.add(scope, 'duplicate_extra_rows', 0, p['duplicate_extra_rows'])
        rep.add(scope, 'amount = 0 rows', EXPECTED_ZERO_AMOUNT, p['zero_amount_rows'])
        rep.add(scope, 'fraud (output)', cfg['expected_fraud_rows'], t['frauds'])
        rep.add(scope, 'total amount (output)', cfg['expected_total_amount'], str(t['amount']))
        rep.add(scope, 'transactions (output)', exp, t['transactions'])
        rep.add(scope, 'groups (output)', cfg['expected_groups'], t['groups'])
        rep.add(scope, 'input sha256 = reference', metrics[ref_row['run_id']]['input_sha256'], m['input_sha256'])
        rep.add(scope, 'dimension sha256 = reference', metrics[ref_row['run_id']]['dimension_sha256'],
                m['dimension_sha256'])
    spark = spark_session()
    from pyspark.sql import functions as F
    digests = {}
    try:
        ref_df = spark.read.parquet(output_dir(ref_row, metrics[ref_row['run_id']]).as_posix())
        ref_schema_txt = (ROOT / ref_row['evidence_dir'] / 'schema_output.txt').read_text(encoding='utf-8')
        ref_rows = ref_df.orderBy('day_index', 'type').collect()
        for r in runs:
            scope = f'{r["label"]}#{r["run_no"]} {r["run_id"][-8:]}'
            df = spark.read.parquet(output_dir(r, metrics[r['run_id']]).as_posix())
            schema_txt = (ROOT / r['evidence_dir'] / 'schema_output.txt').read_text(encoding='utf-8')
            rep.add(scope, 'output columns', EXPECTED_SCHEMA_COLS, len(df.columns))
            rep.add(scope, 'schema = reference', ref_df.schema.simpleString(), df.schema.simpleString())
            rep.add(scope, 'schema_output.txt = reference', 'same', 'same' if schema_txt == ref_schema_txt
                    else 'different')
            rep.add(scope, 'key (day_index,type) unique', 0,
                    df.groupBy('day_index', 'type').count().filter(F.col('count') != 1).count())
            rows = df.select(*ref_df.columns).orderBy('day_index', 'type').collect()
            mismatched = sum(1 for a, b in zip(ref_rows, rows) if tuple(a) != tuple(b))
            mismatched += abs(len(ref_rows) - len(rows))
            rep.add(scope, 'values = reference (sorted, cell by cell)', 0, mismatched)
            digest = hashlib.sha256(canonical(rows).encode('utf-8')).hexdigest()
            digests[r['run_id']] = digest
            rep.add(scope, 'sha256(sorted rows) = reference', hashlib.sha256(
                canonical(ref_rows).encode('utf-8')).hexdigest()[:16], digest[:16])
            only_ref = ref_df.exceptAll(df.select(*ref_df.columns)).count()
            only_run = df.select(*ref_df.columns).exceptAll(ref_df).count()
            rep.add(scope, 'exceptAll both ways', '0/0', f'{only_ref}/{only_run}')
    finally:
        spark.stop()
    tests = None
    if not args.skip_tests:
        proc = subprocess.run([sys.executable, str(ROOT / 'tests' / 'test_dup_check.py')], cwd=ROOT,
                              capture_output=True, text=True, encoding='utf-8', errors='replace',
                              env=dict(os.environ, PYTHONIOENCODING='utf-8'))
        lines = [ln for ln in proc.stdout.splitlines() if ln.startswith(('[PASS]', '[FAIL]', 'RESULT'))]
        tests = {'exit_code': proc.returncode, 'lines': lines}
        rep.add('tests/test_dup_check.py', 'exit code', 0, proc.returncode)
    failed = [c for c in rep.checks if c['status'] == 'FAIL']
    result = {'session': args.session, 'reference_run': ref_row['run_id'],
              'reference_commit': ref_row['git_commit'],
              'commits': {label: sorted({r['git_commit'] for r in runs if r['label'] == label}) for label in labels},
              'runs': [{'label': r['label'], 'run_no': r['run_no'], 'warm_or_cold': r['warm_or_cold'],
                        'run_id': r['run_id'], 'output_sha256_sorted': digests.get(r['run_id'])} for r in runs],
              'tests_dup_check': tests, 'checks_total': len(rep.checks), 'checks_failed': len(failed),
              'overall': 'PASS' if not failed else 'FAIL', 'checks': rep.checks}
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding='utf-8')
    print(f'\nOVERALL: {result["overall"]} ({len(rep.checks) - len(failed)}/{len(rep.checks)} checks) -> '
          f'{out.relative_to(ROOT).as_posix()}')
    return 0 if not failed else 1


if __name__ == '__main__':
    sys.exit(main())
