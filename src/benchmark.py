"""CP4 benchmark runner: run src/pipeline.py N times per label and append one row per run to benchmark.csv.

Each label runs the code of a git ref (default tag checkpoint-4-<label>) from a detached git worktree
under .bench/<commit>, so the baseline is always exactly the tagged code even while the main checkout
changes. The raw CSV is hard-linked into the worktree (same physical file, same OS cache, no copy).

    # Phase 1: baseline only (1 warm-up run discarded + 5 measured runs)
    python src/benchmark.py --label baseline --runs 5 --warmup 1
    # Phase 2: interleaved B, A, B, A, ... (labels run in the given order inside every round)
    python src/benchmark.py --label baseline optimized --runs 5 --warmup 1
    # Median / min / max per label from benchmark.csv (warm-up rows excluded)
    python src/benchmark.py --summary

Per run: evidence/checkpoint4/<label>/<run_id>/ gets the pipeline evidence (run.log, metrics.json,
explain_*.txt, ...) plus stage_metrics.csv, job_metrics.csv, phase_metrics.csv, sql_final_plans.txt
(from the Spark event log, see src/stage_metrics.py), env.json and console.txt.
"""
import argparse
import csv
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import shutil
import statistics
import subprocess
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'src'))
import stage_metrics  # noqa: E402

EVENT_DIR = ROOT / 'logs' / 'spark-events'
BENCH_DIR = ROOT / '.bench'
COLUMNS = [
    # Required columns (brief, in this order)
    'label', 'git_commit', 'run_id', 'run_no', 'warm_or_cold', 'runtime_sec', 'sec_hash', 'sec_profile',
    'sec_clean', 'sec_join', 'sec_output', 'max_task_sec', 'median_task_sec', 'shuffle_read_mb',
    'shuffle_write_mb', 'spill_mb', 'rows_in', 'rows_out', 'fraud_count', 'total_amount', 'spark_version',
    'master', 'driver_memory', 'shuffle_partitions', 'aqe_enabled',
    # Extra detail: finer timing split, target-stage metrics, correctness, machine state
    'ref', 'round', 'started_at_utc', 'process_wall_sec', 'sec_imports', 'sec_spark_startup',
    'sec_read_schema', 'sec_profile_null', 'sec_profile_dup', 'spill_memory_mb', 'fact_scans', 'jobs',
    'stages', 'tasks', 'dup_job_wall_sec', 'dup_shuffle_write_mb', 'dup_spill_memory_mb',
    'dup_spill_disk_mb', 'dup_task_max_sec', 'clean_job_wall_sec', 'clean_fact_scans',
    'rowbatch_spill_warns', 'rows_valid', 'rows_joined', 'unmatched', 'null_total', 'dup_extra_rows',
    'zero_amount_rows', 'checks', 'free_ram_mb_start', 'jvm_max_heap_mb', 'evidence_dir']
SUMMARY_COLS = ['runtime_sec', 'sec_hash', 'sec_spark_startup', 'sec_read_schema', 'sec_profile_null',
                'sec_profile_dup', 'sec_profile', 'sec_clean', 'sec_join', 'sec_output', 'max_task_sec',
                'shuffle_read_mb', 'shuffle_write_mb', 'spill_mb', 'spill_memory_mb',
                'dup_shuffle_write_mb', 'dup_spill_disk_mb', 'dup_task_max_sec']


def git(*args, cwd=ROOT):
    return subprocess.run(['git', *args], cwd=cwd, check=True, capture_output=True,
                          text=True).stdout.strip()


def free_ram_mb():
    try:
        import ctypes

        class MEMORYSTATUSEX(ctypes.Structure):
            _fields_ = [('dwLength', ctypes.c_ulong), ('dwMemoryLoad', ctypes.c_ulong),
                        ('ullTotalPhys', ctypes.c_ulonglong), ('ullAvailPhys', ctypes.c_ulonglong),
                        ('ullTotalPageFile', ctypes.c_ulonglong), ('ullAvailPageFile', ctypes.c_ulonglong),
                        ('ullTotalVirtual', ctypes.c_ulonglong), ('ullAvailVirtual', ctypes.c_ulonglong),
                        ('ullAvailExtendedVirtual', ctypes.c_ulonglong)]
        status = MEMORYSTATUSEX()
        status.dwLength = ctypes.sizeof(MEMORYSTATUSEX)
        ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status))
        return round(status.ullAvailPhys / 2**20)
    except (AttributeError, OSError):
        return ''


def prepare_worktree(ref):
    """Detached worktree at ref; raw input hard-linked from the main checkout. Returns (dir, commit)."""
    commit = git('rev-parse', f'{ref}^{{commit}}')
    wt = BENCH_DIR / commit[:12]
    if not wt.exists():
        git('worktree', 'add', '--detach', str(wt), commit)
    if git('rev-parse', 'HEAD', cwd=wt) != commit:
        raise SystemExit(f'Worktree {wt} is not at {commit}')
    dirty = git('status', '--porcelain', cwd=wt)
    if dirty:
        raise SystemExit(f'Worktree {wt} has local changes:\n{dirty}')
    manifest = json.loads((wt / 'metadata' / 'manifest.json').read_text(encoding='utf-8'))
    src, dst = ROOT / manifest['destination'], wt / manifest['destination']
    if not dst.exists():
        dst.parent.mkdir(parents=True, exist_ok=True)
        os.link(src, dst)  # same NTFS volume: one physical file, no 493 MB copy
    if not os.path.samefile(src, dst):
        raise SystemExit(f'{dst} is not the same file as {src}')
    return wt, commit


def run_once(label, ref, wt, commit, round_no, run_no, warm, evidence_root, csv_path):
    started = datetime.now(timezone.utc).isoformat()
    ram = free_ram_mb()
    cmd = [sys.executable, 'src/pipeline.py', '--config', 'config/config.json',
           '--evidence-dir', f'{evidence_root}/{label}', '--event-log-dir', str(EVENT_DIR)]
    env = dict(os.environ, PYTHONIOENCODING='utf-8')
    print(f'\n=== {label} round={round_no} run_no={run_no} ({warm}) ref={ref} commit={commit[:8]} '
          f'free_ram_mb={ram} ===', flush=True)
    t0 = time.perf_counter()
    proc = subprocess.run(cmd, cwd=wt, env=env, capture_output=True, text=True,
                          encoding='utf-8', errors='replace')
    wall = round(time.perf_counter() - t0, 3)
    console = proc.stdout + proc.stderr
    found = re.search(r'\[EVIDENCE\] copied \d+ files to (\S+?):', console)
    if proc.returncode != 0 or not found:
        fail_log = ROOT / 'logs' / f'benchmark_failed_{label}_{datetime.now():%Y%m%dT%H%M%S}.txt'
        fail_log.write_text(console, encoding='utf-8')
        raise SystemExit(f'Pipeline failed (exit {proc.returncode}); console saved to {fail_log}')
    produced = wt / found.group(1)
    target = ROOT / found.group(1)
    if produced.resolve() != target.resolve():  # move evidence from the worktree into the main repo
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(produced), str(target))
    (target / 'console.txt').write_text(console, encoding='utf-8')
    m = json.loads((target / 'metrics.json').read_text(encoding='utf-8'))
    env_info = m['environment']
    app_id = env_info['spark_app_id']
    event_log = EVENT_DIR / app_id
    if not event_log.exists():
        event_log = EVENT_DIR / f'{app_id}.inprogress'
    s = stage_metrics.write_outputs(event_log, target)
    phases = s['phases']
    t = m['timings_sec']
    prof, rows, join, out = m['input_profile'], m['row_counts'], m['join'], m['output']['totals']
    cfg = json.loads((wt / 'config' / 'config.json').read_text(encoding='utf-8-sig'))
    null_total = sum(v for k, v in prof.items() if k.startswith('null_'))
    checks = all([prof['rows'] == cfg['expected_rows'], rows['valid'] == cfg['expected_rows'],
                  join['after_rows'] == cfg['expected_rows'], join['unmatched_rows'] == 0,
                  rows['invalid'] == 0, null_total == 0, prof['duplicate_extra_rows'] == 0,
                  prof['duplicate_groups'] == 0, prof['zero_amount_rows'] == 16,
                  out['frauds'] == cfg['expected_fraud_rows'], out['groups'] == cfg['expected_groups'],
                  out['transactions'] == cfg['expected_rows'],
                  str(out['amount']) == cfg['expected_total_amount']])
    sql_conf = env_info['effective_sql_conf']
    dup, clean = phases.get('profile_dup', {}), phases.get('clean', {})
    row = {
        'label': label, 'git_commit': commit, 'run_id': m['run_id'], 'run_no': run_no,
        'warm_or_cold': warm, 'runtime_sec': m['runtime_sec'], 'sec_hash': t['hash'],
        'sec_profile': round(t['profile_null'] + t['profile_dup'], 3), 'sec_clean': t['clean'],
        'sec_join': t['join'], 'sec_output': t['output'], 'max_task_sec': s['max_task_sec'],
        'median_task_sec': s['median_task_sec'], 'shuffle_read_mb': s['shuffle_read_mb'],
        'shuffle_write_mb': s['shuffle_write_mb'], 'spill_mb': s['spill_disk_mb'],
        'rows_in': prof['rows'], 'rows_out': out['groups'], 'fraud_count': out['frauds'],
        'total_amount': out['amount'], 'spark_version': env_info['spark'], 'master': env_info['master'],
        'driver_memory': env_info['driver_memory'],
        'shuffle_partitions': sql_conf['spark.sql.shuffle.partitions'],
        'aqe_enabled': sql_conf['spark.sql.adaptive.enabled'],
        'ref': ref, 'round': round_no, 'started_at_utc': started, 'process_wall_sec': wall,
        'sec_imports': t['imports'], 'sec_spark_startup': t['spark_startup'],
        'sec_read_schema': t['read_schema'], 'sec_profile_null': t['profile_null'],
        'sec_profile_dup': t['profile_dup'], 'spill_memory_mb': s['spill_memory_mb'],
        'fact_scans': s['fact_scans'], 'jobs': s['jobs'], 'stages': s['stages_executed'],
        'tasks': s['tasks'], 'dup_job_wall_sec': dup.get('job_wall_sum_sec', ''),
        'dup_shuffle_write_mb': dup.get('shuffle_write_mb', ''),
        'dup_spill_memory_mb': dup.get('spill_memory_mb', ''),
        'dup_spill_disk_mb': dup.get('spill_disk_mb', ''), 'dup_task_max_sec': dup.get('task_max_sec', ''),
        'clean_job_wall_sec': clean.get('job_wall_sum_sec', ''), 'clean_fact_scans': clean.get('fact_scans', ''),
        'rowbatch_spill_warns': console.count('RowBasedKeyValueBatch: Calling spill()'),
        'rows_valid': rows['valid'], 'rows_joined': join['after_rows'], 'unmatched': join['unmatched_rows'],
        'null_total': null_total, 'dup_extra_rows': prof['duplicate_extra_rows'],
        'zero_amount_rows': prof['zero_amount_rows'], 'checks': 'PASS' if checks else 'FAIL',
        'free_ram_mb_start': ram, 'jvm_max_heap_mb': round(env_info['jvm_max_heap_bytes'] / 2**20),
        'evidence_dir': target.relative_to(ROOT).as_posix() if target.is_relative_to(ROOT) else str(target)}
    (target / 'env.json').write_text(json.dumps({
        'benchmark': {k: row[k] for k in ('label', 'ref', 'git_commit', 'round', 'run_no', 'warm_or_cold',
                                          'started_at_utc', 'process_wall_sec', 'free_ram_mb_start')},
        'spark_version': env_info['spark'], 'master': env_info['master'],
        'logical_cpus': env_info['logical_cpus'], 'default_parallelism': env_info.get('default_parallelism'),
        'total_ram_bytes': env_info['total_ram_bytes'], 'driver_memory': env_info['driver_memory'],
        'jvm_max_heap_bytes': env_info['jvm_max_heap_bytes'], 'os': env_info['os'],
        'python': env_info['python'], 'java_version': env_info['java_version'],
        'effective_sql_conf': sql_conf,
        'explicit_spark_conf': {k: v for k, v in env_info['spark_conf'].items()
                                if not k.startswith(('spark.app.', 'spark.driver.host', 'spark.driver.port',
                                                     'spark.executor.id', 'spark.driver.extraJavaOptions',
                                                     'spark.executor.extraJavaOptions'))},
        'event_log': event_log.relative_to(ROOT).as_posix()}, indent=2, ensure_ascii=False),
        encoding='utf-8')
    new = not csv_path.exists()
    with csv_path.open('a', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=COLUMNS)
        if new:
            writer.writeheader()
        writer.writerow(row)
    print(f'    runtime_sec={row["runtime_sec"]} hash={row["sec_hash"]} startup={row["sec_spark_startup"]} '
          f'profile_null={row["sec_profile_null"]} profile_dup={row["sec_profile_dup"]} '
          f'clean={row["sec_clean"]} join={row["sec_join"]} output={row["sec_output"]} '
          f'checks={row["checks"]} -> {row["evidence_dir"]}', flush=True)
    return row


def summarize(csv_path):
    with csv_path.open(encoding='utf-8') as stream:
        rows = [r for r in csv.DictReader(stream) if r['warm_or_cold'] != 'warmup']
    groups = {}
    for r in rows:
        groups.setdefault((r['label'], r['git_commit'][:8]), []).append(r)
    for (label, commit), items in groups.items():
        print(f'\n## {label} @ {commit}  (n={len(items)}, checks: '
              f'{sorted(set(r["checks"] for r in items))})')
        print('| metric | median | min | max | max-min | stdev | runs |')
        print('|---|---:|---:|---:|---:|---:|---|')
        for col in SUMMARY_COLS:
            vals = [float(r[col]) for r in items if r.get(col) not in (None, '')]
            if not vals:
                continue
            sd = statistics.stdev(vals) if len(vals) > 1 else 0.0
            print(f'| {col} | {statistics.median(vals):.3f} | {min(vals):.3f} | {max(vals):.3f} | '
                  f'{max(vals) - min(vals):.3f} | {sd:.3f} | {", ".join(f"{v:g}" for v in vals)} |')


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--label', nargs='+', default=['baseline'],
                        help='One or more labels; several labels are interleaved within each round.')
    parser.add_argument('--ref', action='append', default=[],
                        help='label=gitref override (default ref for label X is tag checkpoint-4-X).')
    parser.add_argument('--runs', type=int, default=5, help='Measured runs per label (>= 3).')
    parser.add_argument('--warmup', type=int, default=1, help='Warm-up rounds per label, recorded but excluded.')
    parser.add_argument('--pause', type=float, default=5.0, help='Seconds between runs (same for all labels).')
    parser.add_argument('--evidence-root', default='evidence/checkpoint4')
    parser.add_argument('--csv', type=Path, default=ROOT / 'benchmark.csv')
    parser.add_argument('--summary', action='store_true', help='Only print the summary of --csv.')
    args = parser.parse_args()
    if args.summary:
        summarize(args.csv)
        return
    refs = dict(item.split('=', 1) for item in args.ref)
    EVENT_DIR.mkdir(parents=True, exist_ok=True)
    arms = []
    for label in args.label:
        ref = refs.get(label, f'checkpoint-4-{label}')
        wt, commit = prepare_worktree(ref)
        arms.append((label, ref, wt, commit))
        print(f'{label}: ref={ref} commit={commit} worktree={wt.relative_to(ROOT).as_posix()}')
    counters = {label: 0 for label in args.label}
    for round_no in range(1, args.warmup + args.runs + 1):
        warm = 'warmup' if round_no <= args.warmup else 'warm'
        for label, ref, wt, commit in arms:
            run_no = 0
            if warm == 'warm':
                counters[label] += 1
                run_no = counters[label]
            run_once(label, ref, wt, commit, round_no, run_no, warm, args.evidence_root, args.csv)
            time.sleep(args.pause)
    summarize(args.csv)


if __name__ == '__main__':
    main()
