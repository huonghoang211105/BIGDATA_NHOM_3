"""Per-stage / per-job / per-phase metrics from a Spark event log (CP4 benchmark evidence).

Usage:
    python src/stage_metrics.py --event-log logs/spark-events/local-123 --out-dir evidence/checkpoint4/baseline/<run_id>

Writes into --out-dir:
    stage_metrics.csv   one row per executed stage attempt (skipped stages are not executed)
    job_metrics.csv     one row per Spark job (each count()/collect()/first()/write is >= 1 job)
    phase_metrics.csv   stages/jobs grouped by phase (job description 'phase | action' set by pipeline.py)
    sql_final_plans.txt final AQE physical plans (isFinalPlan=true) for the key SQL executions

Units: *_mb = MiB (bytes / 2**20); *_sec = seconds. Task time = Finish - Launch (wall, as Spark UI).
Only the standard library is used, so it runs without Spark.
"""
import argparse
import csv
import json
from pathlib import Path
from statistics import median

MIB = 1024 * 1024
FACT_SCAN_MIN_RECORDS = 1_000_000  # a stage reading >= 1M input records = full scan of the PaySim CSV
SQL = 'org.apache.spark.sql.execution.ui.'
# Executions whose final plan is kept (prefix match on the job description).
PLAN_PREFIXES = ('profile_null', 'profile_dup', 'join | joined.count', 'output | summary')


def phase_of(description):
    return (description or '').split(' | ')[0].strip() or '(none)'


def load_events(path):
    with Path(path).open(encoding='utf-8') as stream:
        for line in stream:
            line = line.strip()
            if line:
                yield json.loads(line)


def parse(path):
    stages, jobs, tasks, sql = {}, {}, {}, {}
    for ev in load_events(path):
        kind = ev.get('Event')
        if kind == 'SparkListenerJobStart':
            props = ev.get('Properties') or {}
            jobs[ev['Job ID']] = {
                'job_id': ev['Job ID'], 'description': props.get('spark.job.description', ''),
                'callsite': props.get('callSite.short', ''),
                'sql_execution_id': props.get('spark.sql.execution.id', ''),
                'stage_ids': ev.get('Stage IDs', []), 'submit_ms': ev.get('Submission Time'),
                'end_ms': None, 'result': None}
        elif kind == 'SparkListenerJobEnd':
            job = jobs.get(ev['Job ID'])
            if job:
                job['end_ms'] = ev.get('Completion Time')
                job['result'] = (ev.get('Job Result') or {}).get('Result')
        elif kind == 'SparkListenerStageSubmitted':
            info, props = ev['Stage Info'], ev.get('Properties') or {}
            key = (info['Stage ID'], info.get('Stage Attempt ID', 0))
            stages.setdefault(key, {})['description'] = props.get('spark.job.description', '')
        elif kind == 'SparkListenerStageCompleted':
            info = ev['Stage Info']
            key = (info['Stage ID'], info.get('Stage Attempt ID', 0))
            stages.setdefault(key, {}).update(
                name=info.get('Stage Name', ''), num_tasks=info.get('Number of Tasks'),
                submit_ms=info.get('Submission Time'), end_ms=info.get('Completion Time'),
                failure=info.get('Failure Reason'))
        elif kind == 'SparkListenerTaskEnd':
            key = (ev['Stage ID'], ev.get('Stage Attempt ID', 0))
            tasks.setdefault(key, []).append(ev)
        elif kind == SQL + 'SparkListenerSQLExecutionStart':
            sql[ev['executionId']] = {'description': ev.get('description', ''),
                                      'plan': ev.get('physicalPlanDescription', ''),
                                      'start_ms': ev.get('time'), 'end_ms': None}
        elif kind == SQL + 'SparkListenerSQLAdaptiveExecutionUpdate':
            if ev['executionId'] in sql:
                sql[ev['executionId']]['plan'] = ev.get('physicalPlanDescription', '')
        elif kind == SQL + 'SparkListenerSQLExecutionEnd':
            if ev['executionId'] in sql:
                sql[ev['executionId']]['end_ms'] = ev.get('time')
    return stages, jobs, tasks, sql


def task_numbers(ev):
    info, m = ev.get('Task Info') or {}, ev.get('Task Metrics') or {}
    sr, sw, inp = (m.get('Shuffle Read Metrics') or {}, m.get('Shuffle Write Metrics') or {},
                   m.get('Input Metrics') or {})
    return {
        'dur_ms': (info.get('Finish Time') or 0) - (info.get('Launch Time') or 0),
        'run_ms': m.get('Executor Run Time', 0), 'cpu_ns': m.get('Executor CPU Time', 0),
        'gc_ms': m.get('JVM GC Time', 0), 'peak_mem': m.get('Peak Execution Memory', 0),
        'in_bytes': inp.get('Bytes Read', 0), 'in_records': inp.get('Records Read', 0),
        'sr_bytes': sr.get('Remote Bytes Read', 0) + sr.get('Local Bytes Read', 0),
        'sr_records': sr.get('Total Records Read', 0),
        'sw_bytes': sw.get('Shuffle Bytes Written', 0), 'sw_records': sw.get('Shuffle Records Written', 0),
        'spill_mem': m.get('Memory Bytes Spilled', 0), 'spill_disk': m.get('Disk Bytes Spilled', 0),
        'failed': bool(info.get('Failed')) or bool(info.get('Killed'))}


def r(value, digits=3):
    return round(value, digits)


def build(event_log):
    stages, jobs, tasks, sql = parse(event_log)
    stage_jobs = {}
    for job in jobs.values():
        for sid in job['stage_ids']:
            stage_jobs.setdefault(sid, []).append(job['job_id'])
    stage_rows = []
    for (sid, attempt), st in sorted(stages.items(), key=lambda kv: (kv[1].get('submit_ms') or 0, kv[0])):
        if st.get('submit_ms') is None:
            continue  # never executed
        nums = [task_numbers(t) for t in tasks.get((sid, attempt), [])]
        durs = [x['dur_ms'] for x in nums] or [0]
        total = lambda k: sum(x[k] for x in nums)  # noqa: E731
        stage_rows.append({
            'stage_id': sid, 'attempt': attempt,
            'job_ids': ' '.join(str(j) for j in sorted(stage_jobs.get(sid, []))),
            'phase': phase_of(st.get('description')), 'description': st.get('description', ''),
            'callsite': st.get('name', ''),
            'duration_sec': r(((st.get('end_ms') or 0) - st['submit_ms']) / 1000),
            'num_tasks': st.get('num_tasks'), 'tasks_finished': len(nums),
            'failed_tasks': sum(x['failed'] for x in nums),
            'task_median_sec': r(median(durs) / 1000), 'task_max_sec': r(max(durs) / 1000),
            'executor_run_sum_sec': r(total('run_ms') / 1000),
            'executor_cpu_sum_sec': r(total('cpu_ns') / 1e9),
            'gc_sum_sec': r(total('gc_ms') / 1000),
            'input_mb': r(total('in_bytes') / MIB), 'input_records': total('in_records'),
            'shuffle_read_mb': r(total('sr_bytes') / MIB), 'shuffle_read_records': total('sr_records'),
            'shuffle_write_mb': r(total('sw_bytes') / MIB), 'shuffle_write_records': total('sw_records'),
            'spill_memory_mb': r(total('spill_mem') / MIB), 'spill_disk_mb': r(total('spill_disk') / MIB),
            'peak_exec_mem_max_mb': r(max([x['peak_mem'] for x in nums] or [0]) / MIB),
            'fact_scan': int(total('in_records') >= FACT_SCAN_MIN_RECORDS),
            '_durs': durs})
    executed = {row['stage_id'] for row in stage_rows}
    job_rows = []
    for job in sorted(jobs.values(), key=lambda j: j['job_id']):
        mine = [s for s in stage_rows if str(job['job_id']) in s['job_ids'].split()]
        job_rows.append({
            'job_id': job['job_id'], 'phase': phase_of(job['description']),
            'description': job['description'], 'callsite': job['callsite'],
            'sql_execution_id': job['sql_execution_id'], 'result': job['result'],
            'duration_sec': r(((job['end_ms'] or job['submit_ms'] or 0) - (job['submit_ms'] or 0)) / 1000),
            'stages_executed': ' '.join(str(s) for s in job['stage_ids'] if s in executed),
            'stages_skipped': sum(1 for s in job['stage_ids'] if s not in executed),
            'fact_scans': sum(s['fact_scan'] for s in mine)})
    phase_rows = {}
    for job in job_rows:
        p = phase_rows.setdefault(job['phase'], {'phase': job['phase'], 'jobs': 0, 'job_wall_sum_sec': 0.0})
        p['jobs'] += 1
        p['job_wall_sum_sec'] += job['duration_sec']
    for st in stage_rows:
        p = phase_rows.setdefault(st['phase'], {'phase': st['phase'], 'jobs': 0, 'job_wall_sum_sec': 0.0})
        p['stages'] = p.get('stages', 0) + 1
        p['tasks'] = p.get('tasks', 0) + st['tasks_finished']
        p['fact_scans'] = p.get('fact_scans', 0) + st['fact_scan']
        for k in ('input_mb', 'shuffle_read_mb', 'shuffle_write_mb', 'spill_memory_mb', 'spill_disk_mb',
                  'executor_run_sum_sec'):
            p[k] = p.get(k, 0.0) + st[k]
        p['task_max_sec'] = max(p.get('task_max_sec', 0.0), st['task_max_sec'])
    phases = []
    for p in phase_rows.values():
        phases.append({k: (r(v) if isinstance(v, float) else v) for k, v in p.items()})
    all_durs = [d for s in stage_rows for d in s['_durs'] if s['tasks_finished']]
    summary = {
        'stages_executed': len(stage_rows), 'jobs': len(job_rows),
        'tasks': sum(s['tasks_finished'] for s in stage_rows),
        'fact_scans': sum(s['fact_scan'] for s in stage_rows),
        'max_task_sec': r(max(all_durs or [0]) / 1000), 'median_task_sec': r(median(all_durs or [0]) / 1000),
        'input_mb': r(sum(s['input_mb'] for s in stage_rows)),
        'shuffle_read_mb': r(sum(s['shuffle_read_mb'] for s in stage_rows)),
        'shuffle_write_mb': r(sum(s['shuffle_write_mb'] for s in stage_rows)),
        'spill_memory_mb': r(sum(s['spill_memory_mb'] for s in stage_rows)),
        'spill_disk_mb': r(sum(s['spill_disk_mb'] for s in stage_rows)),
        'phases': {p['phase']: p for p in phases}}
    for s in stage_rows:
        del s['_durs']
    return stage_rows, job_rows, phases, sql, summary


def write_csv(path, rows):
    if not rows:
        path.write_text('', encoding='utf-8')
        return
    fields = []
    for row in rows:
        fields += [k for k in row if k not in fields]
    with path.open('w', encoding='utf-8', newline='') as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def write_outputs(event_log, out_dir):
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    stage_rows, job_rows, phases, sql, summary = build(event_log)
    write_csv(out_dir / 'stage_metrics.csv', stage_rows)
    write_csv(out_dir / 'job_metrics.csv', job_rows)
    write_csv(out_dir / 'phase_metrics.csv', phases)
    with (out_dir / 'sql_final_plans.txt').open('w', encoding='utf-8') as stream:
        for eid, ex in sorted(sql.items()):
            if ex['description'].startswith(PLAN_PREFIXES):
                dur = ((ex['end_ms'] or ex['start_ms'] or 0) - (ex['start_ms'] or 0)) / 1000
                stream.write(f'===== SQL execution {eid} | {ex["description"]} | {dur:.3f}s =====\n')
                stream.write(ex['plan'].rstrip() + '\n\n')
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--event-log', required=True, type=Path)
    parser.add_argument('--out-dir', required=True, type=Path)
    args = parser.parse_args()
    summary = write_outputs(args.event_log, args.out_dir)
    phases = summary.pop('phases')
    print(json.dumps(summary, indent=2))
    print(f'{"phase":<14}{"jobs":>5}{"wall_s":>9}{"stages":>7}{"scans":>6}{"in_MB":>9}'
          f'{"shR_MB":>9}{"shW_MB":>9}{"spillM":>9}{"spillD":>9}{"maxTask":>8}')
    for p in phases.values():
        print(f'{p["phase"]:<14}{p["jobs"]:>5}{p["job_wall_sum_sec"]:>9.2f}{p.get("stages", 0):>7}'
              f'{p.get("fact_scans", 0):>6}{p.get("input_mb", 0):>9.1f}{p.get("shuffle_read_mb", 0):>9.1f}'
              f'{p.get("shuffle_write_mb", 0):>9.1f}{p.get("spill_memory_mb", 0):>9.1f}'
              f'{p.get("spill_disk_mb", 0):>9.1f}{p.get("task_max_sec", 0):>8.2f}')


if __name__ == '__main__':
    main()
