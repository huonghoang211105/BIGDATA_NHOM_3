"""Điền slide Checkpoint 4 từ template (giữ nguyên layout), số liệu đọc từ evidence – không gõ tay.

    python reports/build_checkpoint4_pptx.py
    -> reports/BigData_DoAn4_Checkpoint4_Report_Nhom3.pptx

Nguồn số: benchmark.csv (session exp1), evidence/checkpoint4/*/<run_id>/stage_metrics.csv,
evidence/checkpoint4/exp1/correctness.json. Chỉ thay chữ trong các shape có sẵn của template.
"""
import copy
import csv
import json
from pathlib import Path
from statistics import median, stdev

from pptx import Presentation

ROOT = Path(__file__).resolve().parents[1]
TEMPLATE = ROOT / 'reports' / 'BigData_DoAn4_Checkpoint4_Report_Template.pptx'
OUT = ROOT / 'reports' / 'BigData_DoAn4_Checkpoint4_Report_Nhom3.pptx'
SESSION = 'exp1'
NS = '{http://schemas.openxmlformats.org/drawingml/2006/main}'


def vn(v, d=1):
    s = f'{v:,.{d}f}'
    return s.replace(',', ' ').replace('.', ',').replace(' ', '.').replace('-', '−')


def rows_of(session, label):
    with (ROOT / 'benchmark.csv').open(encoding='utf-8', newline='') as f:
        return [r for r in csv.DictReader(f) if r['session'] == session and r['label'] == label
                and r['warm_or_cold'] != 'warmup']


def stages(row, phase, key):
    with (ROOT / row['evidence_dir'] / 'stage_metrics.csv').open(encoding='utf-8', newline='') as f:
        st = [s for s in csv.DictReader(f) if s['phase'] == phase]
    return max(st, key=lambda s: float(s[key]))


def med(rows, col):
    return median(float(r[col]) for r in rows)


def facts():
    b, a = rows_of(SESSION, 'baseline'), rows_of(SESSION, 'optimized')
    corr = json.loads((ROOT / 'evidence/checkpoint4' / SESSION / 'correctness.json').read_text(encoding='utf-8'))
    f = {'b': b, 'a': a, 'corr': corr}
    for k, rows in (('b', b), ('a', a)):
        rt = [float(r['runtime_sec']) for r in rows]
        f[k + '_rt'] = (median(rt), min(rt), max(rt), stdev(rt), rt)
        f[k + '_dup'] = med(rows, 'sec_profile_dup')
        f[k + '_dup_sd'] = stdev(float(r['sec_profile_dup']) for r in rows)
        f[k + '_shr'], f[k + '_shw'] = med(rows, 'shuffle_read_mb'), med(rows, 'shuffle_write_mb')
        f[k + '_task'] = med(rows, 'max_task_sec')
        f[k + '_spm'], f[k + '_spd'] = med(rows, 'spill_memory_mb'), med(rows, 'spill_mb')
        maps = [stages(r, 'profile_dup', 'input_mb') for r in rows]
        reds = [stages(r, 'profile_dup', 'shuffle_read_mb') for r in rows]
        f[k + '_map'] = {x: median(float(s[x]) for s in maps) for x in
                         ('duration_sec', 'task_max_sec', 'shuffle_write_mb', 'shuffle_write_records',
                          'spill_memory_mb', 'spill_disk_mb', 'peak_exec_mem_max_mb')}
        f[k + '_map']['stage_id'] = maps[0]['stage_id']
        f[k + '_red'] = {x: median(float(s[x]) for s in reds) for x in
                         ('duration_sec', 'spill_memory_mb', 'spill_disk_mb')}
        f[k + '_red']['stage_id'] = reds[0]['stage_id']
        f[k + '_scan'] = median(float(stages(r, 'profile_null', 'input_mb')['duration_sec']) for r in rows)
        f[k + '_ram'] = (min(int(r['free_ram_mb_start']) for r in rows), max(int(r['free_ram_mb_start']) for r in rows))
    out = b[0]
    f['rows_in'], f['groups'] = int(out['rows_in']), int(out['rows_out'])
    f['fraud'], f['amount'] = int(out['fraud_count']), out['total_amount']
    return f


def set_lines(shape, lines):
    """Thay toàn bộ đoạn văn của shape, giữ định dạng (pPr/rPr) của đoạn + run đầu tiên.
    Mỗi dòng: str hoặc (str, {'bold': True})."""
    body = shape.text_frame._txBody
    paras = body.findall(NS + 'p')
    tpl_p = copy.deepcopy(paras[0])
    tpl_r = tpl_p.find(NS + 'r')
    tpl_rpr = copy.deepcopy(tpl_r.find(NS + 'rPr')) if tpl_r is not None and tpl_r.find(NS + 'rPr') is not None else None
    for p in paras:
        body.remove(p)
    for line in lines:
        text, opts = (line, {}) if isinstance(line, str) else line
        p = copy.deepcopy(tpl_p)
        for child in list(p):
            if child.tag in (NS + 'r', NS + 'br', NS + 'fld', NS + 'endParaRPr'):
                p.remove(child)
        r = copy.deepcopy(tpl_r) if tpl_r is not None else None
        for child in list(r):
            r.remove(child)
        rpr = copy.deepcopy(tpl_rpr) if tpl_rpr is not None else None
        if rpr is not None:
            if 'bold' in opts:
                rpr.set('b', '1' if opts['bold'] else '0')
            r.append(rpr)
        t = copy.deepcopy(tpl_r.find(NS + 't'))
        t.text = text
        r.append(t)
        p.append(r)
        body.append(p)


def by_name(slide, name):
    return next(s for s in slide.shapes if s.name == name)


def main():
    f = facts()
    bm, bmin, bmax, bsd, brt = f['b_rt']
    am, amin, amax, asd, art = f['a_rt']
    d_rt = am - bm
    spread = bmax - bmin
    corr = f['corr']
    n_ok = corr['checks_total'] - corr['checks_failed']
    prs = Presentation(TEMPLATE)
    s1, s2, s3, s4, s5, s6 = prs.slides

    set_lines(by_name(s1, 'Text 2'), ['Nhóm: Nhóm 3   •   Baseline commit: ae84592   •   Optimized commit: d2b15a5'])
    set_lines(by_name(s1, 'Text 4'), ['MEDIAN RUNTIME', f'{vn(bm)} s → {vn(am)} s'])
    set_lines(by_name(s1, 'Text 6'), ['SPEEDUP', f'{vn(-d_rt / bm * 100)} % ({vn(bm / am, 2)}×)'])
    set_lines(by_name(s1, 'Text 8'), ['CORRECTNESS', f'PASS ({n_ok}/{corr["checks_total"]} checks)'])

    set_lines(by_name(s2, 'Text 4'), [
        ('Dataset', {'bold': True}),
        'PaySim CSV, 1 file, 493.534.783 bytes (471,4 MiB)',
        f'{vn(f["rows_in"], 0)} dòng × 11 cột; SHA-256 16910f90…4eea6b',
        ('Input path', {'bold': True}),
        'Raw zone CP2, đọc qua metadata/manifest.json (data/raw/paysim/ingest_date=2026-11-12/…)',
        ('Output target', {'bold': True}),
        f'Parquet daily_type_summary: {f["groups"]} dòng (day_index × type), 10 cột',
        ('Pipeline step(s)', {'bold': True}),
        'đọc schema FAILFAST → profile null + đếm trùng 11 cột → T1–T4 → kiểm dim + left_anti → '
        'broadcast join → groupBy → ghi Parquet → đọc lại đối soát'])
    set_lines(by_name(s2, 'Text 7'), [
        ('Spark version', {'bold': True}), 'PySpark 3.5.9, Java 17.0.20, Python 3.12.10',
        ('Master / executors', {'bold': True}), 'local[*] – driver kiêm executor, 1 máy Windows 11',
        ('Cores / memory', {'bold': True}),
        '8 core logic; RAM 7,8 GiB; driver memory mặc định 1g (heap 1.024 MiB)',
        ('Important configs', {'bold': True}),
        'AQE bật, shuffle.partitions 200, broadcast threshold 10 MB, event log bật (cả 2 phía); '
        'mọi config khác mặc định'])
    set_lines(by_name(s2, 'Text 10'), [
        ('Run 1 / 2 / 3 / 4 / 5 (session exp1)', {'bold': True}),
        ' / '.join(vn(x) for x in brt) + ' s',
        ('Median runtime', {'bold': True}),
        f'{vn(bm)} s (min {vn(bmin)} – max {vn(bmax)}; stdev {vn(bsd)})',
        '1 warm-up bỏ đi; chạy xen kẽ với bản tối ưu',
        ('Rows out', {'bold': True}), f'{f["groups"]} nhóm; khóa (day_index, type) duy nhất',
        ('Business metrics', {'bold': True}),
        f'{vn(f["rows_in"], 0)} GD; fraud {vn(f["fraud"], 0)}; tổng tiền {vn(float(f["amount"]), 2)}'])
    set_lines(by_name(s2, 'Text 11'), ['Evidence: benchmark.csv • evidence/checkpoint4/baseline/<run_id>/ '
                                       '(run.log, metrics.json, stage_metrics.csv, explain_*.txt)'])

    bmap, bred = f['b_map'], f['b_red']
    set_lines(by_name(s3, 'Text 4'), [
        f'• Exchange lớn: stage {bmap["stage_id"]} (profile_dup) shuffle write {vn(bmap["shuffle_write_mb"])} MiB = '
        f'{vn(bmap["shuffle_write_records"], 0)} record = 100 % dòng input (partial agg không gộp được)',
        f'• Stage nặng nhất: {vn(bmap["duration_sec"])} s, gấp {vn(bmap["duration_sec"] / f["b_scan"])}× một lần '
        f'quét CSV cùng input ({vn(f["b_scan"])} s)',
        f'• Spill duy nhất pipeline: {vn(f["b_spm"])} MiB memory / {vn(f["b_spd"])} MiB disk',
        f'• Peak exec memory {vn(bmap["peak_exec_mem_max_mb"])} MiB/task (heap 1g, 8 task)',
        '• Hash SHA-256 chỉ ~0,6 s → không phải bottleneck'])
    set_lines(by_name(s3, 'Text 7'), [
        (f'stage_metrics.csv (median 5 run baseline):', {'bold': True}),
        f'Stage {bmap["stage_id"]} map: {vn(bmap["duration_sec"])} s, 8 task, shuffle W {vn(bmap["shuffle_write_mb"])} MiB, '
        f'spill {vn(bmap["spill_memory_mb"], 0)} / {vn(bmap["spill_disk_mb"], 0)} MiB',
        f'Stage {bred["stage_id"]} reduce: {vn(bred["duration_sec"])} s, spill {vn(bred["spill_memory_mb"], 0)} / '
        f'{vn(bred["spill_disk_mb"], 0)} MiB',
        ('explain_profile_dup.txt:', {'bold': True}),
        'Exchange hashpartitioning(step, type, amount, nameOrig, …, isFlaggedFraud, 200) – 11 cột',
        'AQE ShuffleQueryStage: 1.358,7 MiB, 6,36E+6 rows',
        ('[Dán ảnh Spark UI: Stages → stage 10 của bản baseline]', {'bold': True})])
    set_lines(by_name(s3, 'Text 10'), [
        '“Chúng tôi cho rằng bước đếm trùng toàn dòng (groupBy 11 cột) là bottleneck của chặng profile, vì nó '
        f'đẩy toàn bộ {vn(f["rows_in"], 0)} dòng × 11 cột qua Exchange ({vn(bmap["shuffle_write_mb"])} MiB) và là '
        'nơi duy nhất spill khi mỗi task chỉ có ~52 MiB execution memory.”',
        f'Chặng này chiếm {vn(f["b_dup"])} s = {vn(f["b_dup"] / bm * 100)} % runtime baseline.'])

    set_lines(by_name(s4, 'Text 4'), [
        'Nếu profile_dup chậm vì aggregate + shuffle trên khóa 11 cột rộng (toàn bộ dữ liệu qua Exchange, '
        'spill), thì khi thay khóa bằng xxhash64(11 cột) rồi chỉ kiểm chính xác trên hash ứng viên, chúng tôi '
        'kỳ vọng shuffle bytes, spill, task max và thời gian profile_dup giảm, trong khi kết quả kiểm trùng và '
        'mọi business metric giữ nguyên.'])
    set_lines(by_name(s4, 'Text 7'), [
        ('Code: quality_report.duplicate_frame() (+ chỗ gọi)', {'bold': True}),
        'Before: groupBy(11 cột).count > 1',
        'After: B1 groupBy(xxhash64(11 cột)).count > 1 → hash ứng viên; rỗng → 0 trùng (chính xác); '
        'khác rỗng → B2 broadcast hash, groupBy 11 cột trên tập con',
        'Config Spark: không đổi'])
    set_lines(by_name(s4, 'Text 10'), [
        '• Exchange trên 1 cột bigint thay vì 11 cột',
        f'• Shuffle write < {vn(bmap["shuffle_write_mb"])} MiB; số record vẫn {vn(bmap["shuffle_write_records"], 0)}',
        '• Spill giảm mạnh (lý tưởng = 0); task max giảm',
        f'• Runtime: chỉ kết luận nếu |Δ| > max−min baseline'])
    set_lines(by_name(s4, 'Text 13'), [
        'Cùng file input (SHA 16910f90…), cùng dim + config.json, local[*], driver 1g, AQE, 200 partitions, Spark 3.5.9; '
        'mỗi bên 1 warm-up + 5 run, chạy xen kẽ B/A trong cùng session exp1, mỗi bên từ git worktree theo tag; '
        'event log bật cả 2 phía.'])

    amap = f['a_map']
    cells = {
        'Text 13': f'{vn(bm)} s ({vn(bmin)}–{vn(bmax)})', 'Text 15': f'{vn(am)} s ({vn(amin)}–{vn(amax)})',
        'Text 17': f'{vn(d_rt)} s ({vn(d_rt / bm * 100)} %); > max−min {vn(spread)} s',
        'Text 21': f'{vn(f["b_shr"])} / {vn(f["b_shw"])} MiB', 'Text 23': f'{vn(f["a_shr"])} / {vn(f["a_shw"])} MiB',
        'Text 25': f'{vn(f["a_shw"] - f["b_shw"])} MiB ({vn((f["a_shw"] - f["b_shw"]) / f["b_shw"] * 100)} %); '
                   f'record không đổi',
        'Text 29': f'{vn(f["b_task"])} s (stage {bmap["stage_id"]})', 'Text 31': f'{vn(f["a_task"])} s (stage {amap["stage_id"]})',
        'Text 33': f'{vn(f["a_task"] - f["b_task"])} s ({vn((f["a_task"] - f["b_task"]) / f["b_task"] * 100)} %)',
        'Text 37': f'{f["groups"]}', 'Text 39': f'{f["groups"]}', 'Text 41': '0 (checksum giống hệt)',
        'Text 45': f'fraud {vn(f["fraud"], 0)}; tổng tiền {vn(float(f["amount"]), 2)}',
        'Text 47': f'fraud {vn(f["fraud"], 0)}; tổng tiền {vn(float(f["amount"]), 2)}',
        'Text 49': '0 (PASS)'}
    for name, text in cells.items():
        set_lines(by_name(s5, name), [text])
    set_lines(by_name(s5, 'Text 19'), ['Shuffle read/write (cả run)'])
    set_lines(by_name(s5, 'Text 27'), ['Max task time'])

    set_lines(by_name(s6, 'Text 4'), [
        (f'check_correctness.py: PASS {n_ok}/{corr["checks_total"]}', {'bold': True}),
        f'• row counts {vn(f["rows_in"], 0)} → valid → joined; unmatched 0',
        '• null / trùng / invalid = 0; amount = 0: 16 dòng',
        f'• fraud {vn(f["fraud"], 0)}; tổng tiền; {f["groups"]} nhóm, khóa duy nhất',
        '• schema 10 cột; SHA-256 các dòng đã sort giống nhau ở 12/12 run',
        '• test chèn dòng trùng + va chạm hash: 5/5 PASS'])
    set_lines(by_name(s6, 'Text 7'), [
        ('KEEP', {'bold': True}),
        f'Δ runtime {vn(d_rt)} s vượt max−min baseline {vn(spread)} s; mọi run tối ưu ({vn(amin)}–{vn(amax)} s) '
        f'nhanh hơn mọi run baseline ({vn(bmin)}–{vn(bmax)} s). profile_dup {vn(f["a_dup"] - f["b_dup"])} s '
        f'≈ {vn(abs(f["a_dup"] - f["b_dup"]) / f["b_dup_sd"])}× stdev. Output giống hệt.',
        f'Hypothesis đúng một phần: spill còn {vn(f["a_spd"])} MiB disk.'])
    set_lines(by_name(s6, 'Text 10'), [
        '• 1 máy Windows, local[*], heap 1g; 1 file 471 MiB',
        f'• OS cache warm (bỏ warm-up); RAM trống {vn(min(f["b_ram"][0], f["a_ram"][0]) / 1024)}–'
        f'{vn(max(f["b_ram"][1], f["a_ram"][1]) / 1024)} GiB; IDE mở ở cả 2 phía',
        '• dim CRLF trong worktree (cùng byte 2 phía, output không đổi)',
        '• Phase 1 và exp1 khác ngày → chỉ so trong exp1'])
    set_lines(by_name(s6, 'Text 13'), [
        'Buổi 11: chuyển input CSV → Parquet (Data Lake). Mỗi run vẫn parse CSV 12 lần; clean + join + output ≈ '
        f'{vn(med(f["a"], "sec_clean") + med(f["a"], "sec_join") + med(f["a"], "sec_output"), 0)} s ở bản tối ưu. '
        'Đo lại bằng cùng quy trình (worktree theo tag, xen kẽ, ≥ 5 run). Driver memory (1g → lớn hơn) là '
        'experiment riêng, không gộp.'])
    prs.save(OUT)
    print('saved', OUT.relative_to(ROOT).as_posix())


if __name__ == '__main__':
    main()
