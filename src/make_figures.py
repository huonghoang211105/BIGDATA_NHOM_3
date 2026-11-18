"""Hình cho báo cáo CP4 (matplotlib, nhãn tiếng Việt) từ benchmark.csv + stage_metrics.csv của một session.

    python src/make_figures.py --session exp1
    -> reports/figures/runtime_truoc_sau.png, thoi_gian_theo_chang.png, profile_dup_shuffle_spill.png

Chỉ dùng số đo thật: median (và min/max) của các run đo, bỏ warm-up. Không cần Spark.
"""
import argparse
import csv
from pathlib import Path
from statistics import median

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
# Bảng màu categorical tham chiếu (đã chạy validator: PASS; 3 màu sáng có nhãn số trực tiếp).
SERIES = ['#2a78d6', '#eb6834', '#1baf7a', '#eda100', '#e87ba4', '#008300', '#6250d6']
SURFACE, INK, INK2, GRID = '#fcfcfb', '#0b0b0b', '#52514e', '#e4e3df'
NAMES = {'baseline': 'Baseline (ae84592)', 'optimized': 'Tối ưu (d2b15a5)'}
PHASES = [('sec_spark_startup', 'Khởi động Spark'), ('sec_profile_null', 'Profile null'),
          ('sec_profile_dup', 'Profile trùng'), ('sec_clean', 'Clean (T1–T4)'), ('sec_join', 'Join'),
          ('sec_output', 'Output'), (None, 'Khác (import, hash, đọc schema)')]


def vn(value, digits=1):
    return f'{value:,.{digits}f}'.replace(',', ' ').replace('.', ',').replace(' ', '.')


def style(ax):
    ax.set_facecolor(SURFACE)
    for side in ('top', 'right'):
        ax.spines[side].set_visible(False)
    for side in ('left', 'bottom'):
        ax.spines[side].set_color(INK2)
    ax.tick_params(colors=INK2, labelsize=10)
    ax.yaxis.label.set_color(INK2)
    ax.xaxis.label.set_color(INK2)


def load(csv_path, session):
    with csv_path.open(encoding='utf-8', newline='') as stream:
        rows = [r for r in csv.DictReader(stream)
                if r.get('session') == session and r['warm_or_cold'] != 'warmup']
    by = {}
    for r in rows:
        by.setdefault(r['label'], []).append(r)
    return by


def col(rows, name):
    return [float(r[name]) for r in rows]


def dup_stages(row):
    """(stage map, stage reduce) của profile_dup: map = input lớn nhất, reduce = shuffle read lớn nhất."""
    with (ROOT / row['evidence_dir'] / 'stage_metrics.csv').open(encoding='utf-8', newline='') as stream:
        stages = [s for s in csv.DictReader(stream) if s['phase'] == 'profile_dup']
    return (max(stages, key=lambda s: float(s['input_mb'])),
            max(stages, key=lambda s: float(s['shuffle_read_mb'])))


def fig_runtime(by, out, phase1_median):
    fig, ax = plt.subplots(figsize=(7.2, 4.6), dpi=150, facecolor=SURFACE)
    style(ax)
    labels = ['baseline', 'optimized']
    for i, label in enumerate(labels):
        vals = col(by[label], 'runtime_sec')
        med, lo, hi = median(vals), min(vals), max(vals)
        ax.bar(i, med, width=0.5, color=SERIES[i], zorder=2)
        ax.errorbar(i, med, yerr=[[med - lo], [hi - med]], fmt='none', ecolor=INK, elinewidth=1.5,
                    capsize=8, zorder=3)
        ax.scatter([i + 0.32] * len(vals), vals, s=22, color=SERIES[i], edgecolor=SURFACE, linewidth=1,
                   zorder=3)
        ax.text(i, hi + 3, f'median {vn(med)} s\n(min {vn(lo)} – max {vn(hi)})', ha='center', va='bottom',
                fontsize=9.5, color=INK)
    ax.axhline(phase1_median, color=INK2, linestyle='--', linewidth=1, zorder=1)
    ax.text(1.45, phase1_median + 2, f'Baseline Phase 1: {vn(phase1_median)} s', ha='right', va='bottom',
            fontsize=8.5, color=INK2)
    ax.set_xticks(range(2), [NAMES[l] for l in labels])
    ax.set_xlim(-0.6, 1.6)
    ax.set_ylabel('Thời gian chạy toàn pipeline (giây)')
    top = max(max(col(by[l], 'runtime_sec')) for l in labels)
    ax.set_ylim(0, top * 1.22)
    ax.grid(axis='y', color=GRID, linewidth=0.8, zorder=0)
    n = min(len(by[l]) for l in labels)
    ax.set_title(f'Runtime trước / sau tối ưu (session exp1, {n} run mỗi bên, chạy xen kẽ)\n'
                 'Cột = median, thanh = min–max, chấm = từng run', fontsize=11, color=INK, loc='left')
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def fig_phases(by, out):
    fig, ax = plt.subplots(figsize=(10, 4.2), dpi=150, facecolor=SURFACE)
    style(ax)
    labels = ['baseline', 'optimized']
    for y, label in enumerate(labels):
        rows = by[label]
        left = 0.0
        for k, (name, title) in enumerate(PHASES):
            if name:
                width = median(col(rows, name))
            else:
                width = median([float(r['sec_imports']) + float(r['sec_hash']) + float(r['sec_read_schema'])
                                for r in rows])
            ax.barh(y, width, left=left, height=0.55, color=SERIES[k], edgecolor=SURFACE, linewidth=2,
                    label=title if y == 0 else None, zorder=2)
            if width >= 8:
                ax.text(left + width / 2, y, vn(width), ha='center', va='center', fontsize=9, color=INK)
            left += width
        ax.text(left + 2, y, f'Σ median chặng ≈ {vn(left)} s', va='center', fontsize=9, color=INK2)
    ax.set_yticks(range(2), [NAMES[l] for l in labels])
    ax.invert_yaxis()
    ax.set_xlabel('Giây (median từng chặng, đo bằng mốc [TIMING] trong run.log)')
    ax.set_xlim(0, ax.get_xlim()[1] * 1.12)
    ax.grid(axis='x', color=GRID, linewidth=0.8, zorder=0)
    ax.legend(ncol=4, loc='lower left', bbox_to_anchor=(0, 1.02), frameon=False, fontsize=9,
              labelcolor=INK)
    ax.set_title('Thời gian theo chặng – chỉ chặng Profile trùng được thay đổi', fontsize=11, color=INK,
                 loc='left', pad=46)
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)


def fig_dup(by, out):
    labels = ['baseline', 'optimized']
    data = {}
    for label in labels:
        rows = by[label]
        stages = [dup_stages(r) for r in rows]
        data[label] = {
            'Shuffle write': median(col(rows, 'dup_shuffle_write_mb')),
            'Spill (memory)': median(col(rows, 'dup_spill_memory_mb')),
            'Spill (disk)': median(col(rows, 'dup_spill_disk_mb')),
            'Stage map: task max': median(float(m['task_max_sec']) for m, _ in stages),
            'Stage reduce: duration': median(float(rd['duration_sec']) for _, rd in stages),
            'Cả chặng profile_dup': median(col(rows, 'sec_profile_dup'))}
    fig, axes = plt.subplots(1, 2, figsize=(11, 4.6), dpi=150, facecolor=SURFACE)
    panels = [(['Shuffle write', 'Spill (memory)', 'Spill (disk)'], 'MiB (toàn chặng profile_dup)'),
              (['Stage map: task max', 'Stage reduce: duration', 'Cả chặng profile_dup'], 'Giây')]
    for ax, (metrics, unit) in zip(axes, panels):
        style(ax)
        width = 0.36
        for i, label in enumerate(labels):
            xs = [m + (i - 0.5) * width for m in range(len(metrics))]
            vals = [data[label][m] for m in metrics]
            ax.bar(xs, vals, width=width - 0.03, color=SERIES[i], label=NAMES[label], zorder=2)
            for x, v in zip(xs, vals):
                ax.text(x, v, vn(v), ha='center', va='bottom', fontsize=8.5, color=INK)
        ax.set_xticks(range(len(metrics)), metrics, fontsize=9.5)
        ax.set_ylabel(unit)
        ax.grid(axis='y', color=GRID, linewidth=0.8, zorder=0)
        ax.set_ylim(0, max(max(data[l][m] for m in metrics) for l in labels) * 1.15)
    axes[0].legend(frameon=False, fontsize=9, loc='upper right', labelcolor=INK)
    fig.suptitle('Profile trùng (đếm trùng 11 cột): shuffle, spill và thời gian – median session exp1',
                 fontsize=11, color=INK, x=0.01, ha='left')
    fig.tight_layout()
    fig.savefig(out, facecolor=SURFACE)
    plt.close(fig)
    return data


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--session', default='exp1')
    parser.add_argument('--csv', type=Path, default=ROOT / 'benchmark.csv')
    parser.add_argument('--out-dir', type=Path, default=ROOT / 'reports' / 'figures')
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    by = load(args.csv, args.session)
    phase1 = load(args.csv, 'phase1')['baseline']
    fig_runtime(by, args.out_dir / 'runtime_truoc_sau.png', median(col(phase1, 'runtime_sec')))
    fig_phases(by, args.out_dir / 'thoi_gian_theo_chang.png')
    data = fig_dup(by, args.out_dir / 'profile_dup_shuffle_spill.png')
    for label, values in data.items():
        print(label, {k: round(v, 3) for k, v in values.items()})
    print('figures ->', args.out_dir.relative_to(ROOT).as_posix())


if __name__ == '__main__':
    main()
