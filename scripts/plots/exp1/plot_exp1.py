#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt
import numpy as np


FILE_RE = re.compile(
    r"shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_speed(?P<speed>\d+)_inject(?P<inject>\d+)_(?P<strategy>.+)_run(?P<run>\d+)\.json$"
)

STRATEGY_ORDER = ["SOTA-Lock", "Fine-tuned-Lock", "MVSS", "MVSS-Delta"]
PLOT_METRICS = [
    ("locked_ratio", "Ratio of Locked TXs", False),
    ("tps_global", "Throughput (TPS)", False),
    ("rdt_ratio", "Ratio of Disorderly TXs", False),
]


def safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def load_records(metrics_dir: Path):
    records = []
    for p in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        rec = {
            "file": p,
            "shards": int(m.group("shards")),
            "speed": int(m.group("speed")),
            "inject": int(m.group("inject")),
            "strategy": m.group("strategy"),
            "run": int(m.group("run")),
        }
        for k, _, _ in PLOT_METRICS:
            rec[k] = safe_float(data.get(k))
        records.append(rec)
    return records


def pick_records(records, run_filter: int | None, inject_filter: int | None = None):
    if inject_filter is not None:
        records = [r for r in records if r["inject"] == inject_filter]
    if run_filter is not None:
        return [r for r in records if r["run"] == run_filter]
    grouped = defaultdict(list)
    for r in records:
        grouped[(r["shards"], r["speed"], r["strategy"])].append(r)
    picked = []
    for rows in grouped.values():
        # 同组合下优先更高 inject 档（如 50000 覆盖 24000），再取最新 run
        picked.append(max(rows, key=lambda x: (x["inject"], x["run"])))
    picked.sort(
        key=lambda x: (
            x["shards"],
            x["speed"],
            STRATEGY_ORDER.index(x["strategy"]) if x["strategy"] in STRATEGY_ORDER else 99,
        )
    )
    return picked


def aggregate(records):
    # 兼容旧接口：每条记录已是 (shards, speed, strategy) 的唯一代表时直接返回。
    grouped = defaultdict(list)
    for r in records:
        grouped[(r["shards"], r["speed"], r["strategy"])].append(r)
    out = []
    for (shards, speed, strategy), rows in grouped.items():
        item = {"shards": shards, "speed": speed, "strategy": strategy, "n": len(rows)}
        for k, _, _ in PLOT_METRICS:
            vals = [x[k] for x in rows if x[k] is not None]
            item[k] = mean(vals) if vals else None
        out.append(item)
    out.sort(
        key=lambda x: (
            x["shards"],
            x["speed"],
            STRATEGY_ORDER.index(x["strategy"]) if x["strategy"] in STRATEGY_ORDER else 99,
        )
    )
    return out


def plot_fig3_style(agg, output_path: Path):
    shards_list = sorted({x["shards"] for x in agg})
    speed_list = sorted({x["speed"] for x in agg})
    if not shards_list or not speed_list:
        return

    groups = [(shards, speed) for shards in shards_list for speed in speed_list]
    xlabels = [f"[{speed},{shards}]" for shards, speed in groups]
    x = np.arange(len(groups), dtype=float)
    n_strat = len(STRATEGY_ORDER)
    width = 0.18
    offsets = (np.arange(n_strat) - (n_strat - 1) / 2.0) * width

    data_map = {}
    for row in agg:
        data_map[(row["shards"], row["speed"], row["strategy"])] = row

    fig, axes = plt.subplots(1, 3, figsize=(18, 5), sharex=True)
    for ax_idx, (metric_key, title, use_percent) in enumerate(PLOT_METRICS):
        ax = axes[ax_idx]
        for si, strategy in enumerate(STRATEGY_ORDER):
            vals = []
            for shards, speed in groups:
                row = data_map.get((shards, speed, strategy))
                val = 0.0 if row is None or row.get(metric_key) is None else float(row[metric_key])
                vals.append(val * 100.0 if use_percent else val)
            ax.bar(x + offsets[si], vals, width=width, label=strategy)
        ax.set_title(f"({chr(ord('a') + ax_idx)}) {title}")
        ax.grid(axis="y", alpha=0.3)
        if use_percent:
            ax.set_ylim(bottom=0)

    axes[0].set_xticks(x)
    axes[0].set_xticklabels(xlabels, rotation=25)
    axes[1].set_xticks(x)
    axes[1].set_xticklabels(xlabels, rotation=25)
    axes[2].set_xticks(x)
    axes[2].set_xticklabels(xlabels, rotation=25)
    axes[1].set_xlabel("[Inject Speed, # of Shards]")
    fig.legend(loc="upper center", ncol=4, bbox_to_anchor=(0.5, 1.02))
    fig.suptitle("Exp1 Fig3-Style Grouped Bars")
    fig.tight_layout(rect=(0, 0, 1, 0.93))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=220)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot Exp1 preliminary figures from metrics JSON files.")
    parser.add_argument(
        "--metrics-dir",
        default="results/exp1_scaling/metrics",
        help="Directory containing Exp1 metrics json files.",
    )
    parser.add_argument(
        "--output-dir",
        default="results/figures/exp1",
        help="Directory to save figure PNG files.",
    )
    parser.add_argument(
        "--run",
        type=int,
        default=None,
        help="Optional run index filter (e.g., 1). Default: latest run per (shards,speed,strategy).",
    )
    parser.add_argument(
        "--inject",
        type=int,
        default=None,
        help="Optional MaxInjectTxs tier filter (e.g., 50000). Default: prefer highest inject per combo.",
    )
    parser.add_argument(
        "--speed",
        type=int,
        default=None,
        help="Optional inject speed filter (e.g., 800).",
    )
    parser.add_argument(
        "--output-name",
        default="exp1_fig3_style_grouped.png",
        help="Output figure file name.",
    )
    args = parser.parse_args()

    metrics_dir = Path(args.metrics_dir)
    output_dir = Path(args.output_dir)
    records = load_records(metrics_dir)
    if not records:
        raise SystemExit(f"No valid metrics json found in: {metrics_dir}")
    records = pick_records(records, args.run, args.inject)
    if args.speed is not None:
        records = [r for r in records if r["speed"] == args.speed]
    if not records:
        raise SystemExit(f"No records left after run filter: {args.run}")

    agg = aggregate(records)
    plot_fig3_style(agg, output_dir / args.output_name)

    print(f"[OK] Exp1 figures written to: {output_dir}")
    print(f"  - {args.output_name}")


if __name__ == "__main__":
    main()

