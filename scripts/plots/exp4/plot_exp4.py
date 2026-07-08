#!/usr/bin/env python3
"""Plot Exp4 four-strategy comparison from metrics JSON."""
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
    r"dataset_(?P<dataset>.+)_shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_(?P<strategy>.+)_run(?P<run>\d+)\.json$"
)
STRATEGY_ORDER = ["SOTA-Lock", "Fine-tuned-Lock", "MVSS", "MVSS-Delta"]
METRICS = [
    ("locked_ratio", "Ratio of Locked TXs", True),
    ("tps_global", "Throughput (TPS)", False),
    ("rdt_ratio", "Ratio of Disorderly TXs", True),
]


def load_rows(metrics_dir: Path):
    rows = []
    for p in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        rows.append(
            {
                "strategy": m.group("strategy"),
                "run": int(m.group("run")),
                **{k: data.get(k) for k, _, _ in METRICS},
            }
        )
    return rows


def pick_latest(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["strategy"]].append(r)
    out = []
    for strategy in STRATEGY_ORDER:
        items = grouped.get(strategy, [])
        if items:
            out.append(max(items, key=lambda x: x["run"]))
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--metrics-dir", default="results/exp4_eth_workload/metrics")
    ap.add_argument("--output-dir", default="results/figures/exp4")
    ap.add_argument("--output-name", default="exp4_strategy_compare.png")
    args = ap.parse_args()

    rows = load_rows(Path(args.metrics_dir))
    if not rows:
        raise SystemExit(f"No metrics in {args.metrics_dir}")
    picked = pick_latest(rows)

    fig, axes = plt.subplots(1, 3, figsize=(14, 4.5))
    x = np.arange(len(STRATEGY_ORDER))
    data_map = {r["strategy"]: r for r in picked}

    for ax, (key, title, as_pct) in zip(axes, METRICS):
        vals = []
        for s in STRATEGY_ORDER:
            v = data_map.get(s, {}).get(key)
            val = 0.0 if v is None else float(v)
            vals.append(val * 100.0 if as_pct else val)
        ax.bar(x, vals, color=["#4C72B0", "#55A868", "#C44E52", "#8172B2"])
        ax.set_title(title)
        ax.set_xticks(x)
        ax.set_xticklabels(STRATEGY_ORDER, rotation=20, ha="right")
        ax.grid(axis="y", alpha=0.3)

    fig.suptitle("Exp4: Real Ethereum Workload (4 strategies)")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    out = Path(args.output_dir) / args.output_name
    out.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(out, dpi=220)
    plt.close(fig)
    print(f"[OK] figure={out}")


if __name__ == "__main__":
    main()
