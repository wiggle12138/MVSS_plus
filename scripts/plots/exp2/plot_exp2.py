#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt


FILE_RE = re.compile(r"^(?P<strategy>MVSSDelta|MVSS)_probe(?P<probe>\d+)_run(?P<run>\d+)\.json$")
STRATEGY_LABEL = {"MVSS": "MVSS", "MVSSDelta": "MVSS-Delta"}
METRICS = [
    ("sync_send_count", "Sync Send Count"),
    ("sync_bandwidth_mb", "Sync Bandwidth (MB)"),
    ("exp6_stage3_makespan_ms", "Stage3 Makespan (ms)"),
    ("tps_global", "TPS Global"),
    ("latency_p95", "Latency P95 (ms)"),
]


def safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def load_records(metrics_dir: Path):
    rows = []
    for p in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        rec = {
            "strategy": STRATEGY_LABEL[m.group("strategy")],
            "probe": int(m.group("probe")),
            "run": int(m.group("run")),
            "file": p.name,
        }
        for k, _ in METRICS:
            rec[k] = safe_float(data.get(k))
        rec["probe_ok"] = bool(data.get("probe_ok", False))
        rows.append(rec)
    return rows


def aggregate(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[(r["probe"], r["strategy"])].append(r)
    out = []
    for (probe, strategy), items in grouped.items():
        row = {"probe": probe, "strategy": strategy, "n": len(items)}
        for k, _ in METRICS:
            vals = [x[k] for x in items if x[k] is not None]
            row[k] = mean(vals) if vals else None
        row["probe_ok_rate"] = sum(1 for x in items if x["probe_ok"]) / len(items) if items else 0.0
        out.append(row)
    out.sort(key=lambda x: (x["probe"], 0 if x["strategy"] == "MVSS" else 1))
    return out


def plot_bar_for_probe(agg, probe, output_path: Path):
    rows = [x for x in agg if x["probe"] == probe]
    if not rows:
        return
    rows.sort(key=lambda x: 0 if x["strategy"] == "MVSS" else 1)
    strategies = [r["strategy"] for r in rows]

    fig, axes = plt.subplots(2, 3, figsize=(14, 8))
    axes = axes.flatten()
    for i, (k, title) in enumerate(METRICS):
        vals = [r[k] if r[k] is not None else 0.0 for r in rows]
        axes[i].bar(strategies, vals)
        axes[i].set_title(title)
        axes[i].grid(axis="y", alpha=0.3)

    ok_vals = [r["probe_ok_rate"] * 100 for r in rows]
    axes[5].bar(strategies, ok_vals)
    axes[5].set_title("Probe OK Rate (%)")
    axes[5].set_ylim(0, 105)
    axes[5].grid(axis="y", alpha=0.3)

    fig.suptitle(f"Exp2 Comparison (probe={probe})")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def plot_trend_by_probe(agg, output_path: Path):
    probes = sorted({x["probe"] for x in agg})
    if len(probes) <= 1:
        return

    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()
    trend_metrics = [
        ("sync_send_count", "Sync Send Count"),
        ("sync_bandwidth_mb", "Sync Bandwidth (MB)"),
        ("exp6_stage3_makespan_ms", "Stage3 Makespan (ms)"),
        ("probe_ok_rate", "Probe OK Rate"),
    ]
    for i, (k, title) in enumerate(trend_metrics):
        ax = axes[i]
        for s in ["MVSS", "MVSS-Delta"]:
            rows = sorted([x for x in agg if x["strategy"] == s], key=lambda x: x["probe"])
            xs = [r["probe"] for r in rows]
            ys = [r[k] * 100 if k == "probe_ok_rate" else r[k] for r in rows]
            ax.plot(xs, ys, marker="o", label=s)
        ax.set_title(title)
        ax.set_xlabel("SyncProbeMaxAccounts")
        ax.grid(alpha=0.3)
    handles, labels = axes[0].get_legend_handles_labels()
    if handles:
        fig.legend(handles, labels, loc="upper center", ncol=2)
    fig.suptitle("Exp2 Trend by Probe Accounts")
    fig.tight_layout(rect=(0, 0, 1, 0.95))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot Exp2 preliminary figures from metrics JSON files.")
    parser.add_argument("--metrics-dir", default="results/exp2_concurrency/metrics")
    parser.add_argument("--output-dir", default="results/figures/exp2")
    args = parser.parse_args()

    records = load_records(Path(args.metrics_dir))
    if not records:
        raise SystemExit(f"No valid exp2 metrics files found in: {args.metrics_dir}")

    agg = aggregate(records)
    probes = sorted({x["probe"] for x in agg})
    out_dir = Path(args.output_dir)
    for p in probes:
        plot_bar_for_probe(agg, p, out_dir / f"exp2_compare_probe{p}.png")
    plot_trend_by_probe(agg, out_dir / "exp2_trend_by_probe.png")

    print(f"[OK] Exp2 figures written to: {out_dir}")
    for p in probes:
        print(f"  - exp2_compare_probe{p}.png")
    if len(probes) > 1:
        print("  - exp2_trend_by_probe.png")


if __name__ == "__main__":
    main()

