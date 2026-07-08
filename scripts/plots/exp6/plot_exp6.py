#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from collections import defaultdict
from pathlib import Path
from statistics import mean

import matplotlib.pyplot as plt


FILE_RE = re.compile(
    r"^shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_window(?P<window>\d+)_run(?P<run>\d+)_probe(?P<probe>\d+)_inject(?P<inject>\d+)\.json$"
)


def safe_float(v):
    try:
        if v is None:
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def load_records(metrics_dir: Path, shards: int, nodes: int):
    rows = []
    for p in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(p.name)
        if not m:
            continue
        if int(m.group("shards")) != shards or int(m.group("nodes")) != nodes:
            continue
        data = json.loads(p.read_text(encoding="utf-8"))
        rows.append(
            {
                "window": int(m.group("window")),
                "run": int(m.group("run")),
                "probe": int(m.group("probe")),
                "inject": int(m.group("inject")),
                "stage3_ms": safe_float(data.get("exp6_stage3_makespan_ms")),
                "sync_send_count": safe_float(data.get("sync_send_count")),
                "batch_size_mean": safe_float(data.get("sync_batch_size_mean")),
                "probe_ok": bool(data.get("probe_ok", False)),
                "tps_global": safe_float(data.get("tps_global")),
                "latency_p95": safe_float(data.get("latency_p95")),
            }
        )
    return rows


def aggregate(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["window"]].append(r)
    out = []
    for w, items in grouped.items():
        entry = {"window": w, "n": len(items)}
        for key in ("stage3_ms", "sync_send_count", "batch_size_mean", "tps_global", "latency_p95"):
            vals = [x[key] for x in items if x[key] is not None]
            entry[key] = mean(vals) if vals else None
        entry["probe_ok_rate"] = sum(1 for x in items if x["probe_ok"]) / len(items) if items else 0.0
        out.append(entry)
    out.sort(key=lambda x: x["window"])
    return out


def plot_core(agg, output_path: Path):
    windows = [x["window"] for x in agg]
    if not windows:
        return
    fig, axes = plt.subplots(2, 2, figsize=(12, 8))
    axes = axes.flatten()

    axes[0].plot(windows, [x["stage3_ms"] for x in agg], marker="o")
    axes[0].set_title("Stage3 Makespan (ms)")
    axes[0].set_xlabel("DeltaAggregateWindowMs")
    axes[0].grid(alpha=0.3)

    axes[1].plot(windows, [x["sync_send_count"] for x in agg], marker="o")
    axes[1].set_title("Sync Send Count")
    axes[1].set_xlabel("DeltaAggregateWindowMs")
    axes[1].grid(alpha=0.3)

    axes[2].plot(windows, [x["batch_size_mean"] for x in agg], marker="o")
    axes[2].set_title("Sync Batch Size Mean")
    axes[2].set_xlabel("DeltaAggregateWindowMs")
    axes[2].grid(alpha=0.3)

    axes[3].plot(windows, [x["probe_ok_rate"] * 100 for x in agg], marker="o")
    axes[3].set_title("Probe OK Rate (%)")
    axes[3].set_xlabel("DeltaAggregateWindowMs")
    axes[3].set_ylim(0, 105)
    axes[3].grid(alpha=0.3)

    fig.suptitle("Exp6 Core Sensitivity Curves")
    fig.tight_layout()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def plot_global(agg, output_path: Path):
    windows = [x["window"] for x in agg]
    if not windows:
        return
    fig, ax1 = plt.subplots(figsize=(8, 4.5))
    ax2 = ax1.twinx()

    ax1.plot(windows, [x["tps_global"] for x in agg], marker="o", color="tab:blue", label="TPS Global")
    ax2.plot(windows, [x["latency_p95"] for x in agg], marker="s", color="tab:red", label="Latency P95")
    ax1.set_xlabel("DeltaAggregateWindowMs")
    ax1.set_ylabel("TPS Global", color="tab:blue")
    ax2.set_ylabel("Latency P95 (ms)", color="tab:red")
    ax1.grid(alpha=0.3)

    lines, labels = [], []
    for ax in (ax1, ax2):
        l, lab = ax.get_legend_handles_labels()
        lines.extend(l)
        labels.extend(lab)
    fig.legend(lines, labels, loc="upper center", ncol=2)
    fig.suptitle("Exp6 Global Metrics vs Window")
    fig.tight_layout(rect=(0, 0, 1, 0.92))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=200)
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Plot Exp6 preliminary figures from metrics JSON files.")
    parser.add_argument("--metrics-dir", default="results/exp6_sensitivity/metrics")
    parser.add_argument("--output-dir", default="results/figures/exp6")
    parser.add_argument("--shards", type=int, default=4, help="Shard count filter.")
    parser.add_argument("--nodes", type=int, default=4, help="Nodes-per-shard filter.")
    args = parser.parse_args()

    rows = load_records(Path(args.metrics_dir), args.shards, args.nodes)
    if not rows:
        raise SystemExit(
            f"No valid exp6 metrics files found in {args.metrics_dir} for shards={args.shards}, nodes={args.nodes}"
        )
    agg = aggregate(rows)
    out_dir = Path(args.output_dir)
    suffix = f"shards{args.shards}_nodes{args.nodes}"
    plot_core(agg, out_dir / f"exp6_core_{suffix}.png")
    plot_global(agg, out_dir / f"exp6_global_{suffix}.png")

    print(f"[OK] Exp6 figures written to: {out_dir}")
    print(f"  - exp6_core_{suffix}.png")
    print(f"  - exp6_global_{suffix}.png")


if __name__ == "__main__":
    main()

