#!/usr/bin/env python3
"""Summarize Exp4 metrics into CSV/Markdown."""
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path

FILE_RE = re.compile(
    r"^dataset_(?P<dataset>.+)_shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_(?P<strategy>.+)_run(?P<run>\d+)\.json$"
)


def parse_args():
    p = argparse.ArgumentParser(description="Summarize Exp4 metrics.")
    p.add_argument("--metrics-dir", default="results/exp4_eth_workload/metrics")
    p.add_argument("--all-runs", action="store_true", help="Include all runs (default: latest per strategy).")
    p.add_argument("--out-csv", default="results/exp4_eth_workload/summary/exp4_metrics.csv")
    p.add_argument("--out-md", default="results/exp4_eth_workload/summary/exp4_summary.md")
    return p.parse_args()


def as_float(v, default=0.0):
    try:
        return float(v) if v is not None else default
    except (TypeError, ValueError):
        return default


def main():
    args = parse_args()
    metrics_dir = Path(args.metrics_dir)
    rows_raw = []
    for path in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(path.name)
        if not m:
            continue
        data = json.loads(path.read_text(encoding="utf-8"))
        rows_raw.append(
            {
                "dataset": m.group("dataset"),
                "shards": int(m.group("shards")),
                "nodes": int(m.group("nodes")),
                "strategy": m.group("strategy"),
                "run": int(m.group("run")),
                "tps_global": as_float(data.get("tps_global")),
                "tps_migration": as_float(data.get("tps_migration")),
                "latency_p95": as_float(data.get("latency_p95")),
                "locked_ratio": as_float(data.get("locked_ratio")),
                "rdt_ratio": as_float(data.get("rdt_ratio")),
                "tx_committed_total": int(data.get("tx_committed_total") or 0),
            }
        )

    if not rows_raw:
        raise SystemExit(f"No metrics json in {metrics_dir}")

    if not args.all_runs:
        grouped = {}
        for r in rows_raw:
            key = (r["dataset"], r["shards"], r["strategy"])
            if key not in grouped or r["run"] > grouped[key]["run"]:
                grouped[key] = r
        rows = list(grouped.values())
        tag = "latest_per_combo"
    else:
        rows = rows_raw
        tag = "all_runs"

    rows.sort(key=lambda r: (r["strategy"], r["run"]))
    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    out_csv.parent.mkdir(parents=True, exist_ok=True)

    fields = list(rows[0].keys())
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        w.writeheader()
        w.writerows(rows)

    lines = [
        f"# Exp4 真实以太坊负载汇总（{tag}）",
        "",
        f"- 样本数: {len(rows)}",
        "",
        "| dataset | shards | strategy | run | tps_global | latency_p95 | locked_ratio | rdt_ratio | tx_committed |",
        "|---|---:|---|---:|---:|---:|---:|---:|---:|",
    ]
    for r in rows:
        lines.append(
            f"| {r['dataset']} | {r['shards']} | {r['strategy']} | {r['run']} | "
            f"{r['tps_global']:.3f} | {r['latency_p95']:.1f} | {r['locked_ratio']:.6f} | "
            f"{r['rdt_ratio']:.6f} | {r['tx_committed_total']} |"
        )
    out_md.write_text("\n".join(lines), encoding="utf-8")
    print(f"[OK] rows={len(rows)} csv={out_csv} md={out_md}")


if __name__ == "__main__":
    main()
