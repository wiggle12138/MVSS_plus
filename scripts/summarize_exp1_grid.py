#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import re
from pathlib import Path


FILE_RE = re.compile(
    r"^shards(?P<shards>\d+)_nodes(?P<nodes>\d+)_speed(?P<speed>\d+)_inject(?P<inject>\d+)_(?P<strategy>.+)_run(?P<run>\d+)\.json$"
)


def parse_args():
    p = argparse.ArgumentParser(description="Summarize Exp1 metrics into CSV/Markdown.")
    p.add_argument("--metrics-dir", default="results/exp1_scaling/metrics")
    p.add_argument("--run", type=int, default=None, help="Only include this run index.")
    p.add_argument(
        "--latest-per-combo",
        action="store_true",
        help="Pick latest run per (shards,speed,inject,strategy); ignores --run.",
    )
    p.add_argument(
        "--inject",
        type=int,
        default=None,
        help="Only include this MaxInjectTxs tier (e.g. 50000). Default: all tiers.",
    )
    p.add_argument("--out-csv", default="results/exp1_scaling/summary/exp1_latest_summary.csv")
    p.add_argument("--out-md", default="results/exp1_scaling/summary/exp1_latest_summary.md")
    return p.parse_args()


def as_float(v, default=0.0):
    try:
        if v is None:
            return default
        return float(v)
    except (TypeError, ValueError):
        return default


def as_int(v, default=0):
    try:
        if v is None:
            return default
        return int(v)
    except (TypeError, ValueError):
        return default


def main():
    args = parse_args()
    metrics_dir = Path(args.metrics_dir)
    out_csv = Path(args.out_csv)
    out_md = Path(args.out_md)
    rows_raw = []
    for path in sorted(metrics_dir.glob("*.json")):
        m = FILE_RE.match(path.name)
        if not m:
            continue
        run_idx = int(m.group("run"))
        data = json.loads(path.read_text(encoding="utf-8"))
        rows_raw.append(
            {
                "shards": int(m.group("shards")),
                "nodes": int(m.group("nodes")),
                "speed": int(m.group("speed")),
                "inject": int(m.group("inject")),
                "strategy": m.group("strategy"),
                "run": run_idx,
                "tps_global": as_float(data.get("tps_global")),
                "latency_p95": as_float(data.get("latency_p95")),
                "locked_ratio": as_float(data.get("locked_ratio")),
                "rdt_ratio": as_float(data.get("rdt_ratio")),
                "tx_committed_total": as_int(data.get("tx_committed_total")),
                "probe_ok": bool(data.get("probe_ok", False)),
            }
        )

    if not rows_raw:
        raise SystemExit(f"No metrics json found in {metrics_dir}")

    if args.inject is not None:
        rows_raw = [r for r in rows_raw if r["inject"] == args.inject]

    if args.latest_per_combo:
        grouped = {}
        for r in rows_raw:
            key = (r["shards"], r["speed"], r["inject"], r["strategy"])
            if key not in grouped or r["run"] > grouped[key]["run"]:
                grouped[key] = r
        rows = list(grouped.values())
        summary_tag = "latest_per_combo"
        if args.inject is not None:
            summary_tag += f"_inject{args.inject}"
    else:
        run_idx = args.run if args.run is not None else 1
        rows = [r for r in rows_raw if r["run"] == run_idx]
        summary_tag = f"run{run_idx}"

    if not rows:
        raise SystemExit(f"No rows matched filter in {metrics_dir}")

    rows.sort(key=lambda r: (r["shards"], r["speed"], r["inject"], r["strategy"]))

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    with out_csv.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)

    lines = [
        f"# Exp1 网格运行汇总（{summary_tag}）",
        "",
        f"- 样本数: {len(rows)}",
        "",
        "| shards | speed | inject | strategy | run | tps_global | latency_p95 | locked_ratio | rdt_ratio | tx_committed_total | probe_ok |",
        "|---:|---:|---:|---|---:|---:|---:|---:|---:|---:|---|",
    ]
    for r in rows:
        lines.append(
            "| {shards} | {speed} | {inject} | {strategy} | {run} | {tps:.3f} | {lat:.1f} | {lock:.6f} | {rdt:.6f} | {commit} | {ok} |".format(
                shards=r["shards"],
                speed=r["speed"],
                inject=r["inject"],
                strategy=r["strategy"],
                run=r["run"],
                tps=r["tps_global"],
                lat=r["latency_p95"],
                lock=r["locked_ratio"],
                rdt=r["rdt_ratio"],
                commit=r["tx_committed_total"],
                ok=r["probe_ok"],
            )
        )
    out_md.write_text("\n".join(lines), encoding="utf-8")

    print(f"[OK] rows={len(rows)}")
    print(f"[OK] csv={out_csv}")
    print(f"[OK] md={out_md}")


if __name__ == "__main__":
    main()

