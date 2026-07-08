#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""从 results/ 下 Exp6 raw 目录重算 metrics（不跑仿真）。"""

from __future__ import annotations

import json
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from metrics_definitions import analyze_run, run_metrics_to_dict  # noqa: E402

DATASETS = [
    ("2x2", ROOT / "results/exp6_sensitivity/raw/shards2_nodes2"),
    ("4x4", ROOT / "results/exp6_scale_4x4_full/raw/shards4_nodes4"),
]

KEYS = [
    "tps_global",
    "tps_migration",
    "latency_p95",
    "probe_ok",
    "sync_send_count",
    "sync_bandwidth_mb",
    "dsr",
    "exp6_stage3_makespan_ms",
    "exp6_sync_per_ack_latency_median_ms",
    "exp6_sync_per_ack_latency_p95_ms",
    "exp6_probe_tx1_to_tx3_span_median_ms",
    "sync_batch_size_mean",
    "migration_completion_ms",
]


def main() -> None:
    out_dir = ROOT / "results/exp6_sensitivity/metrics_recalc_v2"
    out_dir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict] = []

    for scale, base in DATASETS:
        if not base.exists():
            print(f"skip missing {base}")
            continue
        for window_dir in sorted(base.glob("window_*")):
            w = window_dir.name.replace("window_", "")
            run_metrics: list[dict] = []
            for run_dir in sorted(window_dir.glob("run*")):
                if not (run_dir / "S0_block.csv").exists():
                    continue
                try:
                    m = run_metrics_to_dict(analyze_run(run_dir))
                except Exception as e:
                    print(f"ERR {run_dir}: {e}")
                    continue
                out_path = out_dir / f"{scale}_window{w}_{run_dir.name}.json"
                out_path.write_text(
                    json.dumps(m, ensure_ascii=False, indent=2), encoding="utf-8"
                )
                run_metrics.append(m)
            if not run_metrics:
                continue
            row: dict = {"scale": scale, "window": int(w), "runs": len(run_metrics)}
            for k in KEYS:
                if k == "probe_ok":
                    vals = [m[k] for m in run_metrics if k in m]
                    row[k] = sum(1 for v in vals if v) / len(vals) if vals else 0.0
                    continue
                vals = [
                    m[k] for m in run_metrics if k in m and isinstance(m[k], (int, float))
                ]
                if not vals:
                    continue
                row[f"{k}_mean"] = statistics.mean(vals)
                if len(vals) > 1:
                    row[f"{k}_std"] = statistics.stdev(vals)
            all_rows.append(row)

    header = ["scale", "window", "runs"] + [k for k in KEYS]
    print("\t".join(header))
    for r in sorted(all_rows, key=lambda x: (x["scale"], x["window"])):
        parts = [r["scale"], str(r["window"]), str(r["runs"])]
        for k in KEYS:
            if k == "probe_ok":
                parts.append(f"{r.get(k, 0) * 100:.0f}%")
            else:
                v = r.get(f"{k}_mean")
                parts.append("-" if v is None else f"{v:.2f}")
        print("\t".join(parts))

    summary_path = out_dir / "exp6_recalc_summary.json"
    summary_path.write_text(
        json.dumps(all_rows, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    md_path = out_dir / "exp6_recalc_summary.md"
    lines = [
        "# Exp6 指标重算汇总（metrics_recalc_v2）",
        "",
        "数据来源：`results/exp6_sensitivity/raw`（2x2）与 `results/exp6_scale_4x4_full/raw`（4x4）。",
        "未重新跑仿真，仅对 raw CSV 调用 `metrics_definitions.py`。",
        "",
        "| 规模 | W | runs | tps_global | tps_mig | latency_p95 | probe_ok | stage3_ms | sync_ack_p50 | sync_ack_p95 | batch_mean | dsr |",
        "|------|---|------|------------|---------|-------------|----------|-----------|--------------|--------------|------------|-----|",
    ]
    for r in sorted(all_rows, key=lambda x: (x["scale"], x["window"])):
        lines.append(
            "| {scale} | {window} | {runs} | {tps_global:.2f} | {tps_mig:.2f} | {lat:.0f} | {probe} | {s3:.0f} | {p50:.0f} | {p95:.0f} | {batch:.1f} | {dsr:.2f} |".format(
                scale=r["scale"],
                window=r["window"],
                runs=r["runs"],
                tps_global=r.get("tps_global_mean", 0),
                tps_mig=r.get("tps_migration_mean", 0),
                lat=r.get("latency_p95_mean", 0),
                probe=f"{r.get('probe_ok', 0) * 100:.0f}%",
                s3=r.get("exp6_stage3_makespan_ms_mean", 0),
                p50=r.get("exp6_sync_per_ack_latency_median_ms_mean", 0),
                p95=r.get("exp6_sync_per_ack_latency_p95_ms_mean", 0),
                batch=r.get("sync_batch_size_mean_mean", 0),
                dsr=r.get("dsr_mean", 0),
            )
        )
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"\nWrote per-run JSON + summary to {out_dir}")


if __name__ == "__main__":
    main()
