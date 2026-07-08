#!/usr/bin/env python3
"""Aggregate Exp2 MVSS vs MVSS-Delta metrics (global + sync + probe)."""

from __future__ import annotations

import argparse
import json
import statistics
import subprocess
import sys
from pathlib import Path


GLOBAL_FIELDS = [
    ("tps_global", "TPS 全局"),
    ("tps_migration", "TPS 迁移窗"),
    ("latency_p95", "时延 P95 (ms)"),
    ("latency_mean", "时延均值 (ms)"),
    ("tx_committed_total", "提交 tx 数"),
    ("relay_ratio", "Relay 比例"),
    ("locked_ratio", "锁定比例"),
    ("avg_queue_len", "平均队列长"),
]

SYNC_FIELDS = [
    ("sync_send_count", "sync send 次数"),
    ("sync_bandwidth_mb", "sync 带宽 (MB)"),
    ("dsr", "DSR"),
    ("exp6_stage3_makespan_ms", "Stage3 makespan (ms)"),
    ("exp6_sync_per_ack_latency_median_ms", "sync/ack P50 (ms)"),
    ("exp6_sync_per_ack_latency_p95_ms", "sync/ack P95 (ms)"),
    ("migration_completion_ms", "迁移完成 (ms)"),
]

PROBE_FIELDS = [
    ("probe_ok", "probe_ok"),
    ("rdt_ratio", "RDT"),
]


def ensure_metrics(log_dir: Path, out_json: Path, repo: Path) -> dict:
    out_json.parent.mkdir(parents=True, exist_ok=True)
    if not out_json.exists() or out_json.stat().st_mtime < log_dir.stat().st_mtime:
        subprocess.run(
            [
                sys.executable,
                str(repo / "scripts" / "metrics_definitions.py"),
                "--log-dir",
                str(log_dir),
                "--out",
                str(out_json),
            ],
            cwd=repo,
            check=True,
        )
    return json.loads(out_json.read_text(encoding="utf-8"))


def pick(m: dict, key: str):
    if key == "probe_ok":
        return bool(m.get("probe_ok"))
    if key == "latency_p95":
        return m.get("latency_p95") or m.get("latency", {}).get("total_p95")
    if key == "latency_mean":
        return m.get("latency_mean") or m.get("latency", {}).get("total_mean")
    return m.get(key)


def mean_std(values: list[float]) -> tuple[float | None, float | None]:
    nums = [float(v) for v in values if v is not None and isinstance(v, (int, float))]
    if not nums:
        return None, None
    if len(nums) == 1:
        return nums[0], 0.0
    return statistics.mean(nums), statistics.pstdev(nums)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--repo", type=Path, default=Path("."))
    ap.add_argument("--probe", type=int, default=50)
    ap.add_argument("--out-dir", type=Path, default=Path("results/exp2_concurrency/summary"))
    args = ap.parse_args()
    repo = args.repo.resolve()
    probe = args.probe
    out_dir = (repo / args.out_dir).resolve()
    metrics_dir = repo / "results" / "exp2_concurrency" / "metrics"
    metrics_dir.mkdir(parents=True, exist_ok=True)

    arms = [
        (
            "MVSS",
            [
                repo
                / f"results/exp2_concurrency/raw/probe_{probe}/strategy_MVSS/shards4_nodes4/window_0/run{r}"
                for r in (1, 2, 3)
            ],
        ),
        (
            "MVSS-Delta",
            [
                repo
                / f"results/exp6_scale_4x4_full/raw/shards4_nodes4/window_200/run{r}"
                for r in (1, 2, 3)
            ],
        ),
    ]

    all_fields = GLOBAL_FIELDS + SYNC_FIELDS + PROBE_FIELDS
    rows: list[dict] = []

    for strategy, run_dirs in arms:
        per_run: list[dict] = []
        for i, log_dir in enumerate(run_dirs, 1):
            if not log_dir.is_dir():
                print(f"[WARN] missing {log_dir}", file=sys.stderr)
                continue
            tag = f"{strategy.replace('-', '')}_probe{probe}_run{i}"
            out_json = metrics_dir / f"{tag}.json"
            m = ensure_metrics(log_dir, out_json, repo)
            per_run.append(m)
            row = {"strategy": strategy, "run": i}
            for key, _ in all_fields:
                row[key] = pick(m, key)
            rows.append(row)

        if not per_run:
            continue

        summary = {"strategy": strategy, "run": "mean±std", "runs": len(per_run)}
        for key, _ in all_fields:
            if key == "probe_ok":
                ok = sum(1 for m in per_run if pick(m, key))
                summary[key] = f"{ok}/{len(per_run)}"
                continue
            mu, sd = mean_std([pick(m, key) for m in per_run])
            if mu is None:
                summary[key] = "—"
            elif sd == 0.0 or sd is None:
                summary[key] = f"{mu:.4g}"
            else:
                summary[key] = f"{mu:.4g}±{sd:.4g}"
        rows.append(summary)

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"exp2_probe{probe}_metrics.csv"
    md_path = out_dir / f"exp2_probe{probe}_metrics.md"

    headers = ["strategy", "run"] + [k for k, _ in all_fields]
    with csv_path.open("w", encoding="utf-8-sig", newline="") as f:
        f.write(",".join(headers) + "\n")
        for row in rows:
            f.write(",".join(str(row.get(h, "")) for h in headers) + "\n")

    lines = [
        f"# Exp2 指标汇总（4×4，probe={probe}，inject=24000）",
        "",
        "- MVSS：`results/exp2_concurrency/raw/probe_50/strategy_MVSS/`（W=0）",
        "- MVSS-Delta：复用 `results/exp6_scale_4x4_full/.../window_200/`（W=200）",
        "",
        "## 全局性能",
        "",
        "| 策略 | run | TPS 全局 | TPS 迁移窗 | 时延 P95 | 提交 tx | Relay 比例 |",
        "|------|-----|----------|------------|----------|---------|------------|",
    ]
    for row in rows:
        lines.append(
            f"| {row['strategy']} | {row['run']} | "
            f"{row.get('tps_global', '')} | {row.get('tps_migration', '')} | "
            f"{row.get('latency_p95', '')} | {row.get('tx_committed_total', '')} | "
            f"{row.get('relay_ratio', '')} |"
        )

    lines += [
        "",
        "## 同步 / Stage3 / 正确性",
        "",
        "| 策略 | run | sync_send | 带宽 MB | DSR | Stage3 ms | probe_ok | RDT |",
        "|------|-----|-----------|---------|-----|-----------|----------|-----|",
    ]
    for row in rows:
        lines.append(
            f"| {row['strategy']} | {row['run']} | "
            f"{row.get('sync_send_count', '')} | {row.get('sync_bandwidth_mb', '')} | "
            f"{row.get('dsr', '')} | {row.get('exp6_stage3_makespan_ms', '')} | "
            f"{row.get('probe_ok', '')} | {row.get('rdt_ratio', '')} |"
        )

    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"[done] {csv_path}")
    print(f"[done] {md_path}")


if __name__ == "__main__":
    main()
