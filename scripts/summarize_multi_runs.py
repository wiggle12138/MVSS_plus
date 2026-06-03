#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多次运行汇总脚本（按策略目录 + run 目录组织）。

期望输入目录结构:
root_dir/
  SOTA-Lock/
    run1/
      S0_block.csv
      S0_transaction.csv
      S0_queueLen.csv
      S1_block.csv
      ...
    run2/
    ...
  Fine-Tune-Lock/   (或 Fine-tuned-Lock / Fine-Tuned-Lock)
    run1/
    ...

说明:
1) 当前汇总仅依赖以下 CSV:
   - S*_block.csv
   - S*_transaction.csv
   - S*_queueLen.csv
2) 其它 CSV (S*_mig1.csv/S*_mig2.csv/S*_sync.csv/migration.csv 等) 暂不参与统计。

输出:
out_dir/
  run_metrics.csv          # 每个 run 一行
  strategy_summary.csv     # 每个策略一行（均值/标准差）
  strategy_summary.md      # 可直接贴报告的简要表
"""

from __future__ import annotations

import argparse
import csv
import statistics
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from analyze_logs import (
    ShardMetrics,
    analyze_shard,
    collect_shards,
    read_csv_rows,
    to_int,
    to_float,
    to_bool,
    percentile,
)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, header: List[str], rows: Iterable[List[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


def safe_mean(vals: List[float]) -> float:
    return statistics.mean(vals) if vals else 0.0


def safe_stdev(vals: List[float]) -> float:
    # 样本数小于2时标准差定义为0，避免抛异常。
    return statistics.stdev(vals) if len(vals) >= 2 else 0.0


def discover_strategy_dirs(root_dir: Path) -> List[Path]:
    return sorted([p for p in root_dir.iterdir() if p.is_dir()])


def discover_run_dirs(strategy_dir: Path) -> List[Path]:
    runs = [p for p in strategy_dir.iterdir() if p.is_dir()]
    # 约定 run1/run2...，同时允许其它命名，只要目录存在。
    return sorted(runs, key=lambda p: p.name.lower())


def compute_run_metrics(log_dir: Path) -> Dict[str, float]:
    shards = collect_shards(log_dir)
    if not shards:
        raise ValueError(f"目录缺少 S*_block.csv: {log_dir}")

    per_shard: List[ShardMetrics] = []
    for shard in shards:
        m, _ = analyze_shard(log_dir, shard)
        per_shard.append(m)

    block_count_sum = sum(m.block_count for m in per_shard)
    active_block_sum = sum(m.active_block_count for m in per_shard)
    tx_total_sum = sum(m.total_tx_total for m in per_shard)
    tx_committed_sum = sum(m.total_tx_committed for m in per_shard)
    relay_sum = sum(m.total_relay_tx for m in per_shard)
    locked_sum = sum(m.total_locked_txs for m in per_shard)
    migration_block_sum = sum(m.migration_block_count for m in per_shard)
    queue_auc_sum = sum(m.queue_len_auc for m in per_shard)
    max_queue_global = max((m.max_queue_len for m in per_shard), default=0)

    # 汇总时延按“交易条数加权平均”，避免简单均值偏置。
    lat_base = sum(m.tx_count_for_latency for m in per_shard)
    if lat_base > 0:
        latency_mean = sum(m.latency_mean * m.tx_count_for_latency for m in per_shard) / lat_base
        latency_p50 = sum(m.latency_p50 * m.tx_count_for_latency for m in per_shard) / lat_base
        latency_p95 = sum(m.latency_p95 * m.tx_count_for_latency for m in per_shard) / lat_base
        latency_p99 = sum(m.latency_p99 * m.tx_count_for_latency for m in per_shard) / lat_base
    else:
        latency_mean = latency_p50 = latency_p95 = latency_p99 = 0.0

    # 队列均值按 “queue auc / 总区块数” 估计。
    avg_queue_len_global = (queue_auc_sum / block_count_sum) if block_count_sum > 0 else 0.0

    # ---------------------------
    # 迁移相关子集 + 迁移窗口期指标
    # ---------------------------
    mig_related_latencies: List[float] = []
    window_latencies: List[float] = []
    window_queue_vals: List[int] = []
    window_tx_total_sum = 0
    window_locked_sum = 0
    window_found_shards = 0

    def is_migration_related_tx(row: Dict[str, str]) -> bool:
        # 锁标记是最稳妥依据；其次用迁移时间/锁时间辅助判断。
        if (
            to_bool(row.get("SenLock", "false"))
            or to_bool(row.get("RecLock", "false"))
            or to_bool(row.get("HalfLock", "false"))
            or to_bool(row.get("RelayLock", "false"))
        ):
            return True
        txmig1 = to_float(row.get("TXmig1_time", row.get("TXmig1_Time", "0")))
        txmig2 = to_float(row.get("TXmig2_Time", "0"))
        lock_t = to_float(row.get("lock", "0"))
        unlock_t = to_float(row.get("unlock", "0"))
        return (txmig1 > 0) or (txmig2 > 0) or (lock_t > 0) or (unlock_t > 0)

    for shard in shards:
        block_rows = read_csv_rows(log_dir / f"{shard}_block.csv")
        tx_rows = read_csv_rows(log_dir / f"{shard}_transaction.csv")
        queue_rows = read_csv_rows(log_dir / f"{shard}_queueLen.csv")

        mig_blocks: List[int] = []
        for r in block_rows:
            h = to_int(r.get("blockHeight", "0"))
            mig_total = (
                to_int(r.get("mig1", "0"))
                + to_int(r.get("mig2", "0"))
                + to_int(r.get("ann", "0"))
                + to_int(r.get("ns", "0"))
            )
            if mig_total > 0:
                mig_blocks.append(h)

        if mig_blocks:
            window_found_shards += 1
            w_start = min(mig_blocks)
            w_end = max(mig_blocks)

            for r in block_rows:
                h = to_int(r.get("blockHeight", "0"))
                if w_start <= h <= w_end:
                    window_tx_total_sum += to_int(r.get("tx_total", "0"))
                    window_locked_sum += to_int(r.get("locked_txs", "0"))

            for r in queue_rows:
                b = to_int(r.get("block", "0"))
                if w_start <= b <= w_end:
                    window_queue_vals.append(to_int(r.get("queueLen", "0")))

            for r in tx_rows:
                h = to_int(r.get("blockHeight", "0"))
                v = to_float(r.get("commit-request", "0"))
                if v < 0:
                    continue
                if w_start <= h <= w_end:
                    window_latencies.append(v)
                if is_migration_related_tx(r):
                    mig_related_latencies.append(v)
        else:
            # 没有窗口时，迁移相关子集仍可按标签统计
            for r in tx_rows:
                v = to_float(r.get("commit-request", "0"))
                if v >= 0 and is_migration_related_tx(r):
                    mig_related_latencies.append(v)

    migration_window_locked_ratio = (
        (window_locked_sum / window_tx_total_sum) if window_tx_total_sum > 0 else 0.0
    )
    migration_window_avg_queue = safe_mean([float(x) for x in window_queue_vals])
    migration_window_max_queue = float(max(window_queue_vals)) if window_queue_vals else 0.0
    mig_related_latency_mean = safe_mean(mig_related_latencies)
    mig_related_latency_p95 = percentile(mig_related_latencies, 95)
    mig_related_latency_p99 = percentile(mig_related_latencies, 99)
    window_latency_mean = safe_mean(window_latencies)
    window_latency_p95 = percentile(window_latencies, 95)
    window_latency_p99 = percentile(window_latencies, 99)

    return {
        "shard_count": float(len(per_shard)),
        "block_count": float(block_count_sum),
        "active_block_count": float(active_block_sum),
        "tx_total": float(tx_total_sum),
        "tx_committed": float(tx_committed_sum),
        "avg_committed_per_active_block": (tx_committed_sum / active_block_sum) if active_block_sum > 0 else 0.0,
        "relay_ratio": (relay_sum / tx_total_sum) if tx_total_sum > 0 else 0.0,
        "locked_ratio": (locked_sum / tx_total_sum) if tx_total_sum > 0 else 0.0,
        "migration_block_count": float(migration_block_sum),
        "avg_queue_len_global": avg_queue_len_global,
        "max_queue_len_global": float(max_queue_global),
        "latency_mean": latency_mean,
        "latency_p50": latency_p50,
        "latency_p95": latency_p95,
        "latency_p99": latency_p99,
        "mig_related_tx_count": float(len(mig_related_latencies)),
        "mig_related_latency_mean": mig_related_latency_mean,
        "mig_related_latency_p95": mig_related_latency_p95,
        "mig_related_latency_p99": mig_related_latency_p99,
        "migration_window_shard_count": float(window_found_shards),
        "migration_window_tx_total": float(window_tx_total_sum),
        "migration_window_locked_ratio": migration_window_locked_ratio,
        "migration_window_avg_queue_len": migration_window_avg_queue,
        "migration_window_max_queue_len": migration_window_max_queue,
        "migration_window_latency_mean": window_latency_mean,
        "migration_window_latency_p95": window_latency_p95,
        "migration_window_latency_p99": window_latency_p99,
    }


def write_markdown_summary(path: Path, summary_rows: List[Dict[str, float]]) -> None:
    lines: List[str] = []
    lines.append("# 多次运行汇总")
    lines.append("")
    lines.append("| strategy | runs | latency_mean_mean | mig_related_latency_mean_mean | migration_window_latency_mean_mean | locked_ratio_mean | migration_window_locked_ratio_mean |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for row in summary_rows:
        lines.append(
            f"| {row['strategy']} | {int(row['run_count'])} | "
            f"{row['latency_mean_mean']:.3f} | {row['mig_related_latency_mean_mean']:.3f} | "
            f"{row['migration_window_latency_mean_mean']:.3f} | {row['locked_ratio_mean']:.4f} | "
            f"{row['migration_window_locked_ratio_mean']:.4f} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="汇总策略目录下多次运行日志。")
    parser.add_argument(
        "--root-dir",
        default="results/raw",
        help="根目录，包含各策略文件夹（默认: results/raw）",
    )
    parser.add_argument(
        "--out-dir",
        default="results/multi_run_summary",
        help="输出目录（默认: results/multi_run_summary）",
    )
    args = parser.parse_args()

    root_dir = Path(args.root_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    ensure_dir(out_dir)

    if not root_dir.exists():
        raise SystemExit(f"root_dir 不存在: {root_dir}")

    strategy_dirs = discover_strategy_dirs(root_dir)
    if not strategy_dirs:
        raise SystemExit(f"root_dir 下未找到策略目录: {root_dir}")

    run_records: List[Dict[str, float]] = []
    summary_rows: List[Dict[str, float]] = []

    for strategy_dir in strategy_dirs:
        strategy_name = strategy_dir.name
        run_dirs = discover_run_dirs(strategy_dir)
        if not run_dirs:
            print(f"[warn] 跳过空策略目录: {strategy_dir}")
            continue

        strategy_run_rows: List[Dict[str, float]] = []
        for run_dir in run_dirs:
            try:
                m = compute_run_metrics(run_dir)
            except Exception as e:
                print(f"[warn] 跳过 run 目录 {run_dir.name}: {e}")
                continue

            rec = {"strategy": strategy_name, "run": run_dir.name}
            rec.update(m)
            run_records.append(rec)
            strategy_run_rows.append(rec)

        if not strategy_run_rows:
            print(f"[warn] 策略目录无有效 run: {strategy_dir}")
            continue

        def col(name: str) -> List[float]:
            return [float(r[name]) for r in strategy_run_rows]

        summary_rows.append(
            {
                "strategy": strategy_name,
                "run_count": float(len(strategy_run_rows)),
                "tx_committed_mean": safe_mean(col("tx_committed")),
                "tx_committed_std": safe_stdev(col("tx_committed")),
                "avg_committed_per_active_block_mean": safe_mean(col("avg_committed_per_active_block")),
                "avg_committed_per_active_block_std": safe_stdev(col("avg_committed_per_active_block")),
                "relay_ratio_mean": safe_mean(col("relay_ratio")),
                "relay_ratio_std": safe_stdev(col("relay_ratio")),
                "locked_ratio_mean": safe_mean(col("locked_ratio")),
                "locked_ratio_std": safe_stdev(col("locked_ratio")),
                "migration_block_count_mean": safe_mean(col("migration_block_count")),
                "migration_block_count_std": safe_stdev(col("migration_block_count")),
                "avg_queue_len_global_mean": safe_mean(col("avg_queue_len_global")),
                "avg_queue_len_global_std": safe_stdev(col("avg_queue_len_global")),
                "max_queue_len_global_mean": safe_mean(col("max_queue_len_global")),
                "max_queue_len_global_std": safe_stdev(col("max_queue_len_global")),
                "latency_mean_mean": safe_mean(col("latency_mean")),
                "latency_mean_std": safe_stdev(col("latency_mean")),
                "latency_p95_mean": safe_mean(col("latency_p95")),
                "latency_p95_std": safe_stdev(col("latency_p95")),
                "latency_p99_mean": safe_mean(col("latency_p99")),
                "latency_p99_std": safe_stdev(col("latency_p99")),
                "mig_related_tx_count_mean": safe_mean(col("mig_related_tx_count")),
                "mig_related_tx_count_std": safe_stdev(col("mig_related_tx_count")),
                "mig_related_latency_mean_mean": safe_mean(col("mig_related_latency_mean")),
                "mig_related_latency_mean_std": safe_stdev(col("mig_related_latency_mean")),
                "mig_related_latency_p95_mean": safe_mean(col("mig_related_latency_p95")),
                "mig_related_latency_p95_std": safe_stdev(col("mig_related_latency_p95")),
                "mig_related_latency_p99_mean": safe_mean(col("mig_related_latency_p99")),
                "mig_related_latency_p99_std": safe_stdev(col("mig_related_latency_p99")),
                "migration_window_locked_ratio_mean": safe_mean(col("migration_window_locked_ratio")),
                "migration_window_locked_ratio_std": safe_stdev(col("migration_window_locked_ratio")),
                "migration_window_avg_queue_len_mean": safe_mean(col("migration_window_avg_queue_len")),
                "migration_window_avg_queue_len_std": safe_stdev(col("migration_window_avg_queue_len")),
                "migration_window_latency_mean_mean": safe_mean(col("migration_window_latency_mean")),
                "migration_window_latency_mean_std": safe_stdev(col("migration_window_latency_mean")),
                "migration_window_latency_p95_mean": safe_mean(col("migration_window_latency_p95")),
                "migration_window_latency_p95_std": safe_stdev(col("migration_window_latency_p95")),
                "migration_window_latency_p99_mean": safe_mean(col("migration_window_latency_p99")),
                "migration_window_latency_p99_std": safe_stdev(col("migration_window_latency_p99")),
            }
        )

    if not run_records:
        raise SystemExit("未收集到有效 run 指标，请检查目录与 CSV。")

    run_header = [
        "strategy",
        "run",
        "shard_count",
        "block_count",
        "active_block_count",
        "tx_total",
        "tx_committed",
        "avg_committed_per_active_block",
        "relay_ratio",
        "locked_ratio",
        "migration_block_count",
        "avg_queue_len_global",
        "max_queue_len_global",
        "latency_mean",
        "latency_p50",
        "latency_p95",
        "latency_p99",
        "mig_related_tx_count",
        "mig_related_latency_mean",
        "mig_related_latency_p95",
        "mig_related_latency_p99",
        "migration_window_shard_count",
        "migration_window_tx_total",
        "migration_window_locked_ratio",
        "migration_window_avg_queue_len",
        "migration_window_max_queue_len",
        "migration_window_latency_mean",
        "migration_window_latency_p95",
        "migration_window_latency_p99",
    ]
    run_rows = [[r.get(k, "") for k in run_header] for r in run_records]
    write_csv(out_dir / "run_metrics.csv", run_header, run_rows)

    summary_header = [
        "strategy",
        "run_count",
        "tx_committed_mean",
        "tx_committed_std",
        "avg_committed_per_active_block_mean",
        "avg_committed_per_active_block_std",
        "relay_ratio_mean",
        "relay_ratio_std",
        "locked_ratio_mean",
        "locked_ratio_std",
        "migration_block_count_mean",
        "migration_block_count_std",
        "avg_queue_len_global_mean",
        "avg_queue_len_global_std",
        "max_queue_len_global_mean",
        "max_queue_len_global_std",
        "latency_mean_mean",
        "latency_mean_std",
        "latency_p95_mean",
        "latency_p95_std",
        "latency_p99_mean",
        "latency_p99_std",
        "mig_related_tx_count_mean",
        "mig_related_tx_count_std",
        "mig_related_latency_mean_mean",
        "mig_related_latency_mean_std",
        "mig_related_latency_p95_mean",
        "mig_related_latency_p95_std",
        "mig_related_latency_p99_mean",
        "mig_related_latency_p99_std",
        "migration_window_locked_ratio_mean",
        "migration_window_locked_ratio_std",
        "migration_window_avg_queue_len_mean",
        "migration_window_avg_queue_len_std",
        "migration_window_latency_mean_mean",
        "migration_window_latency_mean_std",
        "migration_window_latency_p95_mean",
        "migration_window_latency_p95_std",
        "migration_window_latency_p99_mean",
        "migration_window_latency_p99_std",
    ]
    summary_csv_rows = [[r.get(k, "") for k in summary_header] for r in summary_rows]
    write_csv(out_dir / "strategy_summary.csv", summary_header, summary_csv_rows)
    write_markdown_summary(out_dir / "strategy_summary.md", summary_rows)

    print(f"[done] root_dir={root_dir}")
    print(f"[done] out_dir={out_dir}")
    print(f"[done] runs={len(run_records)}, strategies={len(summary_rows)}")


if __name__ == "__main__":
    main()

