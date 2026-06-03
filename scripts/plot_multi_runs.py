#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
多次运行结果绘图脚本。

输入:
  - strategy_summary.csv  (来自 summarize_multi_runs.py)
  - run_metrics.csv       (可选，用于补充散点/箱线信息)

输出:
  - figures/*.png
  - figures_manifest.md
"""

from __future__ import annotations

import argparse
import csv
from pathlib import Path
from typing import Dict, List, Tuple

from analyze_logs import try_load_matplotlib


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def to_float(v: str, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_strategy_name(name: str) -> str:
    """
    统一策略命名，便于图例和颜色映射。
    """
    s = name.strip().lower().replace("_", "-").replace(" ", "-")
    if s in {"fine-tune-lock", "fine-tuned-lock", "finetuned-lock", "finetune-lock"}:
        return "Fine-Tune-Lock"
    if s in {"sota-lock", "sotalock"}:
        return "SOTA-Lock"
    return name.strip()


def strategy_color(name: str) -> str:
    """
    固定颜色映射，保持全文图风一致。
    """
    n = normalize_strategy_name(name)
    if n == "SOTA-Lock":
        return "#1f77b4"  # blue
    if n == "Fine-Tune-Lock":
        return "#ff7f0e"  # orange
    return "#2ca02c"      # green fallback


def draw_bar_with_error(
    plt,
    labels: List[str],
    means: List[float],
    stds: List[float],
    title: str,
    ylabel: str,
    out_file: Path,
) -> None:
    colors = [strategy_color(x) for x in labels]
    x = list(range(len(labels)))
    plt.figure(figsize=(8, 5))
    bars = plt.bar(
        x,
        means,
        yerr=stds,
        capsize=6,
        color=colors,
        edgecolor="#333333",
        linewidth=0.8,
        alpha=0.92,
    )
    plt.xticks(x, labels)
    plt.ylabel(ylabel)
    plt.title(title)
    plt.grid(axis="y", alpha=0.25, linestyle="--")
    # 顶部数值标签
    for b, v in zip(bars, means):
        plt.text(
            b.get_x() + b.get_width() / 2.0,
            b.get_height(),
            f"{v:.3f}",
            ha="center",
            va="bottom",
            fontsize=9,
        )
    plt.tight_layout()
    plt.savefig(out_file, dpi=220)
    plt.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="根据多次运行汇总 CSV 绘制对比图。")
    parser.add_argument(
        "--summary-csv",
        default="results/multi_run_summary/strategy_summary.csv",
        help="策略汇总CSV路径（默认: results/multi_run_summary/strategy_summary.csv）",
    )
    parser.add_argument(
        "--run-csv",
        default="results/multi_run_summary/run_metrics.csv",
        help="逐run指标CSV路径（当前主要用于完整性检查，默认: results/multi_run_summary/run_metrics.csv）",
    )
    parser.add_argument(
        "--out-dir",
        default="results/multi_run_summary/figures",
        help="图像输出目录（默认: results/multi_run_summary/figures）",
    )
    args = parser.parse_args()

    plt = try_load_matplotlib()
    if plt is None:
        raise SystemExit("当前环境 matplotlib 不可用，无法绘图。请先修复绘图库环境。")

    summary_csv = Path(args.summary_csv).resolve()
    run_csv = Path(args.run_csv).resolve()
    out_dir = Path(args.out_dir).resolve()
    ensure_dir(out_dir)

    summary_rows = read_csv_rows(summary_csv)
    if not summary_rows:
        raise SystemExit(f"未读取到 summary 数据: {summary_csv}")

    # run_csv 当前非强依赖，但先检查是否存在，便于用户排查流程问题
    if not run_csv.exists():
        print(f"[warn] 未找到 run_csv: {run_csv}（不影响当前汇总图绘制）")

    # 指标配置: (mean列, std列, 标题, y轴标签, 输出文件名)
    metrics: List[Tuple[str, str, str, str, str]] = [
        ("latency_mean_mean", "latency_mean_std", "Mean Latency Comparison", "Latency (commit-request)", "fig01_latency_mean.png"),
        ("latency_p95_mean", "latency_p95_std", "P95 Latency Comparison", "Latency P95", "fig02_latency_p95.png"),
        ("latency_p99_mean", "latency_p99_std", "P99 Latency Comparison", "Latency P99", "fig03_latency_p99.png"),
        ("locked_ratio_mean", "locked_ratio_std", "Locked Ratio Comparison", "Locked Ratio", "fig04_locked_ratio.png"),
        (
            "avg_committed_per_active_block_mean",
            "avg_committed_per_active_block_std",
            "Throughput Proxy Comparison",
            "Avg tx_committed per active block",
            "fig05_avg_committed_per_active_block.png",
        ),
        ("avg_queue_len_global_mean", "avg_queue_len_global_std", "Average Queue Length Comparison", "Average Queue Length", "fig06_avg_queue_len.png"),
        ("max_queue_len_global_mean", "max_queue_len_global_std", "Max Queue Length Comparison", "Max Queue Length", "fig07_max_queue_len.png"),
        ("relay_ratio_mean", "relay_ratio_std", "Relay Ratio Comparison", "Relay Ratio", "fig08_relay_ratio.png"),
        ("tx_committed_mean", "tx_committed_std", "Total Committed TX Comparison", "Total tx_committed", "fig09_tx_committed.png"),
        (
            "mig_related_latency_mean_mean",
            "mig_related_latency_mean_std",
            "Migration-related Mean Latency Comparison",
            "Latency (migration-related tx subset)",
            "fig10_mig_related_latency_mean.png",
        ),
        (
            "migration_window_latency_mean_mean",
            "migration_window_latency_mean_std",
            "Migration-window Mean Latency Comparison",
            "Latency (migration-window blocks)",
            "fig11_migration_window_latency_mean.png",
        ),
        (
            "migration_window_locked_ratio_mean",
            "migration_window_locked_ratio_std",
            "Migration-window Locked Ratio Comparison",
            "Locked Ratio (migration-window blocks)",
            "fig12_migration_window_locked_ratio.png",
        ),
    ]

    # 统一策略名，保持可读
    for r in summary_rows:
        r["strategy"] = normalize_strategy_name(r.get("strategy", "Unknown"))
    summary_rows = sorted(summary_rows, key=lambda x: x["strategy"])
    labels = [r["strategy"] for r in summary_rows]

    manifest_lines: List[str] = []
    manifest_lines.append("# Figures Manifest")
    manifest_lines.append("")
    manifest_lines.append("| File | Description |")
    manifest_lines.append("|---|---|")

    for mean_col, std_col, title, ylabel, filename in metrics:
        means = [to_float(r.get(mean_col, "0")) for r in summary_rows]
        stds = [to_float(r.get(std_col, "0")) for r in summary_rows]
        out_file = out_dir / filename
        draw_bar_with_error(
            plt=plt,
            labels=labels,
            means=means,
            stds=stds,
            title=title,
            ylabel=ylabel,
            out_file=out_file,
        )
        manifest_lines.append(f"| `{filename}` | {title} |")

    manifest_path = out_dir / "figures_manifest.md"
    manifest_path.write_text("\n".join(manifest_lines), encoding="utf-8")

    print(f"[done] summary_csv={summary_csv}")
    print(f"[done] out_dir={out_dir}")
    print(f"[done] figures={len(metrics)}")


if __name__ == "__main__":
    main()

