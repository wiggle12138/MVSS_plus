#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
基于当前运行的 log CSV 生成指标表格与图像。

默认输入: ./log
默认输出: ./results/current_run

输出内容:
1) 表格
   - metrics_per_shard.csv
   - metrics_overall.csv
   - metrics_summary.md
2) 图像
   - tx_committed_by_block.png
   - queue_len_by_block.png
   - locked_txs_by_block.png
   - latency_cdf.png
"""

from __future__ import annotations

import argparse
import csv
import io
import math
import statistics
import contextlib
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

plt = None


def try_load_matplotlib():
    """
    按需加载 matplotlib。
    某些环境下 numpy/matplotlib 版本不匹配会在 import 时打印大量报错，
    这里吞掉导入期输出，仅通过返回值判断是否可绘图。
    """
    global plt
    if plt is not None:
        return plt
    buf_out = io.StringIO()
    buf_err = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf_out), contextlib.redirect_stderr(buf_err):
            import matplotlib.pyplot as _plt  # type: ignore
        plt = _plt
    except Exception:  # pragma: no cover
        plt = None
    return plt


def to_int(v: str, default: int = 0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def to_float(v: str, default: float = 0.0) -> float:
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def to_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def percentile(values: List[float], p: float) -> float:
    if not values:
        return 0.0
    if p <= 0:
        return float(min(values))
    if p >= 100:
        return float(max(values))
    data = sorted(values)
    k = (len(data) - 1) * (p / 100.0)
    lo = math.floor(k)
    hi = math.ceil(k)
    if lo == hi:
        return float(data[lo])
    w = k - lo
    return float(data[lo] * (1 - w) + data[hi] * w)


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_csv(path: Path, header: List[str], rows: Iterable[List[object]]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)


@dataclass
class ShardMetrics:
    shard: str
    block_count: int
    active_block_count: int
    total_tx_total: int
    total_tx_committed: int
    avg_tx_committed_per_active_block: float
    total_relay_tx: int
    relay_ratio: float
    total_locked_txs: int
    locked_ratio: float
    migration_block_count: int
    avg_queue_len: float
    max_queue_len: int
    queue_len_auc: int
    tx_count_for_latency: int
    latency_mean: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    senlock_ratio: float
    reclock_ratio: float
    halflock_ratio: float
    relaylock_ratio: float


def collect_shards(log_dir: Path) -> List[str]:
    shards = []
    for p in sorted(log_dir.glob("S*_block.csv")):
        name = p.name
        shard = name.split("_")[0]
        shards.append(shard)
    return sorted(set(shards))


def analyze_shard(log_dir: Path, shard: str) -> Tuple[ShardMetrics, Dict[str, List[float]]]:
    block_rows = read_csv_rows(log_dir / f"{shard}_block.csv")
    tx_rows = read_csv_rows(log_dir / f"{shard}_transaction.csv")
    queue_rows = read_csv_rows(log_dir / f"{shard}_queueLen.csv")

    block_count = len(block_rows)
    tx_total_sum = 0
    tx_committed_sum = 0
    relay_sum = 0
    locked_sum = 0
    active_blocks = 0
    migration_blocks = 0

    block_heights: List[float] = []
    committed_series: List[float] = []
    locked_series: List[float] = []

    for r in block_rows:
        h = to_int(r.get("blockHeight", "0"))
        tx_total = to_int(r.get("tx_total", "0"))
        tx_committed = to_int(r.get("tx_committed", "0"))
        relay_sender = to_int(r.get("tx_relay_sender", "0"))
        relay_receiver = to_int(r.get("tx_relay_receiver", "0"))
        locked = to_int(r.get("locked_txs", "0"))
        mig_total = (
            to_int(r.get("mig1", "0"))
            + to_int(r.get("mig2", "0"))
            + to_int(r.get("ann", "0"))
            + to_int(r.get("ns", "0"))
        )
        if tx_total > 0:
            active_blocks += 1
        if mig_total > 0:
            migration_blocks += 1

        tx_total_sum += tx_total
        tx_committed_sum += tx_committed
        relay_sum += relay_sender + relay_receiver
        locked_sum += locked

        block_heights.append(float(h))
        committed_series.append(float(tx_committed))
        locked_series.append(float(locked))

    queue_series_x: List[float] = []
    queue_series_y: List[float] = []
    queue_vals: List[int] = []
    for r in queue_rows:
        b = to_int(r.get("block", "0"))
        q = to_int(r.get("queueLen", "0"))
        queue_series_x.append(float(b))
        queue_series_y.append(float(q))
        queue_vals.append(q)

    latencies: List[float] = []
    senlock_cnt = 0
    reclock_cnt = 0
    halflock_cnt = 0
    relaylock_cnt = 0
    tx_latency_base = 0
    for r in tx_rows:
        v = to_float(r.get("commit-request", "0"))
        if v >= 0:
            latencies.append(v)
        tx_latency_base += 1
        senlock_cnt += 1 if to_bool(r.get("SenLock", "false")) else 0
        reclock_cnt += 1 if to_bool(r.get("RecLock", "false")) else 0
        halflock_cnt += 1 if to_bool(r.get("HalfLock", "false")) else 0
        relaylock_cnt += 1 if to_bool(r.get("RelayLock", "false")) else 0

    avg_committed = (
        (tx_committed_sum / active_blocks) if active_blocks > 0 else 0.0
    )
    relay_ratio = (relay_sum / tx_total_sum) if tx_total_sum > 0 else 0.0
    locked_ratio = (locked_sum / tx_total_sum) if tx_total_sum > 0 else 0.0

    avg_queue = statistics.mean(queue_vals) if queue_vals else 0.0
    max_queue = max(queue_vals) if queue_vals else 0
    queue_auc = sum(queue_vals)

    lat_mean = statistics.mean(latencies) if latencies else 0.0
    lat_p50 = percentile(latencies, 50)
    lat_p95 = percentile(latencies, 95)
    lat_p99 = percentile(latencies, 99)

    def ratio(cnt: int, base: int) -> float:
        return cnt / base if base > 0 else 0.0

    m = ShardMetrics(
        shard=shard,
        block_count=block_count,
        active_block_count=active_blocks,
        total_tx_total=tx_total_sum,
        total_tx_committed=tx_committed_sum,
        avg_tx_committed_per_active_block=avg_committed,
        total_relay_tx=relay_sum,
        relay_ratio=relay_ratio,
        total_locked_txs=locked_sum,
        locked_ratio=locked_ratio,
        migration_block_count=migration_blocks,
        avg_queue_len=avg_queue,
        max_queue_len=max_queue,
        queue_len_auc=queue_auc,
        tx_count_for_latency=len(latencies),
        latency_mean=lat_mean,
        latency_p50=lat_p50,
        latency_p95=lat_p95,
        latency_p99=lat_p99,
        senlock_ratio=ratio(senlock_cnt, tx_latency_base),
        reclock_ratio=ratio(reclock_cnt, tx_latency_base),
        halflock_ratio=ratio(halflock_cnt, tx_latency_base),
        relaylock_ratio=ratio(relaylock_cnt, tx_latency_base),
    )

    series = {
        "block_x": block_heights,
        "committed_y": committed_series,
        "locked_y": locked_series,
        "queue_x": queue_series_x,
        "queue_y": queue_series_y,
        "latencies": latencies,
    }
    return m, series


def plot_lines(
    series_map: Dict[str, Dict[str, List[float]]],
    out_file: Path,
    x_key: str,
    y_key: str,
    title: str,
    xlabel: str,
    ylabel: str,
) -> None:
    if try_load_matplotlib() is None:
        return
    plt.figure(figsize=(9, 5))
    for shard, s in sorted(series_map.items()):
        if not s[x_key] or not s[y_key]:
            continue
        plt.plot(s[x_key], s[y_key], marker="o", linewidth=1.2, markersize=2.5, label=shard)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_file, dpi=160)
    plt.close()


def plot_latency_cdf(series_map: Dict[str, Dict[str, List[float]]], out_file: Path) -> None:
    if try_load_matplotlib() is None:
        return
    plt.figure(figsize=(9, 5))
    for shard, s in sorted(series_map.items()):
        lat = sorted(s["latencies"])
        if not lat:
            continue
        y = [(i + 1) / len(lat) for i in range(len(lat))]
        plt.plot(lat, y, linewidth=1.4, label=shard)
    plt.title("Latency CDF (commit-request)")
    plt.xlabel("Latency")
    plt.ylabel("CDF")
    plt.legend()
    plt.grid(alpha=0.3)
    plt.tight_layout()
    plt.savefig(out_file, dpi=160)
    plt.close()


def write_markdown_summary(path: Path, per_shard: List[ShardMetrics], overall: Dict[str, float]) -> None:
    lines: List[str] = []
    lines.append("# 日志分析摘要")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- 总分片数: {int(overall['shard_count'])}")
    lines.append(f"- 总区块数: {int(overall['block_count'])}")
    lines.append(f"- 总提交交易数: {int(overall['tx_committed'])}")
    lines.append(f"- 平均每活跃块提交交易: {overall['avg_committed_per_active_block']:.3f}")
    lines.append(f"- relay 占比: {overall['relay_ratio']:.4f}")
    lines.append(f"- locked_txs 占比: {overall['locked_ratio']:.4f}")
    lines.append(f"- 平均时延(commit-request): {overall['latency_mean']:.3f}")
    lines.append(f"- P95 时延: {overall['latency_p95']:.3f}")
    lines.append("")
    lines.append("## Per Shard")
    lines.append("")
    lines.append("| shard | tx_committed | relay_ratio | locked_ratio | avg_queue_len | max_queue_len | latency_mean | latency_p95 |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|")
    for m in per_shard:
        lines.append(
            f"| {m.shard} | {m.total_tx_committed} | {m.relay_ratio:.4f} | {m.locked_ratio:.4f} | "
            f"{m.avg_queue_len:.2f} | {m.max_queue_len} | {m.latency_mean:.3f} | {m.latency_p95:.3f} |"
        )
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="分析 log 目录 CSV 并生成图表与表格。")
    parser.add_argument("--log-dir", default="log", help="日志目录，默认 log")
    parser.add_argument("--out-dir", default="results/current_run", help="输出目录，默认 results/current_run")
    args = parser.parse_args()

    log_dir = Path(args.log_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    table_dir = out_dir / "tables"
    fig_dir = out_dir / "figures"
    ensure_dir(table_dir)
    ensure_dir(fig_dir)

    shards = collect_shards(log_dir)
    if not shards:
        raise SystemExit(f"未找到分片 block 日志，请检查目录: {log_dir}")

    per_shard: List[ShardMetrics] = []
    series_map: Dict[str, Dict[str, List[float]]] = {}
    all_lat: List[float] = []

    for shard in shards:
        m, series = analyze_shard(log_dir, shard)
        per_shard.append(m)
        series_map[shard] = series
        all_lat.extend(series["latencies"])

    block_count_sum = sum(m.block_count for m in per_shard)
    active_block_sum = sum(m.active_block_count for m in per_shard)
    tx_total_sum = sum(m.total_tx_total for m in per_shard)
    tx_committed_sum = sum(m.total_tx_committed for m in per_shard)
    relay_sum = sum(m.total_relay_tx for m in per_shard)
    locked_sum = sum(m.total_locked_txs for m in per_shard)

    overall = {
        "shard_count": float(len(per_shard)),
        "block_count": float(block_count_sum),
        "active_block_count": float(active_block_sum),
        "tx_total": float(tx_total_sum),
        "tx_committed": float(tx_committed_sum),
        "avg_committed_per_active_block": (tx_committed_sum / active_block_sum) if active_block_sum > 0 else 0.0,
        "relay_ratio": (relay_sum / tx_total_sum) if tx_total_sum > 0 else 0.0,
        "locked_ratio": (locked_sum / tx_total_sum) if tx_total_sum > 0 else 0.0,
        "latency_mean": statistics.mean(all_lat) if all_lat else 0.0,
        "latency_p50": percentile(all_lat, 50),
        "latency_p95": percentile(all_lat, 95),
        "latency_p99": percentile(all_lat, 99),
    }

    per_shard_header = [
        "shard",
        "block_count",
        "active_block_count",
        "total_tx_total",
        "total_tx_committed",
        "avg_tx_committed_per_active_block",
        "total_relay_tx",
        "relay_ratio",
        "total_locked_txs",
        "locked_ratio",
        "migration_block_count",
        "avg_queue_len",
        "max_queue_len",
        "queue_len_auc",
        "tx_count_for_latency",
        "latency_mean",
        "latency_p50",
        "latency_p95",
        "latency_p99",
        "senlock_ratio",
        "reclock_ratio",
        "halflock_ratio",
        "relaylock_ratio",
    ]
    per_shard_rows = [
        [
            m.shard,
            m.block_count,
            m.active_block_count,
            m.total_tx_total,
            m.total_tx_committed,
            f"{m.avg_tx_committed_per_active_block:.6f}",
            m.total_relay_tx,
            f"{m.relay_ratio:.6f}",
            m.total_locked_txs,
            f"{m.locked_ratio:.6f}",
            m.migration_block_count,
            f"{m.avg_queue_len:.6f}",
            m.max_queue_len,
            m.queue_len_auc,
            m.tx_count_for_latency,
            f"{m.latency_mean:.6f}",
            f"{m.latency_p50:.6f}",
            f"{m.latency_p95:.6f}",
            f"{m.latency_p99:.6f}",
            f"{m.senlock_ratio:.6f}",
            f"{m.reclock_ratio:.6f}",
            f"{m.halflock_ratio:.6f}",
            f"{m.relaylock_ratio:.6f}",
        ]
        for m in per_shard
    ]
    write_csv(table_dir / "metrics_per_shard.csv", per_shard_header, per_shard_rows)

    overall_header = list(overall.keys())
    overall_row = [f"{overall[k]:.6f}" for k in overall_header]
    write_csv(table_dir / "metrics_overall.csv", overall_header, [overall_row])

    write_markdown_summary(table_dir / "metrics_summary.md", per_shard, overall)

    plot_lines(
        series_map,
        fig_dir / "tx_committed_by_block.png",
        "block_x",
        "committed_y",
        "Tx Committed by Block",
        "Block Height",
        "tx_committed",
    )
    plot_lines(
        series_map,
        fig_dir / "queue_len_by_block.png",
        "queue_x",
        "queue_y",
        "Queue Length by Block",
        "Block Height",
        "queueLen",
    )
    plot_lines(
        series_map,
        fig_dir / "locked_txs_by_block.png",
        "block_x",
        "locked_y",
        "Locked TXs by Block",
        "Block Height",
        "locked_txs",
    )
    plot_latency_cdf(series_map, fig_dir / "latency_cdf.png")

    print(f"[done] log_dir={log_dir}")
    print(f"[done] output={out_dir}")
    if plt is None:
        print("[warn] matplotlib 不可用，未生成图像，仅输出表格。")


if __name__ == "__main__":
    main()

