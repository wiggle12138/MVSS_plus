#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
实验指标统一定义与计算（单次 run 的 log 目录 → 结构化指标）。

设计原则：
- 指标公式集中在本文件与 说明文档/实验指标定义.md，便于查阅与修改；
- 复用 analyze_logs.py 的 CSV 读取与基础统计，不重复造轮子；
- 不修改 Go 运行时；仅消费 log/*.csv。

用法:
  python scripts/metrics_definitions.py --log-dir log --out results/current_run/metrics.json
"""

from __future__ import annotations

import argparse
import json
import statistics
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from analyze_logs import (
    collect_shards,
    percentile,
    read_csv_rows,
    to_bool,
    to_float,
    to_int,
)

# 探针交易 Id 基址（与 core.SyncProbeIDBase 一致）
PROBE_ID_BASE = 9_000_000_000
# 无全量 sync 对照时，DSR 分母用的账户状态估算字节（EOA 量级）
ESTIMATED_FULL_STATE_BYTES = 128


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------


@dataclass
class MigrationWindow:
    """迁移窗口：含任一 mig1/2/ann/ns 的区块高度闭区间。"""

    w_start: int = 0
    w_end: int = 0
    t_start_ms: int = 0
    t_end_ms: int = 0
    duration_ms: int = 0
    mig_block_count: int = 0
    # block.csv 的 pbfttime（Unix ms），与 sync.csv 的 ts 同量纲
    t_start_pbft_ms: int = 0
    t_end_pbft_ms: int = 0

    @property
    def valid(self) -> bool:
        return self.w_end >= self.w_start > 0 and self.duration_ms > 0


@dataclass
class LatencyBreakdown:
    """延迟分解（工程近似，单位 ms）。"""

    total_mean: float = 0.0
    total_p50: float = 0.0
    total_p95: float = 0.0
    source_queue_s_mean: float = 0.0
    lock_l_mean: float = 0.0
    target_queue_t_mean: float = 0.0
    sample_count: int = 0


@dataclass
class SyncMetrics:
    """Stage3 同步指标（来自 S*_sync.csv）。"""

    send_count: int = 0
    send_bytes_total: int = 0
    send_bytes_sync: int = 0
    send_bytes_delta: int = 0
    recv_count: int = 0
    apply_count: int = 0
    ack_send_count: int = 0
    ack_recv_count: int = 0
    abort_count: int = 0
    sync_latency_median_ms: float = 0.0
    sync_latency_p95_ms: float = 0.0
    sync_extra_delay_ratio: float = 0.0
    dsr_ratio: float = 0.0
    bandwidth_mb: float = 0.0
    # batch send 解析（reason=batch=N）
    batch_send_count: int = 0
    batch_size_mean: float = 0.0
    batch_size_max: int = 0


@dataclass
class Exp6Stage3Metrics:
    """Exp6 窗口敏感性专用：Stage3 sync 时序与 batch 形态（不影响既有全局指标）。"""

    stage3_makespan_ms: float = 0.0
    send_to_first_ack_ms: float = 0.0
    send_to_last_ack_ms: float = 0.0
    sync_per_ack_latency_median_ms: float = 0.0
    sync_per_ack_latency_p95_ms: float = 0.0
    probe_tx1_to_tx3_span_median_ms: float = 0.0
    probe_tx1_to_tx3_span_p95_ms: float = 0.0
    delta_account_count_est: int = 0


@dataclass
class ProbeCorrectness:
    """Sync 探针正确性（无探针时 probe_tx_count=0）。"""

    probe_tx_count: int = 0
    probe_account_count: int = 0
    block_order_ok: bool = False
    sync_pipeline_ok: bool = False
    abort_delta_count: int = 0
    issues: List[str] = field(default_factory=list)


@dataclass
class RunMetrics:
    """单次实验 run 的汇总指标。"""

    log_dir: str
    shard_count: int
    # 论文核心四指标 + DSR
    latency: LatencyBreakdown = field(default_factory=LatencyBreakdown)
    rlt_block_ratio: float = 0.0
    rlt_tx_ratio: float = 0.0
    rlt_window_ratio: float = 0.0
    tps_migration: float = 0.0
    tps_global: float = 0.0
    rdt_ratio: float = 0.0
    rdt_disordered_count: int = 0
    rdt_sample_count: int = 0
    # 迁移窗口
    migration_window: MigrationWindow = field(default_factory=MigrationWindow)
    migration_completion_ms: int = 0
    mig_related_latency_mean: float = 0.0
    # 同步 / Delta
    sync: SyncMetrics = field(default_factory=SyncMetrics)
    exp6: Exp6Stage3Metrics = field(default_factory=Exp6Stage3Metrics)
    probe: ProbeCorrectness = field(default_factory=ProbeCorrectness)
    # 辅助
    tx_committed_total: int = 0
    relay_ratio: float = 0.0
    locked_ratio: float = 0.0
    avg_queue_len: float = 0.0
    algorithm_time_ms: int = 0


# ---------------------------------------------------------------------------
# 迁移窗口
# ---------------------------------------------------------------------------


def _block_mig_total(row: Dict[str, str]) -> int:
    return (
        to_int(row.get("mig1", "0"))
        + to_int(row.get("mig2", "0"))
        + to_int(row.get("ann", "0"))
        + to_int(row.get("ns", "0"))
    )


def _height_to_pbfttime(log_dir: Path, shards: List[str]) -> Dict[int, int]:
    """区块高度 → pbfttime（Unix ms），与 sync.csv 的 ts 同量纲。"""
    height_to_pbft: Dict[int, int] = {}
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            h = to_int(r.get("blockHeight", "0"))
            pbft = to_int(r.get("pbfttime", "0"))
            if h > 0 and pbft > 0:
                if h not in height_to_pbft or pbft < height_to_pbft[h]:
                    height_to_pbft[h] = pbft
    return height_to_pbft


def _shard_height_to_pbfttime(log_dir: Path, shards: List[str]) -> Dict[Tuple[str, int], int]:
    """(分片, 区块高度) → pbfttime（Unix ms）。"""
    out: Dict[Tuple[str, int], int] = {}
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            h = to_int(r.get("blockHeight", "0"))
            pbft = to_int(r.get("pbfttime", "0"))
            if h > 0 and pbft > 0:
                key = (shard, h)
                if key not in out or pbft < out[key]:
                    out[key] = pbft
    return out


def _shard_index(shard: str) -> int:
    if shard.startswith("S") and shard[1:].isdigit():
        return int(shard[1:])
    return 0


def _tx_commit_key(
    shard: str,
    block_height: int,
    txid: int,
    shard_pbft: Dict[Tuple[str, int], int],
) -> Tuple[int, int, int, int]:
    """跨分片全局提交序：(pbfttime, shard_idx, blockHeight, txid)。"""
    pbft = shard_pbft.get((shard, block_height), 0)
    return (pbft, _shard_index(shard), block_height, txid)


def _tx_logic_key(request_time: float, txid: int) -> Tuple[float, int]:
    """客户端逻辑提交序；request_time 相同时用 txid（注入序）打破平局。"""
    return (request_time, txid)


def compute_migration_window(log_dir: Path, shards: List[str]) -> MigrationWindow:
    """跨分片合并迁移块，取高度区间与对应时间戳。"""
    mig_heights: List[int] = []
    height_to_ts: Dict[int, int] = {}
    height_to_pbft = _height_to_pbfttime(log_dir, shards)

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            h = to_int(r.get("blockHeight", "0"))
            if _block_mig_total(r) > 0:
                mig_heights.append(h)
            ts = to_int(r.get("timestamp", "0"))
            if h > 0 and ts > 0:
                # 同高度多块时取最早时间戳
                if h not in height_to_ts or ts < height_to_ts[h]:
                    height_to_ts[h] = ts

    if not mig_heights:
        return MigrationWindow()

    w_start, w_end = min(mig_heights), max(mig_heights)
    t_start = height_to_ts.get(w_start, 0)
    t_end = height_to_ts.get(w_end, 0)
    duration = max(0, t_end - t_start) if t_start > 0 and t_end > 0 else 0

    return MigrationWindow(
        w_start=w_start,
        w_end=w_end,
        t_start_ms=t_start,
        t_end_ms=t_end,
        duration_ms=duration,
        mig_block_count=len(mig_heights),
        t_start_pbft_ms=height_to_pbft.get(w_start, 0),
        t_end_pbft_ms=height_to_pbft.get(w_end, 0),
    )


# ---------------------------------------------------------------------------
# 延迟 S + L + T（工程近似）
# ---------------------------------------------------------------------------


def _tx_lock_latency_ms(row: Dict[str, str]) -> float:
    """锁等待 L：两段锁时间之和。"""
    total = 0.0
    for lock_k, unlock_k in (("lock", "unlock"), ("lock2", "unlock2")):
        lock_t = to_float(row.get(lock_k, "0"))
        unlock_t = to_float(row.get(unlock_k, "0"))
        if lock_t > 0 and unlock_t > lock_t:
            total += unlock_t - lock_t
    return total


def _tx_source_queue_ms(row: Dict[str, str]) -> float:
    """
    源片排队 S（近似）：
    - 有 lock：request → lock；
    - 无 lock 且有 TXmig1：request → TXmig1；
    - 否则：0（目标片或无关交易）。
    """
    req = to_float(row.get("request_time", "0"))
    lock_t = to_float(row.get("lock", "0"))
    mig1_t = to_float(row.get("TXmig1_time", row.get("TXmig1_Time", "0")))
    if lock_t > req > 0:
        return lock_t - req
    if mig1_t > req > 0:
        return mig1_t - req
    return 0.0


def _tx_target_queue_ms(row: Dict[str, str], total_ms: float, s_ms: float, l_ms: float) -> float:
    """目标片排队 T：总延迟减去 S、L，下限 0。"""
    residual = total_ms - s_ms - l_ms
    return max(0.0, residual)


def is_migration_related_tx(row: Dict[str, str]) -> bool:
    """是否为迁移关联交易（与 summarize_multi_runs 一致）。"""
    if (
        to_bool(row.get("SenLock", "false"))
        or to_bool(row.get("RecLock", "false"))
        or to_bool(row.get("HalfLock", "false"))
        or to_bool(row.get("RelayLock", "false"))
    ):
        return True
    mig1 = to_float(row.get("TXmig1_time", row.get("TXmig1_Time", "0")))
    mig2 = to_float(row.get("TXmig2_Time", "0"))
    lock_t = to_float(row.get("lock", "0"))
    unlock_t = to_float(row.get("unlock", "0"))
    return (mig1 > 0) or (mig2 > 0) or (lock_t > 0) or (unlock_t > 0)


def compute_latency_breakdown(
    log_dir: Path, shards: List[str], *, migration_related_only: bool = False
) -> LatencyBreakdown:
    totals: List[float] = []
    s_vals: List[float] = []
    l_vals: List[float] = []
    t_vals: List[float] = []

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            if migration_related_only and not is_migration_related_tx(r):
                continue
            total = to_float(r.get("commit-request", "0"))
            if total < 0:
                continue
            s = _tx_source_queue_ms(r)
            l = _tx_lock_latency_ms(r)
            t = _tx_target_queue_ms(r, total, s, l)
            totals.append(total)
            s_vals.append(s)
            l_vals.append(l)
            t_vals.append(t)

    if not totals:
        return LatencyBreakdown()

    return LatencyBreakdown(
        total_mean=statistics.mean(totals),
        total_p50=percentile(totals, 50),
        total_p95=percentile(totals, 95),
        source_queue_s_mean=statistics.mean(s_vals),
        lock_l_mean=statistics.mean(l_vals),
        target_queue_t_mean=statistics.mean(t_vals),
        sample_count=len(totals),
    )


# ---------------------------------------------------------------------------
# RLT / TPS
# ---------------------------------------------------------------------------


def compute_rlt(log_dir: Path, shards: List[str], window: MigrationWindow) -> Tuple[float, float, float]:
    """
    返回 (全局 block 级 RLT, 全局 tx 级 RLT, 迁移窗口内 block 级 RLT)。
    block 级：Σlocked_txs / Σtx_total
    tx 级：count(SenLock|RecLock|HalfLock) / count(tx)
    """
    locked_sum = 0
    tx_total_sum = 0
    window_locked = 0
    window_tx_total = 0
    lock_tx_cnt = 0
    tx_cnt = 0

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            h = to_int(r.get("blockHeight", "0"))
            tx_total = to_int(r.get("tx_total", "0"))
            locked = to_int(r.get("locked_txs", "0"))
            locked_sum += locked
            tx_total_sum += tx_total
            if window.valid and window.w_start <= h <= window.w_end:
                window_locked += locked
                window_tx_total += tx_total

        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            tx_cnt += 1
            if (
                to_bool(r.get("SenLock", "false"))
                or to_bool(r.get("RecLock", "false"))
                or to_bool(r.get("HalfLock", "false"))
            ):
                lock_tx_cnt += 1

    rlt_block = locked_sum / tx_total_sum if tx_total_sum > 0 else 0.0
    rlt_tx = lock_tx_cnt / tx_cnt if tx_cnt > 0 else 0.0
    rlt_window = window_locked / window_tx_total if window_tx_total > 0 else 0.0
    return rlt_block, rlt_tx, rlt_window


def compute_tps(
    log_dir: Path, shards: List[str], window: MigrationWindow
) -> Tuple[float, float]:
    """返回 (迁移窗口 TPS, 全局 TPS)。全局 TPS 用首尾块时间戳。"""
    committed_window = 0
    committed_all = 0
    all_ts: List[int] = []

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            h = to_int(r.get("blockHeight", "0"))
            committed = to_int(r.get("tx_committed", "0"))
            committed_all += committed
            ts = to_int(r.get("timestamp", "0"))
            if ts > 0:
                all_ts.append(ts)
            if window.valid and window.w_start <= h <= window.w_end:
                committed_window += committed

    tps_mig = (
        committed_window / (window.duration_ms / 1000.0)
        if window.valid and window.duration_ms > 0
        else 0.0
    )
    if len(all_ts) >= 2:
        span_ms = max(all_ts) - min(all_ts)
        tps_global = committed_all / (span_ms / 1000.0) if span_ms > 0 else 0.0
    else:
        tps_global = 0.0
    return tps_mig, tps_global


# ---------------------------------------------------------------------------
# RDT（乱序比例）
# ---------------------------------------------------------------------------


def _is_probe_txid(txid: int) -> bool:
    return txid >= PROBE_ID_BASE


def _rdt_in_migration_window(bh: int, window: Optional[MigrationWindow]) -> bool:
    if window is None or not window.valid:
        return True
    return window.w_start <= bh <= window.w_end


def compute_rdt(
    log_dir: Path,
    shards: List[str],
    window: Optional[MigrationWindow] = None,
) -> Tuple[float, int, int]:
    """
    RDT（口径 B：执行序错乱，对齐论文公式 7 的「按笔数」统计）。

    在迁移窗口内收集 TX_reg（迁移关联交易），按 sender 分组：
    若存在两笔 tx 满足「逻辑上先发后到」（request_time, txid），
    但「全局提交序」上先到后提交（pbfttime, shard, blockHeight, txid），
    则后提交的那笔计入乱序（later submitted, earlier committed）。

    注：论文 old→new→old 交错（TX_ti）需 ClientTimestamp + TXmig1 分流；
    自然负载（Exp1/4）无可靠发起时间时，口径 B 用注入序近似逻辑序。
    受控交错机理见 Exp2/6 探针（块序 tx1<tx2<tx3）。

    探针交易（Exp2/6）：块高不满足 block(tx1) < block(tx2) < block(tx3) 时，
    该组探针 tx 计入乱序分子。

    返回 (rdt_ratio, disordered_tx_count, tx_reg_count)。
    """
    disordered_txids: set[int] = set()
    tx_reg_count = 0
    shard_pbft = _shard_height_to_pbfttime(log_dir, shards)

    probe_by_acct: Dict[int, Dict[int, Tuple[int, int]]] = {}
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            txid = to_int(r.get("txid", "0"))
            if not _is_probe_txid(txid):
                continue
            bh = to_int(r.get("blockHeight", "0"))
            if not _rdt_in_migration_window(bh, window):
                continue
            offset = txid - PROBE_ID_BASE
            acct_idx = offset // 10
            slot = offset % 10
            if slot not in (1, 2, 3):
                continue
            probe_by_acct.setdefault(acct_idx, {})[slot] = (txid, bh)
            tx_reg_count += 1

    for slots in probe_by_acct.values():
        if len(slots) < 3:
            continue
        b1 = slots.get(1, (-1, -1))[1]
        b2 = slots.get(2, (-1, 10**9))[1]
        b3 = slots.get(3, (-1, 10**9))[1]
        if not (b1 < b2 < b3):
            for slot in (1, 2, 3):
                if slot in slots:
                    disordered_txids.add(slots[slot][0])

    by_sender: Dict[str, List[Tuple[float, int, Tuple[int, int, int, int]]]] = {}
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            txid = to_int(r.get("txid", "0"))
            if _is_probe_txid(txid):
                continue
            if not is_migration_related_tx(r):
                continue
            bh = to_int(r.get("blockHeight", "0"))
            if not _rdt_in_migration_window(bh, window):
                continue
            sender = r.get("sender", "")
            req = to_float(r.get("request_time", "0"))
            commit_key = _tx_commit_key(shard, bh, txid, shard_pbft)
            by_sender.setdefault(sender, []).append((req, txid, commit_key))
            tx_reg_count += 1

    for txs in by_sender.values():
        if len(txs) < 2:
            continue
        for i in range(len(txs)):
            req_i, txid_i, commit_i = txs[i]
            logic_i = _tx_logic_key(req_i, txid_i)
            for j in range(len(txs)):
                if i == j:
                    continue
                req_j, txid_j, commit_j = txs[j]
                logic_j = _tx_logic_key(req_j, txid_j)
                if logic_i < logic_j and commit_i > commit_j:
                    disordered_txids.add(txid_j)

    disordered_count = len(disordered_txids)
    ratio = disordered_count / tx_reg_count if tx_reg_count > 0 else 0.0
    return ratio, disordered_count, tx_reg_count


# ---------------------------------------------------------------------------
# Sync / DSR
# ---------------------------------------------------------------------------


def _parse_batch_size(reason: str) -> int:
    """从 sync.csv reason 解析 batch 大小，如 batch=14 → 14。"""
    reason = (reason or "").strip()
    if reason.startswith("batch="):
        try:
            n = int(reason.split("=", 1)[1])
            return max(1, n)
        except ValueError:
            pass
    return 1


def _pair_sync_latency_ms(events: List[Dict[str, str]]) -> List[float]:
    """
    配对 send → ack_recv 延迟（ms）。
    支持 per-addr send 与 batch send（send 行 addr 为空）：batch 后每条 ack_recv 均相对最近一次 send。
    """
    per_addr_send: Dict[str, int] = {}
    last_batch_send_ts: Optional[int] = None
    latencies: List[float] = []

    for r in sorted(events, key=lambda x: to_int(x.get("ts", "0"))):
        ev = r.get("event", "")
        addr = r.get("addr", "")
        ts = to_int(r.get("ts", "0"))
        if ev == "send":
            if addr:
                per_addr_send[addr] = ts
            else:
                last_batch_send_ts = ts
        elif ev == "ack_recv" and addr:
            base_ts: Optional[int] = None
            if addr in per_addr_send:
                base_ts = per_addr_send.pop(addr)
            elif last_batch_send_ts is not None:
                base_ts = last_batch_send_ts
            if base_ts is not None and ts >= base_ts:
                latencies.append(float(ts - base_ts))
    return latencies


def _collect_sync_events(log_dir: Path, shards: List[str]) -> List[Dict[str, str]]:
    events: List[Dict[str, str]] = []
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_sync.csv"):
            row = dict(r)
            row["_shard"] = shard
            events.append(row)
    return events


def _delta_account_count_from_sends(send_events: List[Dict[str, str]], apply_count: int) -> int:
    """估算本轮 delta 涉及账户数（用于 DSR 分母）。"""
    addrs = {r.get("addr", "") for r in send_events if r.get("addr")}
    if addrs:
        return len(addrs)
    batch_total = sum(
        _parse_batch_size(r.get("reason", ""))
        for r in send_events
        if r.get("event") == "send"
    )
    if batch_total > 0:
        return batch_total
    return max(0, apply_count)


def compute_sync_metrics(log_dir: Path, shards: List[str], mig_duration_ms: int) -> SyncMetrics:
    events = _collect_sync_events(log_dir, shards)

    m = SyncMetrics()
    if not events:
        return m

    all_send_events: List[Dict[str, str]] = []
    batch_sizes: List[int] = []

    for r in events:
        ev = r.get("event", "")
        mode = r.get("mode", "")
        nbytes = to_int(r.get("bytes", "0"))
        if ev == "send":
            m.send_count += 1
            m.send_bytes_total += nbytes
            all_send_events.append(r)
            reason = r.get("reason", "")
            if reason.startswith("batch="):
                batch_sizes.append(_parse_batch_size(reason))
                m.batch_send_count += 1
            if mode == "delta":
                m.send_bytes_delta += nbytes
            elif mode == "sync":
                m.send_bytes_sync += nbytes
        elif ev == "recv":
            m.recv_count += 1
        elif ev == "apply":
            m.apply_count += 1
        elif ev == "ack_send":
            m.ack_send_count += 1
        elif ev == "ack_recv":
            m.ack_recv_count += 1
        elif ev == "abort":
            m.abort_count += 1

    if batch_sizes:
        m.batch_size_mean = statistics.mean(batch_sizes)
        m.batch_size_max = max(batch_sizes)

    m.bandwidth_mb = m.send_bytes_total / 1_000_000.0

    # 按分片配对后再合并，避免跨分片 send/ack 误配
    send_latencies: List[float] = []
    for shard in shards:
        shard_events = [r for r in events if r.get("_shard") == shard]
        send_latencies.extend(_pair_sync_latency_ms(shard_events))

    if send_latencies:
        m.sync_latency_median_ms = percentile(send_latencies, 50)
        m.sync_latency_p95_ms = percentile(send_latencies, 95)

    if mig_duration_ms > 0 and send_latencies:
        m.sync_extra_delay_ratio = statistics.mean(send_latencies) / float(mig_duration_ms)

    if m.send_bytes_sync > 0:
        m.dsr_ratio = m.send_bytes_delta / float(m.send_bytes_sync)
    elif m.send_bytes_delta > 0:
        delta_accounts = _delta_account_count_from_sends(all_send_events, m.apply_count)
        denom = max(1, delta_accounts) * ESTIMATED_FULL_STATE_BYTES
        m.dsr_ratio = m.send_bytes_delta / float(denom)
    else:
        m.dsr_ratio = 0.0

    return m


def compute_exp6_stage3_metrics(
    log_dir: Path, shards: List[str], height_to_pbft: Dict[int, int]
) -> Exp6Stage3Metrics:
    """Exp6 专用：Stage3 时序与探针 tx1→tx3 跨度（与全局 TPS/latency 并列输出）。"""
    out = Exp6Stage3Metrics()
    events = _collect_sync_events(log_dir, shards)
    if not events:
        return out

    send_ts_list: List[int] = []
    ack_recv_ts_list: List[int] = []
    per_ack_latencies: List[float] = []

    for shard in shards:
        shard_events = [r for r in events if r.get("_shard") == shard]
        per_ack_latencies.extend(_pair_sync_latency_ms(shard_events))
        for r in shard_events:
            ev = r.get("event", "")
            ts = to_int(r.get("ts", "0"))
            if ev == "send":
                send_ts_list.append(ts)
            elif ev == "ack_recv" and r.get("addr"):
                ack_recv_ts_list.append(ts)

    if send_ts_list and ack_recv_ts_list:
        first_send = min(send_ts_list)
        first_ack = min(ack_recv_ts_list)
        last_ack = max(ack_recv_ts_list)
        out.send_to_first_ack_ms = float(max(0, first_ack - first_send))
        out.send_to_last_ack_ms = float(max(0, last_ack - first_send))
        out.stage3_makespan_ms = float(max(0, last_ack - first_send))

    if per_ack_latencies:
        out.sync_per_ack_latency_median_ms = percentile(per_ack_latencies, 50)
        out.sync_per_ack_latency_p95_ms = percentile(per_ack_latencies, 95)

    send_events = [r for r in events if r.get("event") == "send"]
    apply_count = sum(1 for r in events if r.get("event") == "apply")
    out.delta_account_count_est = _delta_account_count_from_sends(send_events, apply_count)

    # 探针 tx1 → tx3：同账户块高映射 pbfttime 差（仅 slot 1/3 均在源片出现）
    by_acct: Dict[int, Dict[int, int]] = {}
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            txid = to_int(r.get("txid", "0"))
            if not _is_probe_txid(txid):
                continue
            offset = txid - PROBE_ID_BASE
            acct_idx = offset // 10
            slot = offset % 10
            if slot not in (1, 3):
                continue
            bh = to_int(r.get("blockHeight", "0"))
            by_acct.setdefault(acct_idx, {})[slot] = bh

    tx13_spans: List[float] = []
    for slots in by_acct.values():
        if 1 not in slots or 3 not in slots:
            continue
        t1 = height_to_pbft.get(slots[1], 0)
        t3 = height_to_pbft.get(slots[3], 0)
        if t1 > 0 and t3 > 0 and t3 >= t1:
            tx13_spans.append(float(t3 - t1))

    if tx13_spans:
        out.probe_tx1_to_tx3_span_median_ms = percentile(tx13_spans, 50)
        out.probe_tx1_to_tx3_span_p95_ms = percentile(tx13_spans, 95)

    return out


# ---------------------------------------------------------------------------
# 探针正确性（轻量版，不依赖 analyze_sync_probe 全文）
# ---------------------------------------------------------------------------


def compute_probe_correctness(log_dir: Path, shards: List[str]) -> ProbeCorrectness:
    probe_txs: List[Tuple[int, int, int]] = []  # (acct_idx, slot, block_height)
    issues: List[str] = []

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            txid = to_int(r.get("txid", "0"))
            if not _is_probe_txid(txid):
                continue
            offset = txid - PROBE_ID_BASE
            acct_idx = offset // 10
            slot = offset % 10
            bh = to_int(r.get("blockHeight", "0"))
            probe_txs.append((acct_idx, slot, bh))

    p = ProbeCorrectness(probe_tx_count=len(probe_txs))
    if not probe_txs:
        p.block_order_ok = True
        p.sync_pipeline_ok = True
        return p

    accts = {x[0] for x in probe_txs}
    p.probe_account_count = len(accts)

    by_acct: Dict[int, Dict[int, int]] = {}
    for acct_idx, slot, bh in probe_txs:
        by_acct.setdefault(acct_idx, {})[slot] = bh

    block_ok = True
    for acct_idx, slots in by_acct.items():
        if len(slots) != 3:
            block_ok = False
            issues.append(f"账户 {acct_idx} 探针交易数 {len(slots)} ≠ 3")
            continue
        b1, b2, b3 = slots.get(1, -1), slots.get(2, 10**9), slots.get(3, 10**9)
        if not (b1 < b2 < b3):
            block_ok = False
            issues.append(f"账户 {acct_idx} 块序失败: {b1} < {b2} < {b3}")
    p.block_order_ok = block_ok

    # sync 通路
    events = []
    for shard in shards:
        events.extend(read_csv_rows(log_dir / f"{shard}_sync.csv"))
    p.abort_delta_count = sum(
        1 for r in events if r.get("event") == "abort" and r.get("mode") == "delta"
    )
    has_send = any(r.get("event") == "send" for r in events)
    has_apply = any(r.get("event") == "apply" for r in events)
    has_ack = any(r.get("event") == "ack_recv" for r in events)
    p.sync_pipeline_ok = has_send and has_apply and has_ack and p.abort_delta_count == 0
    if not p.sync_pipeline_ok:
        if not has_send:
            issues.append("缺少 sync send")
        if not has_apply:
            issues.append("缺少 sync apply")
        if not has_ack:
            issues.append("缺少 sync ack_recv")
        if p.abort_delta_count > 0:
            issues.append(f"abort,delta 共 {p.abort_delta_count} 条")

    p.issues = issues
    return p


# ---------------------------------------------------------------------------
# 单次 run 汇总
# ---------------------------------------------------------------------------


def analyze_run(log_dir: Path) -> RunMetrics:
    """从 log 目录计算全部可得指标。"""
    log_dir = log_dir.resolve()
    shards = collect_shards(log_dir)
    if not shards:
        raise ValueError(f"未找到 S*_block.csv: {log_dir}")

    window = compute_migration_window(log_dir, shards)
    height_to_pbft = _height_to_pbfttime(log_dir, shards)
    latency_all = compute_latency_breakdown(log_dir, shards, migration_related_only=False)
    latency_mig = compute_latency_breakdown(log_dir, shards, migration_related_only=True)
    rlt_block, rlt_tx, rlt_window = compute_rlt(log_dir, shards, window)
    tps_mig, tps_global = compute_tps(log_dir, shards, window)
    rdt_ratio, rdt_dis, rdt_n = compute_rdt(log_dir, shards, window)
    sync = compute_sync_metrics(log_dir, shards, window.duration_ms)
    exp6 = compute_exp6_stage3_metrics(log_dir, shards, height_to_pbft)
    probe = compute_probe_correctness(log_dir, shards)

    tx_committed = 0
    tx_total = 0
    locked_sum = 0
    relay_sum = 0
    queue_vals: List[int] = []

    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_block.csv"):
            tx_committed += to_int(r.get("tx_committed", "0"))
            tx_total += to_int(r.get("tx_total", "0"))
            locked_sum += to_int(r.get("locked_txs", "0"))
            relay_sum += to_int(r.get("tx_relay_sender", "0")) + to_int(
                r.get("tx_relay_receiver", "0")
            )
        for r in read_csv_rows(log_dir / f"{shard}_queueLen.csv"):
            queue_vals.append(to_int(r.get("queueLen", "0")))

    mig_completion = window.duration_ms
    # sync.ts 与 block.pbfttime 均为 Unix ms；勿与 block.timestamp（相对 ms）混减
    all_sync_ts: List[int] = []
    for shard in shards:
        for r in read_csv_rows(log_dir / f"{shard}_sync.csv"):
            ts = to_int(r.get("ts", "0"))
            if ts > 0:
                all_sync_ts.append(ts)
    if all_sync_ts and window.t_start_pbft_ms > 0:
        mig_completion = max(0, max(all_sync_ts) - window.t_start_pbft_ms)
    elif exp6.stage3_makespan_ms > 0:
        mig_completion = int(round(exp6.stage3_makespan_ms))

    algo_ms = 0
    mig_csv = log_dir / "migration.csv"
    if mig_csv.exists():
        rows = read_csv_rows(mig_csv)
        if rows:
            algo_ms = to_int(rows[-1].get("pagerank_time", "0"))

    return RunMetrics(
        log_dir=str(log_dir),
        shard_count=len(shards),
        latency=latency_all,
        rlt_block_ratio=rlt_block,
        rlt_tx_ratio=rlt_tx,
        rlt_window_ratio=rlt_window,
        tps_migration=tps_mig,
        tps_global=tps_global,
        rdt_ratio=rdt_ratio,
        rdt_disordered_count=rdt_dis,
        rdt_sample_count=rdt_n,
        migration_window=window,
        migration_completion_ms=mig_completion,
        mig_related_latency_mean=latency_mig.total_mean,
        sync=sync,
        exp6=exp6,
        probe=probe,
        tx_committed_total=tx_committed,
        relay_ratio=relay_sum / tx_total if tx_total > 0 else 0.0,
        locked_ratio=locked_sum / tx_total if tx_total > 0 else 0.0,
        avg_queue_len=statistics.mean(queue_vals) if queue_vals else 0.0,
        algorithm_time_ms=algo_ms,
    )


def run_metrics_to_dict(m: RunMetrics) -> dict:
    """转为可 JSON 序列化的 dict（扁平化常用字段）。"""
    d = asdict(m)
    # 附加扁平字段，方便批处理脚本读取
    d["latency_mean"] = m.latency.total_mean
    d["latency_p95"] = m.latency.total_p95
    d["latency_s_mean"] = m.latency.source_queue_s_mean
    d["latency_l_mean"] = m.latency.lock_l_mean
    d["latency_t_mean"] = m.latency.target_queue_t_mean
    d["dsr"] = m.sync.dsr_ratio
    d["sync_bandwidth_mb"] = m.sync.bandwidth_mb
    d["sync_send_count"] = m.sync.send_count
    d["sync_batch_send_count"] = m.sync.batch_send_count
    d["sync_batch_size_mean"] = m.sync.batch_size_mean
    d["sync_batch_size_max"] = m.sync.batch_size_max
    # Exp6 Stage3 专用（扁平字段，便于批处理）
    d["exp6_stage3_makespan_ms"] = m.exp6.stage3_makespan_ms
    d["exp6_send_to_first_ack_ms"] = m.exp6.send_to_first_ack_ms
    d["exp6_send_to_last_ack_ms"] = m.exp6.send_to_last_ack_ms
    d["exp6_sync_per_ack_latency_median_ms"] = m.exp6.sync_per_ack_latency_median_ms
    d["exp6_sync_per_ack_latency_p95_ms"] = m.exp6.sync_per_ack_latency_p95_ms
    d["exp6_probe_tx1_to_tx3_span_median_ms"] = m.exp6.probe_tx1_to_tx3_span_median_ms
    d["exp6_probe_tx1_to_tx3_span_p95_ms"] = m.exp6.probe_tx1_to_tx3_span_p95_ms
    d["exp6_delta_account_count_est"] = m.exp6.delta_account_count_est
    d["probe_ok"] = (
        (m.probe.probe_tx_count == 0 or m.probe.block_order_ok)
        and (m.sync.send_count == 0 or m.probe.sync_pipeline_ok)
    )
    return d


def main() -> None:
    parser = argparse.ArgumentParser(description="计算单次 run 实验指标（见 说明文档/实验指标定义.md）")
    parser.add_argument("--log-dir", default="log", help="日志目录")
    parser.add_argument("--out", default="", help="输出 JSON 路径（可选）")
    args = parser.parse_args()

    log_dir = Path(args.log_dir)
    metrics = analyze_run(log_dir)
    data = run_metrics_to_dict(metrics)

    print(json.dumps(data, ensure_ascii=False, indent=2))

    if args.out:
        out = Path(args.out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"[done] wrote {out}", flush=True)


if __name__ == "__main__":
    main()
