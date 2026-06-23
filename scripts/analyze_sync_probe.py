#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Sync 探针实验数据分析：交易上链视角 + sync 消息通信视角。

默认输入: ./log
默认输出: ./results/sync_probe

用法:
  python scripts/analyze_sync_probe.py
  python scripts/analyze_sync_probe.py --log-dir log --out-dir results/sync_probe
"""

from __future__ import annotations

import argparse
import csv
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Tuple

PROBE_BASE = 9_000_000_000
PROBE_STRIDE = 10
PROBE_SLOT_NAMES = {1: "tx1_old", 2: "tx2_new", 3: "tx3_old"}
PROBE_CLIENT_TS = {1: 100, 2: 200, 3: 300}
EXPECTED_SHARD = {1: "S0", 2: "S1", 3: "S0"}

SYNC_EVENT_DESC = {
    "send": "源片发出 State_ini",
    "recv": "目标片收到 State_ini",
    "apply": "目标片应用 State_ini",
    "abort": "同步中止",
    "ack_send": "目标片发出 ack",
    "ack_recv": "源片收到 ack",
}

SYNC_FLOW_HINT = {
    ("S0", "send"): "S0 → S1",
    ("S1", "recv"): "S0 → S1",
    ("S1", "apply"): "S1 本地",
    ("S1", "ack_send"): "S1 → S0",
    ("S0", "ack_recv"): "S1 → S0",
    ("S0", "abort"): "S0 本地",
    ("S1", "abort"): "S1 本地",
}


def to_int(v: str, default: int = 0) -> int:
    try:
        return int(float(v))
    except (TypeError, ValueError):
        return default


def to_bool(v: str) -> bool:
    return str(v).strip().lower() in {"1", "true", "yes", "y"}


def read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, header: List[str], rows: List[List[object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)


def decode_probe_txid(txid: int) -> Optional[Tuple[int, int, str]]:
    if txid < PROBE_BASE:
        return None
    offset = txid - PROBE_BASE
    if offset <= 0 or offset >= 1000:
        return None
    account_idx = offset // PROBE_STRIDE
    slot = offset % PROBE_STRIDE
    if slot not in PROBE_SLOT_NAMES:
        return None
    return account_idx, slot, PROBE_SLOT_NAMES[slot]


def collect_shards(log_dir: Path, suffix: str) -> List[str]:
    shards = []
    for p in sorted(log_dir.glob(f"S*_{suffix}.csv")):
        shards.append(p.name.split("_")[0])
    return sorted(set(shards))


@dataclass
class ProbeTx:
    shard: str
    txid: int
    account_idx: int
    slot: int
    probe_type: str
    client_ts: int
    sender: str
    recipient: str
    block_height: int
    request_time: int
    second_request_time: int
    commit_latency: int
    is_success: bool

    @property
    def expected_shard(self) -> str:
        return EXPECTED_SHARD[self.slot]


@dataclass
class ProbeAccountReport:
    account_idx: int
    sender: str
    txs: Dict[int, ProbeTx] = field(default_factory=dict)
    issues: List[str] = field(default_factory=list)

    def validate(self) -> None:
        self.issues.clear()
        for slot in (1, 2, 3):
            if slot not in self.txs:
                self.issues.append(f"缺少 {PROBE_SLOT_NAMES[slot]} (slot={slot})")
        for slot, tx in self.txs.items():
            if tx.shard != tx.expected_shard:
                self.issues.append(
                    f"{tx.probe_type} 应在 {tx.expected_shard}，实际在 {tx.shard}"
                )
        if 1 in self.txs and 2 in self.txs and 3 in self.txs:
            b1, b2, b3 = self.txs[1].block_height, self.txs[2].block_height, self.txs[3].block_height
            if not (b1 < b2 < b3):
                self.issues.append(f"块序异常: tx1={b1}, tx2={b2}, tx3={b3}，期望 tx1 < tx2 < tx3")
            if b1 == b3:
                self.issues.append("tx1 与 tx3 同块，期望不同块")
        # 探针为合成 1-value 转账，isSuccess 字段可能恒为 false，不作为硬失败条件


@dataclass
class SyncEvent:
    shard: str
    ts: int
    event: str
    mode: str
    addr: str
    start_n: int
    end_n: int
    ok: bool
    reason: str
    nbytes: int

    @property
    def flow(self) -> str:
        return SYNC_FLOW_HINT.get((self.shard, self.event), "")

    @property
    def summary(self) -> str:
        parts = [SYNC_EVENT_DESC.get(self.event, self.event)]
        if self.addr:
            parts.append(f"addr={self.addr[:12]}…")
        if self.start_n or self.end_n:
            parts.append(f"nonce {self.start_n}→{self.end_n}")
        if self.reason:
            parts.append(self.reason)
        return " | ".join(parts)


def load_probe_transactions(log_dir: Path) -> List[ProbeTx]:
    rows: List[ProbeTx] = []
    for shard in collect_shards(log_dir, "transaction"):
        for r in read_csv_rows(log_dir / f"{shard}_transaction.csv"):
            txid = to_int(r.get("txid", "0"))
            decoded = decode_probe_txid(txid)
            if decoded is None:
                continue
            account_idx, slot, probe_type = decoded
            rows.append(
                ProbeTx(
                    shard=shard,
                    txid=txid,
                    account_idx=account_idx,
                    slot=slot,
                    probe_type=probe_type,
                    client_ts=PROBE_CLIENT_TS[slot],
                    sender=r.get("sender", ""),
                    recipient=r.get("recipient", ""),
                    block_height=to_int(r.get("blockHeight", "0")),
                    request_time=to_int(r.get("request_time", "0")),
                    second_request_time=to_int(r.get("2nd_request_time", "0")),
                    commit_latency=to_int(r.get("commit-request", "0")),
                    is_success=to_bool(r.get("isSuccess", "false")),
                )
            )
    rows.sort(key=lambda x: (x.account_idx, x.client_ts, x.block_height))
    return rows


def group_by_account(probe_txs: List[ProbeTx]) -> List[ProbeAccountReport]:
    by_idx: Dict[int, ProbeAccountReport] = {}
    for tx in probe_txs:
        if tx.account_idx not in by_idx:
            by_idx[tx.account_idx] = ProbeAccountReport(
                account_idx=tx.account_idx,
                sender=tx.sender,
            )
        by_idx[tx.account_idx].txs[tx.slot] = tx
    reports = [by_idx[k] for k in sorted(by_idx)]
    for rep in reports:
        rep.validate()
    return reports


def load_sync_events(log_dir: Path) -> List[SyncEvent]:
    events: List[SyncEvent] = []
    for shard in collect_shards(log_dir, "sync"):
        for r in read_csv_rows(log_dir / f"{shard}_sync.csv"):
            events.append(
                SyncEvent(
                    shard=shard,
                    ts=to_int(r.get("ts", "0")),
                    event=r.get("event", ""),
                    mode=r.get("mode", ""),
                    addr=r.get("addr", ""),
                    start_n=to_int(r.get("start_n", "0")),
                    end_n=to_int(r.get("end_n", "0")),
                    ok=to_bool(r.get("ok", "0")),
                    reason=r.get("reason", ""),
                    nbytes=to_int(r.get("bytes", "0")),
                )
            )
    events.sort(key=lambda e: (e.ts, e.shard, e.event, e.addr))
    return events


def check_global_exec_order(probe_txs: List[ProbeTx]) -> Tuple[bool, str]:
    """按全局上链块高检查 old-new-old 交错是否成立。"""
    if not probe_txs:
        return False, "未找到探针交易"
    ordered = sorted(probe_txs, key=lambda x: (x.block_height, x.client_ts, x.txid))
    seq = " → ".join(f"{t.probe_type}@{t.shard}#B{t.block_height}" for t in ordered)
    slots = [t.slot for t in ordered]
    ok = slots == sorted(slots, key=lambda s: (0 if s == 1 else 1 if s == 2 else 2, s))
    # 更严格：所有 tx1 在 tx2 前，所有 tx2 在 tx3 前
    b1 = max((t.block_height for t in probe_txs if t.slot == 1), default=-1)
    b2 = min((t.block_height for t in probe_txs if t.slot == 2), default=10**9)
    b3 = min((t.block_height for t in probe_txs if t.slot == 3), default=10**9)
    strict_ok = b1 < b2 < b3
    msg = f"全局块序: {seq}"
    if not strict_ok:
        msg += f" | 块界检查失败: max(tx1)={b1}, min(tx2)={b2}, min(tx3)={b3}"
    return strict_ok, msg


def check_sync_pipeline(events: List[SyncEvent]) -> Tuple[bool, List[str]]:
    issues: List[str] = []
    if not events:
        return False, ["未找到 sync 日志（需 SYNC_PROBE=1 且 N0 写盘）"]

    aborts = [e for e in events if e.event == "abort"]
    if aborts:
        issues.append(f"存在 {len(aborts)} 条 abort 事件")

    s0_send = [e for e in events if e.shard == "S0" and e.event == "send"]
    s1_recv = [e for e in events if e.shard == "S1" and e.event == "recv"]
    s1_apply = [e for e in events if e.shard == "S1" and e.event == "apply"]
    s1_ack = [e for e in events if e.shard == "S1" and e.event == "ack_send"]
    s0_ack = [e for e in events if e.shard == "S0" and e.event == "ack_recv"]

    if not s0_send:
        issues.append("缺少 S0 send")
    if not s1_recv:
        issues.append("缺少 S1 recv")
    if not s1_apply:
        issues.append("缺少 S1 apply")
    if not s1_ack:
        issues.append("缺少 S1 ack_send")
    if not s0_ack:
        issues.append("缺少 S0 ack_recv")

    if s0_send and s1_recv and s0_send[0].ts > s1_recv[0].ts:
        issues.append("时序异常: S0 send 晚于 S1 recv")
    if s1_apply and s1_ack and s1_apply[-1].ts > s1_ack[0].ts:
        issues.append("时序异常: 末条 apply 晚于首条 ack_send")
    if s1_ack and s0_ack and s1_ack[0].ts > s0_ack[0].ts:
        issues.append("时序异常: S1 ack_send 晚于 S0 ack_recv")

    apply_addrs = {e.addr for e in s1_apply if e.addr}
    ack_addrs = {e.addr for e in s0_ack if e.addr}
    if apply_addrs and ack_addrs and apply_addrs != ack_addrs:
        issues.append(f"apply/ack_recv 账户集合不一致: apply={len(apply_addrs)} ack={len(ack_addrs)}")

    for e in s1_apply:
        if e.start_n >= e.end_n:
            issues.append(f"S1 apply 无增量: addr={e.addr[:12]}… nonce {e.start_n}→{e.end_n}")

    return len(issues) == 0, issues


def write_probe_tx_csv(out_dir: Path, probe_txs: List[ProbeTx]) -> None:
    header = [
        "account_idx",
        "probe_type",
        "slot",
        "client_ts",
        "txid",
        "shard",
        "expected_shard",
        "block_height",
        "request_time",
        "2nd_request_time",
        "commit_latency",
        "is_success",
        "sender",
        "recipient",
    ]
    rows = [
        [
            tx.account_idx,
            tx.probe_type,
            tx.slot,
            tx.client_ts,
            tx.txid,
            tx.shard,
            tx.expected_shard,
            tx.block_height,
            tx.request_time,
            tx.second_request_time,
            tx.commit_latency,
            tx.is_success,
            tx.sender,
            tx.recipient,
        ]
        for tx in probe_txs
    ]
    write_csv(out_dir / "probe_tx_detail.csv", header, rows)


def write_sync_timeline_csv(out_dir: Path, events: List[SyncEvent]) -> None:
    header = [
        "order",
        "ts",
        "shard",
        "event",
        "mode",
        "flow",
        "addr",
        "start_n",
        "end_n",
        "ok",
        "reason",
        "bytes",
        "summary",
    ]
    rows = [
        [
            i + 1,
            e.ts,
            e.shard,
            e.event,
            e.mode,
            e.flow,
            e.addr,
            e.start_n,
            e.end_n,
            int(e.ok),
            e.reason,
            e.nbytes,
            e.summary,
        ]
        for i, e in enumerate(events)
    ]
    write_csv(out_dir / "sync_timeline.csv", header, rows)


def render_tx_section(
    probe_txs: List[ProbeTx],
    accounts: List[ProbeAccountReport],
    global_ok: bool,
    global_msg: str,
) -> List[str]:
    lines = ["## 1. 探针交易上链分析", ""]
    if not probe_txs:
        lines.append("未在 `*_transaction.csv` 中找到探针交易（txid ≥ 9000000001）。")
        return lines

    lines.append(f"- 探针交易总数: **{len(probe_txs)}**")
    lines.append(f"- 探针账户数: **{len(accounts)}**")
    lines.append(f"- 全局块序验收: **{'通过' if global_ok else '未通过'}**")
    lines.append(f"- {global_msg}")
    lines.append("")

    lines.append("### 1.1 按 ClientTS 逻辑序（old → new → old）")
    lines.append("")
    lines.append("| account | probe_type | client_ts | txid | shard | block | request_time | ok |")
    lines.append("|---:|---|---:|---:|---|---:|---:|---|")
    for tx in sorted(probe_txs, key=lambda x: (x.account_idx, x.client_ts)):
        lines.append(
            f"| {tx.account_idx} | {tx.probe_type} | {tx.client_ts} | {tx.txid} | "
            f"{tx.shard} | {tx.block_height} | {tx.request_time} | {tx.is_success} |"
        )
    lines.append("")

    lines.append("### 1.2 按全局上链块高（实际执行序）")
    lines.append("")
    lines.append("| order | probe_type | shard | block | txid | client_ts |")
    lines.append("|---:|---|---|---:|---:|---:|")
    for i, tx in enumerate(
        sorted(probe_txs, key=lambda x: (x.block_height, x.client_ts, x.txid)), start=1
    ):
        lines.append(
            f"| {i} | {tx.probe_type} | {tx.shard} | {tx.block_height} | {tx.txid} | {tx.client_ts} |"
        )
    lines.append("")

    lines.append("### 1.3 分账户验收")
    lines.append("")
    for rep in accounts:
        status = "通过" if not rep.issues else "未通过"
        lines.append(f"**账户 {rep.account_idx}** (`{rep.sender[:16]}…`) — {status}")
        if rep.issues:
            for issue in rep.issues:
                lines.append(f"- {issue}")
        else:
            t1, t2, t3 = rep.txs[1], rep.txs[2], rep.txs[3]
            lines.append(
                f"- 块序: tx1@B{t1.block_height} < tx2@B{t2.block_height} < tx3@B{t3.block_height}"
            )
        lines.append("")
    return lines


def render_sync_section(events: List[SyncEvent], sync_ok: bool, sync_issues: List[str]) -> List[str]:
    lines = ["## 2. Sync 消息通信时序（按 ts 全局排序）", ""]
    if not events:
        lines.append("未找到 `*_sync.csv`。")
        return lines

    lines.append(f"- 事件总数: **{len(events)}**")
    lines.append(f"- 通路验收: **{'通过' if sync_ok else '未通过'}**")
    if sync_issues:
        for issue in sync_issues:
            lines.append(f"- {issue}")
    lines.append("")

    lines.append("```")
    t0 = events[0].ts
    for i, e in enumerate(events, start=1):
        delta = e.ts - t0
        flow = f" [{e.flow}]" if e.flow else ""
        ok_mark = "OK" if e.ok else "FAIL"
        addr = f" {e.addr[:12]}…" if e.addr else ""
        nonce = f" n{e.start_n}→{e.end_n}" if e.start_n or e.end_n else ""
        extra = f" {e.reason}" if e.reason else ""
        lines.append(
            f"{i:02d} +{delta:5d}ms  {e.shard:2s}  {e.event:8s}  {e.mode:5s}{flow}{addr}{nonce}  [{ok_mark}]{extra}"
        )
    lines.append("```")
    lines.append("")
    lines.append("期望主链路：`S0 send → S1 recv → S1 apply → S1 ack_send → S0 ack_recv`")
    lines.append("")
    return lines


def write_report(
    out_dir: Path,
    probe_txs: List[ProbeTx],
    accounts: List[ProbeAccountReport],
    global_ok: bool,
    global_msg: str,
    events: List[SyncEvent],
    sync_ok: bool,
    sync_issues: List[str],
) -> None:
    lines = ["# Sync 探针实验分析报告", ""]
    all_account_ok = all(not rep.issues for rep in accounts)
    overall = global_ok and all_account_ok and sync_ok and bool(probe_txs)
    warn_success = [tx for tx in probe_txs if not tx.is_success]
    lines.append(f"**总体结论: {'通过' if overall else '需复查'}**")
    if warn_success:
        lines.append(
            f"- 备注: {len(warn_success)} 笔探针交易 isSuccess=false（合成转账常见，以上链块高为准）"
        )
    lines.append("")
    lines.extend(render_tx_section(probe_txs, accounts, global_ok, global_msg))
    lines.extend(render_sync_section(events, sync_ok, sync_issues))
    lines.append("## 3. 输出文件")
    lines.append("")
    lines.append("- `probe_tx_detail.csv` — 探针交易明细（含 probe_type / client_ts）")
    lines.append("- `sync_timeline.csv` — 合并排序后的 sync 事件")
    lines.append("")
    (out_dir / "probe_report.md").write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description="分析 Sync 探针交易与 sync 通信日志。")
    parser.add_argument("--log-dir", default="log", help="日志目录，默认 log")
    parser.add_argument("--out-dir", default="results/sync_probe", help="输出目录")
    args = parser.parse_args()

    log_dir = Path(args.log_dir).resolve()
    out_dir = Path(args.out_dir).resolve()
    out_dir.mkdir(parents=True, exist_ok=True)

    probe_txs = load_probe_transactions(log_dir)
    accounts = group_by_account(probe_txs)
    global_ok, global_msg = check_global_exec_order(probe_txs)
    events = load_sync_events(log_dir)
    sync_ok, sync_issues = check_sync_pipeline(events)

    write_probe_tx_csv(out_dir, probe_txs)
    write_sync_timeline_csv(out_dir, events)
    write_report(
        out_dir, probe_txs, accounts, global_ok, global_msg, events, sync_ok, sync_issues
    )

    print(f"[done] log_dir={log_dir}")
    print(f"[done] output={out_dir}")
    print(f"[probe] txs={len(probe_txs)} accounts={len(accounts)} global_order={'OK' if global_ok else 'FAIL'}")
    print(f"[sync] events={len(events)} pipeline={'OK' if sync_ok else 'FAIL'}")
    print(f"[report] {out_dir / 'probe_report.md'}")


if __name__ == "__main__":
    main()
