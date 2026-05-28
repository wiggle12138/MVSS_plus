#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
从 selectedTxs_300K.csv 生成带 client_ts 列的 MVSS 实验数据集。

输出 CSV 列约定（0-based，与 pbft/client.go dataset_flag==2 一致）：
  3: sender (0x...)
  4: recipient (0x...)
  8: value (wei string)
  9: client_ts (ms, 新增)

语义：
  - CSV 行序 ≈ 客户端注入顺序 → Go 侧 RequestTime（到达时间）在注入时赋值
  - client_ts = 客户端逻辑时间戳 → Go 侧用于 OrderList / DetectInterleave 排序

Episode 设计（每个迁移账户）：
  - 迁移前注入 2 笔 old（client_ts: T, T+200）
  - 迁移后注入 1 笔 new（client_ts: T+100）
  → 按 client_ts 排序为 old-new-old，可触发 MVSS sync

用法示例：
  python scripts/generate_mvss_dataset.py \\
    --input selectedTxs_300K.csv \\
    --output datasets/mvss_interleave_benchmark.csv \\
    --meta datasets/mvss_interleave_benchmark.meta.json \\
    --episodes 5 --max-rows 12000 --mig-index 7500
"""

from __future__ import annotations

import argparse
import csv
import json
import random
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple


# ---------------------------------------------------------------------------
# 与 Go utils.Addr2Shard 一致：地址末 5 位 hex % shard_num
# ---------------------------------------------------------------------------

def normalize_addr(addr: str) -> str:
    addr = addr.strip().lower()
    if addr.startswith("0x"):
        addr = addr[2:]
    if len(addr) != 40:
        raise ValueError(f"invalid address length: {addr!r}")
    return addr


def addr2shard(addr: str, shard_num: int) -> int:
    addr = normalize_addr(addr)
    suffix = addr[-5:]
    return int(suffix, 16) % shard_num


def addr_with_0x(addr: str) -> str:
    addr = normalize_addr(addr)
    return "0x" + addr


# ---------------------------------------------------------------------------
# CSV 行模型
# ---------------------------------------------------------------------------

@dataclass
class TxRow:
    sender: str
    recipient: str
    value: str
    client_ts: int = -1
    source_index: int = -1  # 原始文件行号（0-based 数据行）
    tag: str = "background"  # background | episode_old | episode_new

    def to_csv_row(self) -> List[str]:
        """生成与 selectedTxs_300K 兼容的行，client_ts 放在列 9。"""
        row = [""] * 18
        row[3] = addr_with_0x(self.sender)
        row[4] = addr_with_0x(self.recipient)
        row[7] = "0"
        row[8] = self.value
        row[9] = str(self.client_ts) if self.client_ts >= 0 else ""
        return row


def parse_input_row(row: List[str], line_no: int) -> Optional[TxRow]:
    if len(row) < 9:
        return None
    s, r = row[3].strip(), row[4].strip()
    if len(s) < 42 or len(r) < 42:
        return None
    try:
        normalize_addr(s)
        normalize_addr(r)
    except ValueError:
        return None
    value = row[8].strip()
    if not value:
        return None
    client_ts = -1
    if len(row) > 9 and row[9].strip().lstrip("-").isdigit():
        client_ts = int(row[9].strip())
    return TxRow(
        sender=normalize_addr(s),
        recipient=normalize_addr(r),
        value=value,
        client_ts=client_ts,
        source_index=line_no,
    )


def load_transactions(path: Path, max_rows: int) -> List[TxRow]:
    txs: List[TxRow] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)
        if header is None:
            raise ValueError(f"empty csv: {path}")
        for i, row in enumerate(reader):
            if max_rows > 0 and len(txs) >= max_rows:
                break
            tx = parse_input_row(row, i)
            if tx is not None:
                txs.append(tx)
    return txs


def assign_monotonic_client_ts(txs: List[TxRow], base_ts: int = 1_600_000_000_000, step_ms: int = 1000) -> None:
    for i, tx in enumerate(txs):
        if tx.client_ts < 0:
            tx.client_ts = base_ts + i * step_ms


# ---------------------------------------------------------------------------
# Hot account 与 episode 构造
# ---------------------------------------------------------------------------

@dataclass
class EpisodePlan:
    account: str
    old_tx1: TxRow
    old_tx2: TxRow
    new_tx: TxRow
    client_ts_old1: int
    client_ts_new: int
    client_ts_old2: int


def pick_hot_accounts(
    txs: List[TxRow],
    shard_num: int,
    source_shard: int,
    top_k: int,
    min_tx_count: int,
) -> List[Tuple[str, int]]:
    """返回 [(addr, tx_count), ...]，地址初始在 source_shard。"""
    counter: Counter = Counter()
    by_sender: Dict[str, List[TxRow]] = defaultdict(list)
    for tx in txs:
        counter[tx.sender] += 1
        by_sender[tx.sender].append(tx)

    candidates = []
    for addr, cnt in counter.most_common():
        if cnt < min_tx_count:
            continue
        if addr2shard(addr, shard_num) != source_shard:
            continue
        candidates.append((addr, cnt))
        if len(candidates) >= top_k * 3:
            break
    return candidates[:top_k]


def build_episode_for_account(
    account: str,
    pool: List[TxRow],
    episode_base_ts: int,
    rng: random.Random,
) -> EpisodePlan:
    """从该账户真实交易中抽 3 笔，构造 old-old(new 到达)-new 的 client_ts。"""
    related = [t for t in pool if t.sender == account]
    if len(related) < 2:
        # 退化：复制同一笔并改 recipient
        base = pool[0]
        related = [
            TxRow(base.sender, base.recipient, base.value, tag="episode_old"),
            TxRow(base.sender, base.recipient, base.value, tag="episode_old"),
        ]
    rng.shuffle(related)
    old1 = TxRow(related[0].sender, related[0].recipient, related[0].value, tag="episode_old")
    old2 = TxRow(related[1].sender, related[1].recipient, related[1].value, tag="episode_old")
    # new：优先选第三笔；否则换 recipient 制造 distinct tx
    if len(related) >= 3:
        new_src = related[2]
    else:
        new_src = related[0]
    new_tx = TxRow(new_src.sender, new_src.recipient, new_src.value, tag="episode_new")

    ts1 = episode_base_ts
    ts_new = episode_base_ts + 100
    ts2 = episode_base_ts + 200
    old1.client_ts = ts1
    old2.client_ts = ts2
    new_tx.client_ts = ts_new

    return EpisodePlan(
        account=account,
        old_tx1=old1,
        old_tx2=old2,
        new_tx=new_tx,
        client_ts_old1=ts1,
        client_ts_new=ts_new,
        client_ts_old2=ts2,
    )


def estimate_mig_index(
    max_commit_block: int,
    shard_num: int,
    inject_speed: int,
    max_rows: int,
    inject_interval_sec: int = 2,
) -> int:
    """
    估算 Client 触发 SendMigrateWanted 时的注入行号。

    max_commit_block * shard_num = 跨分片累计 cReply 次数阈值（默认 2×10=20）。
    注入：每 inject_interval_sec 秒注入 inject_speed*2 笔（见 pbft/client.go InjectTXS）。

    实测（Max_Commit_Block=10, Inject_speed=400）：第 20 次 cReply 时约已注入 7200~8000 笔。
    """
    commits = max_commit_block * shard_num
    # 20 次 commit 约 18~22s 墙钟 → 9~11 个注入批次
    wall_sec = max(commits, int(commits * 1.0))
    batches = max(1, wall_sec // inject_interval_sec)
    idx = batches * inject_speed * 2
    idx = max(500, idx)
    if max_rows > 0:
        idx = min(idx, max_rows - 400)
    return idx


def calibrate_mig_index_from_logs(
    log_dir: Path,
    max_commit_block: int,
    shard_num: int,
    inject_speed: int,
    inject_interval_sec: int = 2,
) -> Optional[int]:
    """
    从 log/S*_block.csv 按时间戳合并分片出块顺序，取第 N 次 cReply（N=阈值）估算注入行号。
    若存在 mig1 块，一并写入 meta 供核对。
    """
    log_dir = Path(log_dir)
    blocks: List[Tuple[int, str, int, int]] = []  # ts, shard, height, mig1
    for shard in ("S0", "S1"):
        path = log_dir / f"{shard}_block.csv"
        if not path.is_file():
            continue
        with path.open("r", encoding="utf-8", newline="") as f:
            for row in csv.DictReader(f):
                blocks.append(
                    (
                        int(row["timestamp"]),
                        shard,
                        int(row["blockHeight"]),
                        int(row["mig1"]),
                    )
                )
    if not blocks:
        return None
    blocks.sort(key=lambda x: x[0])
    commits_needed = max_commit_block * shard_num
    if len(blocks) < commits_needed:
        return None
    t0 = blocks[0][0]
    trigger_ts = blocks[commits_needed - 1][0]
    elapsed_sec = max(0.0, (trigger_ts - t0) / 1000.0)
    batches = max(1, int(round(elapsed_sec / inject_interval_sec)))
    return batches * inject_speed * 2


def load_mig_accounts_from_log(
    path: Path,
    limit: int,
    shard_num: int,
    source_shard: int,
    target_shard: int = 1,
) -> List[str]:
    """从 log/S0_mig1.csv 读取上一轮实际迁出账户（优先与 CLPA/PageRank 结果一致）。"""
    if not path.is_file():
        return []
    addrs: List[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                tgt = int(row.get("target", row.get("Target", "1")))
            except ValueError:
                continue
            if tgt != target_shard:
                continue
            raw = row.get("acc") or row.get("address") or ""
            if not raw:
                continue
            addr = normalize_addr(raw.replace("0x", ""))
            if addr2shard(addr, shard_num) != source_shard:
                continue
            if addr not in addrs:
                addrs.append(addr)
            if len(addrs) >= limit:
                break
    return addrs


def collect_recipients_on_shard(
    txs: List[TxRow],
    shard_num: int,
    target_shard: int,
    min_count: int = 1,
) -> List[str]:
    """
    从已加载交易中收集落在 target_shard 上的真实 recipient 地址。
    必须使用数据集中已有账户，否则 Go 创世状态树 st.Get(recipient) 为 nil 会 panic。
    """
    seen: set = set()
    out: List[str] = []
    for tx in txs:
        try:
            if addr2shard(tx.recipient, shard_num) != target_shard:
                continue
            if tx.recipient in seen:
                continue
            seen.add(tx.recipient)
            out.append(tx.recipient)
            if len(out) >= min_count:
                break
        except ValueError:
            continue
    return out


def scan_recipients_on_shard(
    path: Path,
    shard_num: int,
    target_shard: int,
    min_count: int,
    max_scan_rows: int = 500_000,
) -> List[str]:
    """从原始 CSV 扫描更多 target_shard recipient（当 pre-mig 窗口内不足时）。"""
    seen: set = set()
    out: List[str] = []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for i, row in enumerate(reader):
            if i >= max_scan_rows:
                break
            tx = parse_input_row(row, i)
            if tx is None:
                continue
            try:
                if addr2shard(tx.recipient, shard_num) != target_shard:
                    continue
                if tx.recipient in seen:
                    continue
                seen.add(tx.recipient)
                out.append(tx.recipient)
                if len(out) >= min_count:
                    break
            except ValueError:
                continue
    return out


def inject_clpa_bias_txs(
    background: List[TxRow],
    accounts: List[str],
    mig_index: int,
    shard_num: int,
    target_shard: int,
    per_account: int,
    rng: random.Random,
    base_ts: int,
    recipient_pool: List[str],
) -> List[TxRow]:
    """
    在 mig_index 之前插入跨分片偏置交易，提高账户被重分配算法选中的概率。
    替换 background 末尾一段，保持总长度不变。
    recipient 必须来自数据集真实地址（见 collect_recipients_on_shard）。
    """
    if not accounts or per_account <= 0:
        return background
    if not recipient_pool:
        print("[WARN] no cross-shard recipients in dataset; skip CLPA bias injection", file=sys.stderr)
        return background
    need = len(accounts) * per_account
    if mig_index <= need + 50:
        return background
    head = background[: mig_index - need]
    tail = background[mig_index:]
    bias: List[TxRow] = []
    ts = base_ts + 800_000_000
    for acc in accounts:
        pool = [t for t in background if t.sender == acc]
        template = pool[0] if pool else TxRow(acc, recipient_pool[0], "1000000000000000000")
        for _ in range(per_account):
            recip = rng.choice(recipient_pool)
            bias.append(
                TxRow(
                    sender=acc,
                    recipient=recip,
                    value=template.value,
                    client_ts=ts,
                    tag="clpa_bias",
                )
            )
            ts += 10
    return head + bias + tail


def scan_txs_for_accounts(path: Path, accounts: set, limit_per_account: int = 30) -> Dict[str, List[TxRow]]:
    """从完整 CSV 中为指定账户补充交易模板（用于 episode 不在前 max_rows 的情况）。"""
    found: Dict[str, List[TxRow]] = {a: [] for a in accounts}
    if not accounts:
        return found
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        next(reader, None)
        for row in reader:
            tx = parse_input_row(row, -1)
            if tx is None or tx.sender not in accounts:
                continue
            found[tx.sender].append(tx)
            if all(len(found[a]) >= limit_per_account for a in accounts):
                break
    return found


def splice_dataset(
    background: List[TxRow],
    episodes: List[EpisodePlan],
    mig_index: int,
    rng: random.Random,
    buffer_len: int = 180,
) -> Tuple[List[TxRow], Dict]:
    """
    在 mig_index 附近插入 episodes：
      [0 .. mig_index-1)           背景
      mig_index-2, mig_index-1     各 episode 的 2 笔 old（紧凑 burst）
      [mig_index .. mig_index+K)   背景
      依次插入各 episode 的 new
      剩余背景
    """
    if mig_index < len(episodes) * 2 + 100:
        mig_index = len(episodes) * 2 + 100

    head = background[: mig_index - 2 * len(episodes)]
    tail_start = mig_index
    tail = background[tail_start:]

    out: List[TxRow] = []
    out.extend(head)

    old_insertions: List[Dict] = []
    for ep in episodes:
        out.append(ep.old_tx1)
        out.append(ep.old_tx2)
        old_insertions.append(
            {
                "account": ep.account,
                "old1_client_ts": ep.client_ts_old1,
                "old2_client_ts": ep.client_ts_old2,
                "new_client_ts": ep.client_ts_new,
            }
        )

    # 短背景缓冲（迁移窗口：映射下发后、new episode 注入前）
    buffer_len = min(buffer_len, len(tail))
    out.extend(tail[:buffer_len])
    tail_rest = tail[buffer_len:]

    new_insertions: List[Dict] = []
    for ep in episodes:
        out.append(ep.new_tx)
        new_insertions.append({"account": ep.account, "client_ts": ep.client_ts_new})

    out.extend(tail_rest)

    meta = {
        "mig_index": mig_index,
        "old_burst_end_output_index": len(head) + 2 * len(episodes),
        "new_insert_start_output_index": len(head) + 2 * len(episodes) + buffer_len,
        "episodes": old_insertions,
        "new_insertions": new_insertions,
    }
    return out, meta


def validate_episodes(episodes: List[EpisodePlan]) -> List[str]:
    errors: List[str] = []
    for ep in episodes:
        # 逻辑顺序 old-new-old
        ordered = sorted(
            [
                ("old1", ep.client_ts_old1, "old"),
                ("new", ep.client_ts_new, "new"),
                ("old2", ep.client_ts_old2, "old"),
            ],
            key=lambda x: x[1],
        )
        kinds = [x[2] for x in ordered]
        if kinds != ["old", "new", "old"]:
            errors.append(
                f"account {ep.account}: client_ts order {kinds} != old-new-old "
                f"({ep.client_ts_old1}, {ep.client_ts_new}, {ep.client_ts_old2})"
            )
    return errors


def validate_output_order(output: List[TxRow], mig_index: int, episode_count: int) -> List[str]:
    """检查注入顺序：episode new 应在 old burst 之后。"""
    warnings: List[str] = []
    old_burst_end = mig_index
    first_new_idx = None
    for i, tx in enumerate(output):
        if tx.tag == "episode_new":
            first_new_idx = i
            break
    if first_new_idx is not None and first_new_idx < old_burst_end:
        warnings.append(
            f"episode new at output index {first_new_idx} before mig_index {mig_index}"
        )
    return warnings


# ---------------------------------------------------------------------------
# 写文件
# ---------------------------------------------------------------------------

def write_csv(path: Path, txs: List[TxRow]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        # 与原始文件一致：首行可留空或占位
        writer.writerow([""] * 18)
        for tx in txs:
            writer.writerow(tx.to_csv_row())


def write_meta(path: Path, meta: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate MVSS benchmark CSV with client_ts column.")
    p.add_argument("--input", type=Path, default=Path("selectedTxs_300K.csv"), help="源 CSV 路径")
    p.add_argument("--output", type=Path, default=Path("datasets/mvss_interleave_benchmark.csv"))
    p.add_argument("--meta", type=Path, default=Path("datasets/mvss_interleave_benchmark.meta.json"))
    p.add_argument("--mode", choices=["benchmark", "timestamp-only"], default="benchmark",
                   help="benchmark=插入交错 episode；timestamp-only=仅加单调 client_ts")
    p.add_argument("--max-rows", type=int, default=12000, help="最多读取/输出背景行数（0=全量）")
    p.add_argument("--episodes", type=int, default=5, help="交错 episode 账户数")
    p.add_argument("--shard-num", type=int, default=2)
    p.add_argument("--source-shard", type=int, default=0, help="episode 账户初始分片（迁出片）")
    p.add_argument("--min-tx-count", type=int, default=10, help="hot account 最少出现次数")
    p.add_argument("--mig-index", type=int, default=-1, help="迁移触发行号；-1 自动估算/日志校准")
    p.add_argument("--max-commit-block", type=int, default=10,
                   help="与 params.Max_Commit_Block 一致；×分片数=cReply 阈值")
    p.add_argument("--inject-speed", type=int, default=400)
    p.add_argument("--calibrate-log", type=Path, default=None,
                   help="从 log/ 块日志校准 mig_index（如 log）")
    p.add_argument("--mig-accounts-file", type=Path, default=None,
                   help="从 log/S0_mig1.csv 读取 episode 账户（上一轮实测迁移列表）")
    p.add_argument("--clpa-bias-per-account", type=int, default=60,
                   help="每个 episode 账户在 mig_index 前插入的跨分片偏置 tx 数")
    p.add_argument("--buffer-after-mig", type=int, default=180,
                   help="old burst 与 new episode 之间的背景缓冲行数")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--base-ts", type=int, default=1_600_000_000_000, help="client_ts 起始毫秒")
    p.add_argument("--ts-step", type=int, default=1000, help="背景 tx client_ts 步长(ms)")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    rng = random.Random(args.seed)

    if not args.input.is_file():
        print(f"[ERROR] input not found: {args.input}", file=sys.stderr)
        return 1

    print(f"[1/5] Loading {args.input} (max_rows={args.max_rows}) ...")
    txs = load_transactions(args.input, args.max_rows)
    if not txs:
        print("[ERROR] no valid transactions loaded", file=sys.stderr)
        return 1
    print(f"      loaded {len(txs)} transactions")

    assign_monotonic_client_ts(txs, base_ts=args.base_ts, step_ms=args.ts_step)

    if args.mode == "timestamp-only":
        print(f"[2/5] Mode timestamp-only: writing {args.output}")
        write_csv(args.output, txs)
        meta = {
            "mode": "timestamp-only",
            "input": str(args.input),
            "output": str(args.output),
            "row_count": len(txs),
            "client_ts_column": 9,
            "base_ts": args.base_ts,
            "ts_step": args.ts_step,
        }
        write_meta(args.meta, meta)
        print(f"[3/5] Meta -> {args.meta}")
        print("[DONE] timestamp-only dataset ready.")
        return 0

    mig_index = args.mig_index
    calibrated_from_log = None
    if mig_index < 0 and args.calibrate_log is not None:
        calibrated_from_log = calibrate_mig_index_from_logs(
            args.calibrate_log,
            args.max_commit_block,
            args.shard_num,
            args.inject_speed,
        )
        if calibrated_from_log is not None:
            mig_index = calibrated_from_log
            print(f"      mig_index from log calibration: {mig_index}")
    if mig_index < 0:
        mig_index = estimate_mig_index(
            args.max_commit_block, args.shard_num, args.inject_speed, len(txs)
        )
        print(f"      mig_index from formula estimate: {mig_index}")

    episode_accounts: List[str] = []
    if args.mig_accounts_file is not None:
        episode_accounts = load_mig_accounts_from_log(
            args.mig_accounts_file,
            args.episodes,
            args.shard_num,
            args.source_shard,
        )
        if episode_accounts:
            print(f"[2/6] Episode accounts from {args.mig_accounts_file}: {len(episode_accounts)}")
        else:
            print(f"[WARN] no accounts loaded from {args.mig_accounts_file}, fallback to hot pick", file=sys.stderr)

    if not episode_accounts:
        print(f"[2/6] Selecting hot accounts on S{args.source_shard} (episodes={args.episodes}) ...")
        hot = pick_hot_accounts(
            txs[:mig_index], args.shard_num, args.source_shard, args.episodes, args.min_tx_count
        )
        if len(hot) < args.episodes:
            print(
                f"[WARN] only {len(hot)} hot accounts in pre-mig window (wanted {args.episodes})",
                file=sys.stderr,
            )
        if not hot:
            print("[ERROR] no hot account candidates", file=sys.stderr)
            return 1
        episode_accounts = [a for a, _ in hot[: args.episodes]]
    else:
        hot = [(a, 0) for a in episode_accounts]

    target_shard = 1 if args.source_shard == 0 else 0
    bias_recipient_need = max(32, args.episodes * 4)
    print(f"[3/6] CLPA/PageRank bias txs (per_account={args.clpa_bias_per_account}) ...")
    recipient_pool = collect_recipients_on_shard(txs, args.shard_num, target_shard, bias_recipient_need)
    if len(recipient_pool) < bias_recipient_need:
        extra = scan_recipients_on_shard(args.input, args.shard_num, target_shard, bias_recipient_need)
        for addr in extra:
            if addr not in recipient_pool:
                recipient_pool.append(addr)
            if len(recipient_pool) >= bias_recipient_need:
                break
    print(f"      cross-shard recipient pool (S{target_shard}): {len(recipient_pool)}")
    txs = inject_clpa_bias_txs(
        txs,
        episode_accounts,
        mig_index,
        args.shard_num,
        target_shard=target_shard,
        per_account=args.clpa_bias_per_account,
        rng=rng,
        base_ts=args.base_ts + 700_000_000,
        recipient_pool=recipient_pool,
    )

    by_sender: Dict[str, List[TxRow]] = defaultdict(list)
    for tx in txs:
        by_sender[tx.sender].append(tx)
    missing = [a for a in episode_accounts if len(by_sender[a]) < 2]
    if missing:
        print(f"      scanning input for {len(missing)} accounts with few local txs ...")
        extra = scan_txs_for_accounts(args.input, set(missing), limit_per_account=30)
        for a, rows in extra.items():
            by_sender[a].extend(rows)

    episodes: List[EpisodePlan] = []
    for i, acc in enumerate(episode_accounts):
        ep_ts_base = args.base_ts + 900_000_000 + i * 10_000
        episodes.append(build_episode_for_account(acc, by_sender[acc], ep_ts_base, rng))

    errs = validate_episodes(episodes)
    if errs:
        for e in errs:
            print(f"[ERROR] {e}", file=sys.stderr)
        return 1

    print(f"[4/6] Splicing at mig_index={mig_index} (buffer={args.buffer_after_mig}) ...")
    output, splice_meta = splice_dataset(
        txs, episodes, mig_index, rng, buffer_len=args.buffer_after_mig
    )
    warnings = validate_output_order(output, mig_index, len(episodes))
    for w in warnings:
        print(f"[WARN] {w}")

    print(f"[5/6] Writing {args.output} ({len(output)} rows) ...")
    write_csv(args.output, output)

    meta = {
        "mode": "benchmark",
        "input": str(args.input),
        "output": str(args.output),
        "row_count": len(output),
        "client_ts_column": 9,
        "shard_num": args.shard_num,
        "source_shard": args.source_shard,
        "estimated_mig_index": mig_index,
        "calibrated_mig_index_from_log": calibrated_from_log,
        "params_hint": {
            "Max_Commit_Block": args.max_commit_block,
            "MaxInjectTxs": len(output),
            "Inject_speed": args.inject_speed,
            "Shard_num": args.shard_num,
            "MigrateBeforeInject": False,
            "PorC": "PageRank",
            "cReply_threshold": args.max_commit_block * args.shard_num,
            "note": "第 N 次 cReply 后 Client 触发 SendMigrateWanted；episode new 应在映射下发之后注入",
        },
        "episode_account_source": str(args.mig_accounts_file) if args.mig_accounts_file else "hot_accounts_pre_mig",
        "clpa_bias_per_account": args.clpa_bias_per_account,
        "hot_accounts": [{"address": a, "tx_count": c} for a, c in hot[: args.episodes]],
        "episodes": [
            {
                "account": ep.account,
                "client_ts": {
                    "old1": ep.client_ts_old1,
                    "new": ep.client_ts_new,
                    "old2": ep.client_ts_old2,
                },
                "expected_order_by_client_ts": "old-new-old",
                "old_tx1": {"sender": ep.old_tx1.sender, "recipient": ep.old_tx1.recipient, "value": ep.old_tx1.value},
                "old_tx2": {"sender": ep.old_tx2.sender, "recipient": ep.old_tx2.recipient, "value": ep.old_tx2.value},
                "new_tx": {"sender": ep.new_tx.sender, "recipient": ep.new_tx.recipient, "value": ep.new_tx.value},
            }
            for ep in episodes
        ],
        "splice": splice_meta,
        "go_next_steps": [
            "Add ClientTimestamp to core/Transaction",
            "Parse CSV column 9 in Get_Initial_Map_And_TXS",
            "OrderList / DetectInterleave use ClientTimestamp; old/new still use RequestTime vs Mig1Time",
        ],
    }
    write_meta(args.meta, meta)

    print(f"[6/6] Meta -> {args.meta}")
    print("[DONE] benchmark dataset ready.")
    print(f"       episodes: {len(episodes)}, mig_index≈{mig_index}, output rows: {len(output)}")
    if args.calibrate_log:
        print(f"       (mig_index calibrated from {args.calibrate_log})")
    print("       Run after Go changes:")
    print(f"         start_2shard_2node.bat {args.output} MVSS")
    return 0


if __name__ == "__main__":
    sys.exit(main())
