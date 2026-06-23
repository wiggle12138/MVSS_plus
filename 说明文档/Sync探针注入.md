# MVSS / MVSS-Delta Sync 探针注入

在不依赖原始数据集偶然时序的前提下，**SyncProbe** 在每次迁移轮次主动注入少量合成交易，稳定构造 **old → new → old** 交错，用于验收 MVSS 全量同步与 MVSS-Delta Stage3 增量同步全链路。

**当前状态**：探针 + MVSS / MVSS-Delta 均已跑通；Stage3 已知 bug 已修复（见 §8）。Delta 出站聚合见 [`聚合窗口.md`](./聚合窗口.md)。

---

## 1. 设计动机

原系统按累计出块触发迁移，块内交易组成受运行时影响，难以保证迁移窗口内必然出现可触发 sync 的交错。SyncProbe 在 client 侧额外注入三笔交易（tx1 old / tx2 new / tx3 old），配合 NewMap 后的 Pause/Resume，使实验可重复。

---

## 2. 每轮迁移故事线

### 2.1 三轴顺序

| 轴 | 顺序 |
|----|------|
| 发起 (ClientTS) | 1 → 2 → 3 |
| 到达 (注入模拟) | 1 → 3 → 2 |
| 执行 (上链) | 1 → sync → 2@目标片 → ack → 3@源片 |

### 2.2 时间线

```text
T0   Phase A：tx1+tx3 → 源片池（模拟到达 1→3，均早于 2）
     · tx1 插队首；tx3 接池尾
T0+  SendNewMap：mvssBuildMigCtx → DetectInterleave → Pause tx3
T*   源片出块：打包 tx1（tx3 在池内等待）
T0+Δ Phase B：tx2 → 源片 → 重定向 → 目标片池
T†   源片 prefix-old 提交后发 sync/delta；目标片 apply → 执行 tx2 → ack
T‡   源片 ack_recv → Resume → 打包执行 tx3
```

### 2.3 步骤

```text
1) runMigrationFromPending 计算 new_addr2shard
2) 选取迁出账户（默认最多 3 个，可配置）
3) Phase A：向源片注入 tx1、tx3
4) SendNewAddr2Shard：MigCtx + 提前 Pause tx3
5) Phase B：延迟后注入 tx2，重定向到目标片
6) Stage3 同步（MVSS 全量 / MVSS-Delta 增量）
7) 后续 Ann/NS 收敛
```

---

## 3. MVSS 与 MVSS-Delta 分工

| 层 | MVSS | MVSS-Delta |
|----|------|------------|
| 交错检测、Pause/Resume | 共用 `account/mvss_ctx.go`、`pbft/mvss_sync.go` | 同上 |
| 跨片载体 | `core.TXsync` | `core.TXsyncDelta` |
| 目标片落盘 | `handleTXsync` | `handleTXsyncDelta` → apply |
| 块后编排 | `mvssOnBlockCommitted` | 同上 + pending delta flush + **出站 delta 聚合** |

Stage3 期望链路（Delta）：

```text
S0 send,delta → S1 recv/apply → S1 执行 tx2 → S1 ack_send
→ S0 ack_recv → S0 Resume → tx3 上链
```

---

## 4. 适用条件与参数

| 项 | 要求 |
|----|------|
| 策略 | `-m MVSS` 或 `-m MVSS-Delta` |
| 迁移路径 | pending → 重分配 → `SendNewAddr2Shard`（非 `MigrateBeforeInject`） |
| Client 开关 | `EnableSyncProbe=true` 或 `--enableSyncProbe` |
| 节点日志 | 环境变量 `SYNC_PROBE=1`（sync CSV 刷盘、探针诊断日志） |

| 字段 / CLI | 默认 | 说明 |
|------------|------|------|
| `SyncProbeMaxAccounts` | `3` | 每轮探针账户数（可扩，不硬绑 3） |
| `SyncProbePhaseBDelayMs` | `0` | Phase B 延迟；`0` = **2×`Block_interval`**（ms）；见下方 §4.1 |
| `SyncProbeSettleMs` | `0` | Phase A 后等待入池；`0` = 800ms |
| `SyncProbeAccount` | 空 | 指定单个迁出地址 |

Pause tx3 在 NewMap 时由 `mvssProbeEarlyPauseSuffixOld` 自动完成；Phase B 注入真实 tx2 到目标片。

### 4.1 Phase B 延迟参数优化说明

**约束**：tx2 到达 S1 时，S1 必须已通过 `handleTXmig2Msg` 为迁出账户建立 `MigCtx`（`FSM=WaitSyncIni`）。若 tx2 早于 MigCtx 到达，tx2 会被无 `WaitSyncIni` 阻拦地过早打包，`PendingSyncAck` 无法被设置，Stage3 同步将永久卡死于 *"SyncApplied 但尚无 new 上链"*。

**TXmig2 到达 S1 的时序**：

```
NewMap 发出
  → TXmig1 出块：约 1 × Block_interval
  → TryTXmig1 协程逐账户读 trie + Merkle 证明 + TCP 发送：实测 3~4 账户约 100~600ms
  → TXmig2 到达 S1 并建立 MigCtx：≈ 1 × Block_interval + 100~600ms
```

**参数选取（`Block_interval=2s` 场景）**：

| `SyncProbePhaseBDelayMs` | 结果 | 备注 |
|--------------------------|------|------|
| 200ms | ❌ 卡死 | MigCtx 未建立，tx2 过早打包 |
| 2500ms | ❌ 部分竞态 | TryTXmig1 协程对某些账户仍未完成 |
| **4000ms（2×Block，当前默认）** | ✅ 全部正常 | 为 TryTXmig1 提供充足余量 |
| 6000ms（原默认） | ✅ 正常 | 保守，tx2 晚约 2 块 |

> `SyncProbePhaseBDelayMs=0` 时自动取 `2 × Block_interval`，随出块间隔自适应。

---

## 5. 探针 Id 约定

基址 `9000000000`，步长 `10`（`core.SyncProbeIDBase` / `SyncProbeIDStride`）：

| 账户序号 | tx1 | tx2 | tx3 |
|----------|-----|-----|-----|
| 0 | 9000000001 | 9000000002 | 9000000003 |
| 1 | 9000000011 | 9000000012 | 9000000013 |
| 2 | 9000000021 | 9000000022 | 9000000023 |

账户序号 N 通用公式：txk Id = `9000000000 + N×10 + k`（k=1,2,3）。

---

## 6. 启动与日志

### 6.1 启动

```bat
set SYNC_PROBE=1
start_2shard_2node_log.bat selectedTxs_300K.csv MVSS-Delta
```

换数据集或重跑前删除 `record/triedb/`、`*_blockchain_db*`。

### 6.2 输出位置

| 路径 | 内容 |
|------|------|
| `log/Sx_sync.csv` | Stage3 事件（仅各分片 N0 写，每次覆盖） |
| `log/Sx_transaction.csv` | 已 commit 交易 |
| `log/runs/run_*/Sx_N0.out.log` | 终端完整副本（`tee_go_run.ps1`） |

### 6.3 探针结果分析脚本

跑完后可用 `scripts/analyze_sync_probe.py` 从两个角度生成报告：

```bat
python scripts/analyze_sync_probe.py --log-dir log --out-dir results/sync_probe
```

| 输出 | 内容 |
|------|------|
| `probe_report.md` | 可读摘要：交易上链序 + sync 通信时序 |
| `probe_tx_detail.csv` | 探针交易明细（`probe_type` / `client_ts` / 块高） |
| `sync_timeline.csv` | S0+S1 sync 事件按 `ts` 合并排序，含流向标注 |

**交易视角**：检查 tx1/tx2/tx3 是否各出现 1 次、分片是否正确、块序是否 `tx1 < tx2 < tx3`。  
**sync 视角**：按时间线展示 `S0 send → S1 recv/apply → S1 ack_send → S0 ack_recv` 全链路。

### 6.4 sync.csv 列

`ts,event,mode,addr,start_n,end_n,ok,reason,bytes`

- `event`：`send` / `recv` / `apply` / `abort` / `ack_send` / `ack_recv`
- `mode`：`delta` 或 `sync`
- Delta 批量发送时 `reason=batch=N`

---

## 7. 验收标准

### 7.1 交易侧（`log/Sx_transaction.csv`）

1. 探针 Id 均出现且各 **1 次**（幂等）
2. **S0** 仅有尾号 1/3；**S1** 仅有尾号 2
3. 块序（`blockHeight`）：`block(tx1) < block(tx2@S1) < block(tx3)`；tx1 与 tx3 **不同块**；tx3 在 ack 之后

### 7.2 sync 通路

**MVSS**：源片 `send,sync` → 目标片 `recv/apply` → 源片 `ack_recv`。

**MVSS-Delta**（3 账户典型）：

| 分片 | 期望 |
|------|------|
| S0 | `send,delta`（`batch=3`）→ `ack_recv,delta` ×3 |
| S1 | `recv,delta`（`batch=3`）→ `apply,delta` ×3 → `ack_send,delta` ×3 |

全程 `mode=delta`，无 `abort,delta`。

### 7.3 排查顺序（未通过时）

1. **client.out.log**：`SyncProbe enabled`、MigrateWanted
2. **S1_N0.out.log**：`[SyncProbe][Ingress]` tx2 入池；`apply delta`；无 `nonce 不匹配`
3. **S1_sync.csv**：recv → apply → ack_send 完整
4. **S0_sync.csv**：send 在 tx1 之后；ack_recv 后 tx3 出现在 transaction.csv

常用 grep：`SyncProbe`、`SyncApplied`、`abort,delta`、`ack_send`、`DetectInterleave`、`Pause`。

---

## 8. 已修复问题（摘要）

| 现象 | 处理 |
|------|------|
| S1 apply 后 tx2 nonce 不匹配、无 ack | apply 后 / Phase B 入池后 `mvssPromoteMigNewTxsToHead` 按链上 nonce 重编号 |
| sync.csv short write | `writeSyncLog` 持锁 Write+Flush；bat 向节点传 `SYNC_PROBE=1` |
| sequenceLock 自死锁 | pending flush 不再嵌套加锁 |
| 重复 send / delta 校验失败 | `DetectInterleave` 在 FSM≥SyncOut 时跳过 |
| delta 早于 TXmig2 | pending 队列 + mig2 块后 flush |
| 源片 send 前更新 LastDeltaHash | 改为 ack 后更新 |

细节见 [`debug.md`](./debug.md) 6.4 条目 32–34。

---

## 9. 关键代码

| 文件 | 作用 |
|------|------|
| `pbft/sync_probe.go` | Phase A/B 注入 |
| `pbft/mvss_sync.go` | Stage3 控制面、`mvssOnBlockCommitted` |
| `pbft/mvss_delta.go` | Delta 收发、apply、ack、出站聚合 |
| `pbft/sync_logger.go` | `Sx_sync.csv` |
| `account/mvss_ctx.go` | MigCtx FSM、DetectInterleave |
| `core/txpool.go` | Pause / WaitSyncIni 打包约束 |

---

## 10. 局限

- Phase B 延迟默认 `2×Block_interval`（Block_interval=2s 时约 4s），tx2 上链通常晚于 sync 1~2 块；这是探针引入的额外等待，非协议本身延迟（详见 §4.1）
- `MigrateBeforeInject` 路径未纳入探针
- 探针构造的是**极端但可复现**的 Stage3 时序场景，不代表主网日常负载中交错的发生频率；自然负载下的 RLT/TPS/RDT 等主实验仍应关闭探针单独报告（详见 §12）

---

## 11. 相关文档

- [`聚合窗口.md`](./聚合窗口.md) — State_ini delta 出站聚合与窗口参数
- [`账户迁移时序.md`](./账户迁移时序.md) — 迁移阶段时序
- [`账户迁移策略对比.md`](./账户迁移策略对比.md) — MVSS-Delta 机制与代码 §6
- [`期刊实验规划.md`](./期刊实验规划.md) — 六大实验与探针分工
- [`实验指标定义.md`](./实验指标定义.md) — 指标公式与探针验收字段
- [`debug.md`](./debug.md) — Stage3 变更摘要

---

## 12. 实验设计：为何采用 Sync 探针（探索过程与论文表述）

本节记录团队在期刊实验设计阶段，围绕「能否用真实数据集自然触发 Stage3 同步、是否必须依赖探针」所进行的工程排查与结论。内容可直接改写进论文 **Experimental Setup** 或 **Stage-3 Evaluation** 小节。

### 12.1 问题背景

MVSS / MVSS-Delta 的核心机制之一，是在账户迁移窗口内处理 **时间戳交错（timestamp interleaving）**：对同一迁出账户，若按客户端逻辑顺序应为 old → new → old，而各分片因到达时序与跨片 relay 导致无法按该顺序直接执行，则源片与目标片须经 Stage3（`TXsync` 或 `TXsyncDelta`）做多版本状态桥接。期刊版 MVSS-Delta 进一步要求在该路径上证明增量同步相对全量 sync 的带宽与消息开销优势。

因此，实验除对比 SOTA-Lock / Fine-tuned-Lock 的 RLT、延迟与吞吐外，还必须 **可重复地触发并验收 Stage3 全链路**。团队在实现 MVP 后面临的问题是：若关闭探针、仅依赖当前默认数据集与注入逻辑，能否在仿真中自然出现交错并触发 sync？若不能，仅用探针是否会在审稿时被质疑为「人造场景、实际不重要」？

### 12.2 仿真中的两种时间及其与协议的关系

结合 MVSS 设计（Section III）与本仓库实现，迁移期每笔关联交易涉及两类时间语义：

| 语义 | 代码字段 | 含义 |
|------|----------|------|
| **逻辑序 / 客户端发起顺序** | `ClientTimestamp`（写入 `OrderList`） | 交易「应当」被执行的全局顺序；`DetectInterleave` 按此排序判定是否出现 old–new–old |
| **到达序 / 分片收到时刻** | `RequestTime`（写入 `ArrivalList`；与 `Mig1Time` 比较） | 交易实际进入交易池的时刻；用于区分 TX_old 与 TX_new |

时间戳交错的本质是：**逻辑序上存在 old–new–old，而到达序与跨片处理进度使得源片与目标片无法在不经过 sync 的情况下按该逻辑序提交状态**。这与「用户先发起、后因网络与 relay 导致分片收到顺序不一致」的直觉一致，也是 Sync 探针在 Phase A/B 中分别控制 ClientTS 与注入先后所要模拟的情形。

### 12.3 对「真实数据集自然触发 sync」的排查结论

团队对当前实验数据链做了逐项核对，结论如下。

**（1）默认 `selectedTxs_300K.csv`。** 该文件仅保留 from / to / amount 等字段，第 10 列（index 9）本用于 `ClientTimestamp` 的解析位置为空。Client 在 `InjectTXS` 中对 `ClientTimestamp ≤ 0` 的交易将其设为 `RequestTime`（注入时刻），因而 **逻辑序与到达序被绑定为同一序列**。在此设定下，对任意迁出账户，`DetectInterleave` 所需的「逻辑 old–new–old、到达上 new 介于两笔 old 之间」在结构上无法成立；关闭探针后 **几乎不可能** 触发 Stage3 sync。问题不在 CLPA 或迁移频率，而在 **数据与注入语义未提供可分离的双时间轴**。

**（2）XBlock 原始 `*_BlockTransaction.csv`。** XBlock-ETH 公开导出中确有 `timestamp` 字段（通常为该笔交易所属 **区块的打包时间**，index 1），并非用户钱包「点击发送」的细粒度时刻；**同一区块内多笔交易往往共享相同 timestamp**，块内顺序需依赖 blockNumber 与 transaction index 等另行构造全序。即便将区块时间映射为 `ClientTimestamp`、用注入速率模拟 `RequestTime`，仍是对链上粗粒度顺序与仿真到达的 **再建模**，且迁移窗口下的跨分片并行在真实主网中本不存在，交错频率仍高度依赖实验参数。此外，本仓库对 XBlock 全量格式的解析曾误将 index 9（gasLimit）当作 ClientTimestamp，若未修正则无法正确利用链上时间信息。

**（3）Benchmark 拼接数据集（如 `datasets/mvss_interleave_benchmark.csv`）。** 在 300K 真实地址与金额上手工写入 client_ts 并调整行序，可在工程上逼近交错，但 **逻辑时间序仍为实验者构造**，与探针同属「受控时序场景」，并不能单独支撑「完全来自公开数据、未经任何时序假设」的 Stage3 验收叙事。

**（4）学组结论（数据集路径的根本限制）。** 公开数据集中广泛可用的时间戳是 **区块打包时间**，而非 **交易发起时刻的细粒度时间戳**。同一区块内交易时间戳相同，无法恢复「谁先发起、谁后发起」；而 MVSS Stage3 的交错判定恰恰依赖 **客户端逻辑顺序（OrderList）** 与 **到达/迁移分界（RequestTime vs Mig1Time）** 的分离。因此，**依赖现有公开 CSV 直接复现 Stage3 交错，在信息论意义上走不通——不是团队回避真实数据，而是数据本身不具备所需字段。** 在公开数据上补充任意 client_ts 列，本质仍是实验假设而非链上观测。

### 12.4 会议版 MVSS 论文实验与此缺口的关系

TrustCom 会议版 MVSS 在 Section III 描述了 arrival 分 old/new、client timestamp 建 order list 及交错定义 \(TS^{old} < TS^{new} < TS^{old}\)，Section V 报告 BlockEmulator + XBlock 20 万笔、CLPA 周期性迁移及 RLT / Latency / TPS / **RDT** 等指标。但 **实验章节未说明** ClientTimestamp 与 RequestTime 如何从 XBlock 映射、如何统计 RDT 分子 \(TX_{ti}\)、亦 **未报告 TXsync 触发次数或 Stage3 带宽**。Overhead 讨论中亦将交错表述为 **special scenario**，主实验侧重锁方案乱序（RDT > 0）与 MVSS 有序（RDT = 0）的对比，而非 Stage3 发生频率。换言之，**「在仿真中如何可复现地触发并度量 Stage3」在原文中即为留白**；本仓库的 Sync 探针与 `S*_sync.csv` 日志，正是为期刊 MVSS-Delta 补齐 **Stage3 正确性与增量 sync 开销** 的实验基础设施。原文未写清，反说明本工作所补的是 **方法学上的一环**，而非重复已有实验设置。

### 12.5 Sync 探针的方案定位

综合上述排查，团队确认 **Sync 探针注入在实验设计上站得住脚**，无需因「未从原始 CSV 自然触发 sync」而削弱说服力。探针的定位如下：

| 层次 | 手段 | 目的 |
|------|------|------|
| **正确性 / Stage3 机理** | 开启 `EnableSyncProbe`，构造 old→new→old | 验收 `DetectInterleave`、Pause/Resume、`TXsync` / `TXsyncDelta` 全链路；对比 MVSS 与 MVSS-Delta 的 DSR、消息数、字节数 |
| **主实验（性能对比）** | 关闭探针，使用 300K / XBlock 子集 | 报告 RLT、Latency、TPS、RDT 等；锁基线 vs MVSS 系；**不将探针结果替代主结论** |
| **诚实边界** | 正文与附录分开展示 | 探针 = 受控微基准（controlled microbenchmark）；主实验 = 迁移窗口整体性能与自然乱序（RDT） |

探针并非声称「生产环境中每日大量发生相同交错」，而是证明：**当交错这一协议必须处理的极端情形出现时，实现是正确的，且 Delta 相对全量 sync 具有可度量的同步开销优势。** 会议论文已用 RDT 论证 MVSS 相对锁方案在 **有序执行** 上的优势；期刊增量则在 **交错已发生** 的前提下论证 **同步路径的效率**，二者互补。

### 12.6 实验章节建议结构（与 [`实验指标定义.md`](./实验指标定义.md) 一致）

1. **Workload & migration trigger**：XBlock / 300K 子集、CLPA、Relay、`Max_Commit_Block` 等（与会议设置对齐）。  
2. **Main results（无探针）**：SOTA-Lock / Fine-tuned-Lock / MVSS / MVSS-Delta 的 Latency、RLT、TPS、RDT。  
3. **Stage-3 microbenchmark（有探针）**：`SyncProbeMaxAccounts`、MVSS vs MVSS-Delta；指标来自 `S*_sync.csv` 与 `scripts/analyze_sync_probe.py` / `metrics_definitions.py`（DSR、sync 延迟、带宽、abort 率）。  
4. **Limitation**：公开数据缺乏交易发起细粒度时间戳；探针用于 Stage3 可复现验收；交错在生产迁移中的发生频率未在本文单独建模，留作未来工作。

### 12.7 论文可直接引用的表述（英文 / 中文）

**中文（建议放入实验设置或讨论段）：**

> 公开以太坊数据集（如 XBlock-ETH）提供的 timestamp 为区块级打包时间，同一区块内多笔交易往往相同，无法恢复客户端发起顺序。MVSS 在 Stage3 所依赖的时间戳交错判定，则需要区分客户端逻辑顺序（OrderList）与交易到达分片的时刻（RequestTime 相对迁移起点）。在缺乏细粒度发起时间戳的前提下，无法仅凭原始 CSV 与匀速注入稳定、可复现地触发 Stage3 同步路径。因此，本文在 Stage3 与 MVSS-Delta 增量同步的评估中，采用 **Sync 探针** 在每次迁移轮次主动注入少量合成交易，构造 old→new→old 的极端时序场景，以验证实现正确性与 Delta 相对全量 sync 的带宽优势；迁移窗口内的整体性能（RLT、延迟、TPS、RDT）则在关闭探针的真实负载实验中单独报告。该做法是对会议版 MVSS 实验中 Stage3 可复现评估缺口的方法学补充，而非用探针结果替代主实验结论。

**English (for paper):**

> Public Ethereum traces (e.g., XBlock-ETH) expose block-level timestamps rather than fine-grained transaction initiation times; transactions within the same block often share identical timestamps and thus do not reveal client-side submission order. MVSS Stage-3 interleaving detection, however, requires separating logical order (OrderList) from shard arrival times (RequestTime relative to migration start). Without initiation timestamps, raw CSV replay with uniform injection cannot stably or reproducibly exercise the Stage-3 synchronization path. We therefore employ **Sync probe injection**—injecting a small number of synthetic transactions per migration round to construct extreme old→new→old interleaving—for evaluating correctness of Stage-3 and the bandwidth gains of MVSS-Delta over full-state sync. Overall migration-window performance (RLT, latency, TPS, RDT) is reported separately under real workloads with probes disabled. This methodology fills a reproducibility gap left open in the conference MVSS evaluation rather than substituting probe outcomes for main experimental claims.

### 12.8 探索过程时间线（内部记录）

```text
1. 实现 MVSS-Delta MVP + Sync 探针，跑通 send→apply→ack 与 DSR 日志
2. 规划期刊六大实验；固化指标于 实验指标定义.md + metrics_definitions.py
3. 讨论探针 vs 真实数据：担心「自然不触发 sync → 只能探针 → 工作不重要」
4. 代码级确认：300K 无 ClientTS；InjectTXS 将 ClientTS=RequestTime → 结构性无法交错
5. 调研 XBlock：有 block timestamp，无发起时刻；同块同 ts；与 OrderList 需求不匹配
6. 研读会议版 Section V：主实验未定义双时间映射、未报告 sync 触发率
7. 学组结论：公开数据不具备细粒度发起顺序 → 探针为合理且必要的 Stage3 验收手段
8. 实验叙事定稿：主实验（无探针）+ Stage3 微基准（有探针）+ 正文一句 limitation
```
