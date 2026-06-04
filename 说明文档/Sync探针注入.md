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
| `SyncProbePhaseBDelayMs` | `0` | Phase B 延迟；`0` = 3×`Block_interval` 秒 |
| `SyncProbeSettleMs` | `0` | Phase A 后等待入池；`0` = 800ms |
| `SyncProbeAccount` | 空 | 指定单个迁出地址 |

Pause tx3 在 NewMap 时由 `mvssProbeEarlyPauseSuffixOld` 自动完成；Phase B 注入真实 tx2 到目标片。

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

### 6.3 sync.csv 列

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

细节见 [`debug.md`](./debug.md) §6.4 条目 32–34。

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

- Phase B 延迟为启发式（默认 6s），探针场景下 tx2 上链可能晚于 sync 若干块；联调可缩短 `SyncProbePhaseBDelayMs`
- `MigrateBeforeInject` 路径未纳入探针
- 序验收建议查 CSV 或后续脚本化；自然数据集交错可逐步替代探针

---

## 11. 相关文档

- [`聚合窗口.md`](./聚合窗口.md) — State_ini delta 出站聚合与窗口参数
- [`账户迁移时序.md`](./账户迁移时序.md) — 迁移阶段时序
- [`MVSS-Delta代码实现讲解.md`](./MVSS-Delta代码实现讲解.md) — Delta 实现概览
- [`debug.md`](./debug.md) — 通用调试与变更记录
