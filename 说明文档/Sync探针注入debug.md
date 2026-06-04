# MVSS-Delta Sync 探针：Debug 指南

> **读者**：后续接手的大模型 / 开发者。  
> **前置阅读**：[`Sync探针注入.md`](./Sync探针注入.md)（探针设计、时序、验收标准）。  
> **本文范围**：当前在修什么、已修什么、用什么日志定位、不要重复踩坑。

---

## 1. 我们在解决什么问题（一句话）

在 **MVSS-Delta** 策略下，用 **Sync 探针** 稳定构造 **old→new→old** 交错，打通 **Stage3 增量同步** 全链路：

```text
S0 发 TXsyncDelta → S1 recv/apply → S1 打包执行探针 tx2 → S1 ack_send
→ S0 ack_recv → S0 恢复打包探针 tx3
```

**当前未闭环**：delta **apply 已成功**，但探针 **tx2 未上链**，因此 **ack / tx3  resume 均未发生**。

---

## 2. 策略与代码分工（共享控制面 / 分叉数据面）

| 层 | MVSS（全量） | MVSS-Delta（增量） |
|----|-------------|-------------------|
| 交错检测、Pause suffix-old、prefix-old 就绪 | 共用 `account/mvss_ctx.go`、`pbft/mvss_sync.go` | 同上 |
| 跨片同步载体 | `core.TXsync`（StateOld/StateNew） | `core.TXsyncDelta`（∆balance/∆nonce） |
| 目标片落盘 | `handleTXsync` → `ApplyMVSSAccountState` | `handleTXsyncDelta` → `ApplyMVSSAccountDelta` |
| 块后编排 | `mvssOnBlockCommitted` | 同上 + `mvssFlushPendingTargetDeltas` |

关键文件：

| 文件 | 作用 |
|------|------|
| `pbft/sync_probe.go` | 探针 Phase A/B 注入、Ingress/Redirect 日志 |
| `pbft/mvss_sync.go` | Stage3 控制面、`mvssOnBlockCommitted`、Pause/Resume |
| `pbft/mvss_delta.go` | Delta 收发、apply、ack、pending 队列 |
| `core/txpool.go` | Stage3 打包约束（Pause、阻塞 new、WaitSyncIni） |
| `chain/blockchain.go` | 出块执行、`ApplyMVSSAccountDelta`、`PendingSyncAck` |
| `account/mvss_ctx.go` | MigAccountCtx FSM、DetectInterleave |
| `pbft/sync_logger.go` | `log/Sx_sync.csv`（仅 N0 写） |

---

## 3. 探针交易 Id（过滤用）

基址 `9000000000`，步长 `10`：

| 账户序号 | tx1 (old) | tx2 (new) | tx3 (old, suffix) |
|----------|-----------|-----------|-------------------|
| 0 | 9000000001 | 9000000002 | 9000000003 |
| 1 | 9000000011 | 9000000012 | 9000000013 |
| 2 | 9000000021 | 9000000022 | 9000000023 |

期望上链分布：**S0 有尾号 1/3，S1 有尾号 2**。

---

## 4. 如何跑实验（含日志落盘）

### 4.1 手动启动（推荐 debug）

```bat
REM 1. 清理上次运行 DB（换数据集或重跑前必做）
REM    删 record\triedb\ 、*_blockchain_db* 、*_blockchain_db.lock

REM 2. 探针开关
set SYNC_PROBE=1

REM 3a. 只看终端
start_2shard_2node.bat selectedTxs_300K.csv MVSS-Delta

REM 3b. 终端 + 落盘（依赖 scripts\tee_go_run.ps1）
start_2shard_2node_log.bat selectedTxs_300K.csv MVSS-Delta
```

- **client** 需 `--enableSyncProbe`（bat 在 `SYNC_PROBE=1` 时自动加）
- **节点** 用 `SYNC_PROBE=1` 开启探针相关日志与 sync CSV 刷盘

### 4.2 两类输出目录

| 路径 | 内容 | 谁写 |
|------|------|------|
| `log/*.csv` | 实验指标（transaction、sync、block…） | Go 程序运行时 |
| `log/runs/run_<时间戳>/*.out.log` | 各窗口 stdout+stderr 副本 | `start_2shard_2node_log.bat` + `tee_go_run.ps1` |

**Debug 时**：CSV 看结构化事件；`.out.log` 看完整终端上下文（grep 关键字、查死锁/循环）。

---

## 5. 预期 vs 当前现象（2026-06 快照）

### 5.1 预期（探针 + MVSS-Delta 通过）

**`log/S0_sync.csv`**

```csv
send,delta → ack_recv,delta
```

**`log/S1_sync.csv`**

```csv
recv,delta → apply,delta (ok=1) → ack_send,delta
```

无 `abort,delta`；无重复 `send,delta`（同一轮）。

**`log/Sx_transaction.csv`**

- 6 个探针 Id（3 账户 × tx1/tx2/tx3）均出现且各 1 次
- 序：`block(tx1) < block(tx2@S1) < block(tx3)`，且 tx3 在 S0 ack 之后

### 5.2 当前典型现象（未通过）

| 观测 | 含义 |
|------|------|
| S0/S1 `_sync.csv` 仅有 `send/recv,delta`，无 `apply/ack` | Stage3 未跑完 |
| S1 N0 反复打印 `SyncApplied 但尚无 new 上链，等待打包` | FSM 已到 SyncApplied，但块内没有 new 交易 commit |
| `[SyncProbe][Exec] tx=900000000x nonce 不匹配 expect=N got=M，跳过执行` | tx2 进块了但执行被 skip，**未真正上链** |
| `Sx_transaction.csv` 搜不到 `900000000x` | 探针三笔均未 commit |
| S0 持续出块但像「空块/少 tx」 | 次生：tx3 被 Pause + sync 未完成，队首无可打包 old |

### 5.3 当前怀疑根因（待修）

```text
delta apply 把链上 nonce 推到 N
→ 池内 tx2 仍带着 apply 之前分配的 nonce（偏大）
→ GenerateBlock/execute 时 nonce 校验失败 → continue 跳过
→ PendingSyncAck 永不为 true → 无 ack_send
→ mvssOnBlockCommitted 只打印「SyncApplied 但尚无 new 上链」
```

相关代码：

- apply 后重编号：`pbft/mvss_sync.go` → `mvssPromoteMigNewTxsToHead`
- 执行时校验：`chain/blockchain.go`（`[SyncProbe][Exec] nonce 不匹配`）
- ack 触发条件：`ctx.FSM == SyncApplied && ctx.PendingSyncAck`（`mvssOnBlockCommitted`）

---

## 6. Debug 查阅顺序（给大模型的 checklist）

### Step 0：确认探针真的触发了

在 `log/runs/.../client.out.log` 或 client 窗口搜：

- `SyncProbe enabled`
- `MigrateWanted` / 迁移划分

在 `S0_N0.out.log` 搜：

- `[SyncProbe]`、`DetectInterleave`、`Pause`

### Step 1：tx2 是否到达 S1 并入池

在 `S1_N0.out.log` 搜：

```text
[SyncProbe][Ingress]
[SyncProbe][Normalize]
[SyncProbe][Redirect]
本片入池
```

期望：tx=`9000000002/012/022`，`shard=S1`，`local=true`。

### Step 2：Delta 同步是否完成 apply

**CSV**：`log/S1_sync.csv`

| event | 说明 |
|-------|------|
| `recv,delta` | 收到 S0 消息 |
| `apply,delta` ok=1 | 校验+落盘成功 |
| `abort,delta` | **失败**，看 reason 列 |

**终端 / S1_N0.out.log**：

```text
[MVSS-Delta] S1 收到分片 S0 的 TXsyncDelta
[MVSS-Delta] 目标片 S1 账户 ... apply delta [start,end)
```

若只有 recv 无 apply：查是否卡在 `sequenceLock`、MigCtx 未就绪、或 pending 队列未 flush（TXmig2 块 commit 后应 flush）。

### Step 3：apply 之后 new 为何没上链

在 `S1_N0.out.log` 搜：

```text
SyncApplied 但尚无 new 上链
SyncProbe][Exec] nonce 不匹配
mvssPromoteMigNewTxsToHead
```

在 `log/S1_transaction.csv` 按 Id `90000000` 过滤——**无行 = 未 commit**。

在 `log/S1_queueLen.csv` / block 日志看 apply 后队列是否很长但探针 tx 排不进块。

### Step 4：ack 与 S0 resume

**S1** 应有 `ack_send,delta`（CSV 或 `[MVSS-Delta] ... delta ack`）。  
**S0** 应有 `ack_recv,delta`，随后：

- `ResumeAfterSyncAck` / suffix-old 提升队首
- tx3（9000000003 等）出现在 `S0_transaction.csv`

### Step 5：源片 sync 发送时机

`log/S0_sync.csv` + `S0_N0.out.log`：

```text
[MVSS+] 源片 S0 账户 ... prefix old 就绪，发送 State_ini sync
```

`send,delta` 应在 S0 **tx1 commit 之后**，且同一账户**不应重复 send**（见 §7 已修 bug）。

---

## 7. 已修复问题（勿重复修）

| 现象 | 根因 | 修复位置 |
|------|------|----------|
| S1 N0 卡死，只有 recv 无 apply | `mvssOnBlockCommitted` 持 `sequenceLock` 时 `mvssFlushPendingTargetDeltas` 再次加锁 → 自死锁 | `pbft/mvss_delta.go`：flush 不再加锁 |
| 第二次 send 导致 S1 `delta 校验失败` / abort | `DetectInterleave` 块后重复触发，FSM 从 SyncOut 打回 PauseOld | `account/mvss_ctx.go`：`FirstNewTxID>0` 或 `FSM>=SyncOut` 则跳过 |
| 重传 delta 误 abort | 目标 nonce 已 ≥ EndN 仍做 PrevHash 校验 | `pbft/mvss_delta.go`：幂等 duplicate 分支 |
| delta 早于 TXmig2 被丢弃 | 账户未入树 | pending 队列 + TXmig2 块后 flush |
| 源片 send 前更新 LastDeltaHash | 目标 PrevHash 链断裂 | send 时不更新，ack 后再更新 |

---

## 8. 日志文件速查表

| 文件 | 用途 |
|------|------|
| `log/S0_sync.csv` / `log/S1_sync.csv` | Stage3 事件时间线（仅 N0） |
| `log/S0_transaction.csv` / `log/S1_transaction.csv` | 已 commit 交易；探针 Id 是否上链 |
| `log/S0_block.csv` / `log/S1_block.csv` | 出块高度、块内 tx 数 |
| `log/migration.csv` | 迁移轮次、账户映射 |
| `log/runs/run_*/S0_N0.out.log` | S0 主节点完整终端 |
| `log/runs/run_*/S1_N0.out.log` | **S1 主节点**（delta/探针/SyncApplied 重点看此文件） |
| `log/runs/run_*/client.out.log` | 注入进度、MigrateWanted、探针 Phase |

**Grep 关键字包**：

```text
MVSS-Delta
SyncProbe
SyncApplied
nonce 不匹配
abort,delta
ack_send
ack_recv
sequenceLock
DetectInterleave
Pause
```

---

## 9. sync.csv 列说明

```csv
ts,event,mode,addr,start_n,end_n,ok,reason,bytes
```

| 列 | 含义 |
|----|------|
| event | `send`/`recv`/`apply`/`abort`/`ack_send`/`ack_recv` |
| mode | `delta` 或 `sync` |
| ok | `1` 成功，`0` 失败 |
| reason | 如 `nonce=1`、`delta 校验失败`、`duplicate` |

注意：CSV 由 N0 在 `./log/` **覆盖创建**（非 append）；每次跑前若需保留请手动备份。`.out.log` 按 `log/runs/run_*` 归档，互不冲突。

---

## 10. Stage3 状态机（目标片 S1，简图）

```text
WaitSyncIni ──(recv+apply delta)──► SyncApplied ──(new tx commit)──► PendingSyncAck
                                        │                                    │
                                        │ (若 new 一直进不了块)                  │
                                        └─► 每块打印「SyncApplied 但尚无 new 上链」
```

源片 S0（简图）：

```text
PauseOld ──(prefix old ready)──► SyncOut ──(send delta)──► 等 ack_recv ──► Active + Resume tx3
```

---

## 11. 下一步修复方向（给接手者）

1. **优先**：delta apply 后 `mvssPromoteMigNewTxsToHead` 是否把探针 tx2 的 `Nonce` 对齐链上 `GetAccountNonce`；或 execute 前对 MigCtx 账户 new tx 重新赋 nonce。
2. 确认 `core/txpool.go` 在 `FSM==SyncApplied` 时**不**再阻塞 new（仅 `WaitSyncIni` 阻塞）。
3. apply 成功后是否应设 `PendingSyncAck` 的替代路径（若 new 已在块内但 nonce skip 导致未标记）。
4. 修完后用 §5.1 验收；**不要**依赖全自动长跑脚本判 PASS（可能有死循环），以 CSV + `S1_N0.out.log` 人工/半自动确认为准。

---

## 12. 相关文档

- [`Sync探针注入.md`](./Sync探针注入.md) — 设计、参数、验收标准
- [`账户迁移时序.md`](./账户迁移时序.md) — 迁移阶段时序（若存在）
- [`debug.md`](./debug.md) — 通用调试笔记（换数据集删 DB 等）

---

*文档随实验进展更新；修改 Stage3 逻辑后请同步更新 §5、§7、§11。*
