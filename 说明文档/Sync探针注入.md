# MVSS / MVSS-Delta Sync 探针注入说明

本文系统梳理“账户迁移阶段 sync 触发测试”所做改造、运行流程、验证标准与局限。

目标：在不依赖原始数据集偶然时序的前提下，稳定制造 **old → new → old** 交错，从而验证 MVSS / MVSS-Delta 的 sync 通路。

---

## 1. 背景与改造动机

原系统按“累计出块数”触发迁移，而块内交易组成受运行时影响，导致：

- 仅靠改数据集，难以保证迁移窗口中必然出现可触发 sync 的交错交易；
- 实验可重复性不足，sync 触发具有偶然性。

为此，新增 **SyncProbe** 机制：在每轮迁移时由 client 额外注入少量合成交易，主动构造交错关系。

---

## 2. 本次修改点（按模块）

### 2.1 触发与注入（client / sync_probe）

- `runMigrationFromPending` 算出 `new_addr2shard` 后，先执行 Phase A，再发 `NewMap`，随后延迟执行 Phase B。
- 对每个探针账户生成三笔探针交易：
  - `tx1`（old）
  - `tx2`（new）
  - `tx3`（old）
- 默认每轮最多 3 个账户（可改为 2；见参数）。

### 2.2 路由与上链稳定性（MVSS 路径）

- Phase B 的 `tx2` 收款方固定在目标片；
- `tx2` 强制 relay 语义，减少迁移窗口映射切换导致的丢弃；
- 源片对 new 交易执行重定向，目标片收到带 `RedirectTag` 的交易后本地入池。

### 2.3 观测与日志

- 新增/强化探针日志（Ingress/Redirect）用于定位 tx2 是否生成、是否重定向、是否入池；
- `Sx_sync.csv` 由每分片 `N0` 统一写；
- 探针模式下 sync 日志按事件刷盘，降低“终端已触发但 CSV 为空”的概率；
- 日志模式同时识别 `EnableSyncProbe=true` 与环境变量 `SYNC_PROBE=1`。

---

## 3. 适用条件

| 项 | 要求 |
|----|------|
| 策略 | `-m MVSS` 或 `-m MVSS-Delta` |
| 迁移路径 | pending → 重分配算法 → `SendNewAddr2Shard`（非 `MigrateBeforeInject`） |
| 开关 | `EnableSyncProbe=true` 或环境变量 `SYNC_PROBE=1`（建议两者至少一项明确开启） |

---

## 4. 每轮迁移的完整故事线

### 4.1 时间线（设计意图）

```text
【触发】client 提交计数 → MigrateWanted → 划分 → 探针开始

T0   Phase A：tx1+tx3 → S0 池（模拟到达 1→3，均早于 2）
     · tx1 插队首；tx3 接在池尾（避免与 tx1 同批抢块）
T0+  SendNewAddr2Shard：mvssBuildMigCtx
     · 为池内 old 赋 nonce；若已有探针 tx1+tx3 → 逻辑 tx2 槽位 DetectInterleave → Pause tx3
T*   S0 出块：仅打包 tx1（tx3 在池内等待）
T0+Δ Phase B：tx2 → S0 → 重定向 → S1 池（真实 new）
T†   S0 prefix-old 提交后发 sync；S1 apply → 执行 tx2 → ack
T‡   S0 ack_recv / ResumeAfterSyncAck → 再打包执行 tx3
```

三轴对照：

| 轴 | 顺序 |
|----|------|
| 发起 (ClientTS) | 1 → 2 → 3 |
| 到达 (注入模拟) | 1 → 3 → 2 |
| 执行 (上链) | 1 → sync → 2@S1 → ack → 3@S0 |

### 4.2 步骤列表

```text
1) runMigrationFromPending 计算 new_addr2shard
2) 选取迁出账户（默认最多 3 个）
3) Phase A：向源片注入 tx1、tx3（old）
4) SendNewAddr2Shard：S0 mvssBuildMigCtx + 提前 Pause tx3
5) Phase B：延迟后向 S0 注入 tx2，重定向到 S1
6) S0 发 TXsync；S1 recv/apply；S1 执行 tx2 后 ack；S0 再执行 tx3
7) 后续 Ann/NS 收敛
```

---

## 5. 参数与推荐值

| 字段 / CLI | 默认 | 说明 |
|------------|------|------|
| `EnableSyncProbe` / `--enableSyncProbe` | `false` | 探针总开关（client 侧） |
| `SYNC_PROBE`（环境变量） | 空 | 节点侧日志模式开关，推荐设 `1` |
| `SyncProbeMaxAccounts` / `--syncProbeMaxAccounts` | `3` | 每轮探针账户数；若你要“前两个账户”，设为 `2` |
| `SyncProbePhaseBDelayMs` | `0` | Phase B 延迟，`0`=3×`Block_interval` 秒 |
| `SyncProbeSettleMs` | `0` | Phase A 注入后等待入池，`0`=800ms |
| `SyncProbeAccount` / `--syncProbeAccount` | 空 | 指定某地址作为探针账户（需属于迁出集合） |

Pause tx3 在 NewMap 时由 `mvssProbeEarlyPauseSuffixOld` 自动完成（逻辑 tx2 槽位仅写入 OrderList）；Phase B 仍注入并上链真实 tx2。

---

## 6. 探针 Id 约定

| 账户序号 | tx1 | tx2 | tx3 |
|----------|-----|-----|-----|
| 0 | 9000000001 | 9000000002 | 9000000003 |
| 1 | 9000000011 | 9000000012 | 9000000013 |
| 2 | 9000000021 | 9000000022 | 9000000023 |

步长 `10`（`syncProbeIDStride`），便于在 `Sx_transaction.csv` 过滤。

---

## 7. 验证标准（两部分）

### 7.1 交易侧验证（是否“顺序且上链”）

主文件：`log/Sx_transaction.csv`

检查点：

1. 注入交易是否出现（`900000000x / 001x / 002x`）；
2. 是否满足“每个探针账户三笔交易最终都上链”；
3. 同一 `txid` 是否仅出现一次（幂等性，防重复上链）。

**序验收（与 §4.1 执行轴一致，按 `blockHeight` 列）：**

| 检查项 | 期望 |
|--------|------|
| S0 仅有 tx1、tx3（Id 尾 1/3） | S1 仅有 tx2（Id 尾 2） |
| `block(tx1) < block(tx3)` | tx1 与 tx3 **不同块** |
| `block(tx1) ≤ block(send sync)` | sync 在 prefix-old 提交之后 |
| `block(tx2@S1) > block(apply)` | new 在 apply 之后执行 |
| `block(tx3) > block(ack_recv)` | suffix-old 在 ack 之后 |

### 7.2 sync 通路验证（是否正确执行）

主文件：`log/Sx_sync.csv` + 分片终端日志

检查点（MVSS）：

- 源片有 `send,sync`；
- 目标片有 `recv/apply`；
- 源片有 `ack_recv`（或终端出现“收到目标片应答”）。

检查点（MVSS-Delta）：

- S0：`send,delta` → `ack_recv,delta`（或 `ack,delta` 事件）；
- S1：`recv,delta` → `apply,delta` → `ack_send,delta`；
- 全程 `mode=delta`，且无 `abort,delta`。

---

## 8. 关于“顺序”的判定口径（重点）

1. **到达序（模拟）**：先注 tx1+tx3、延迟注 tx2 → 到达 1→3→2（无丢包，用注入间隔代替网络延迟）。
2. **执行序（协议）**：S0 先 commit tx1 → sync/ack → S1 commit tx2 → S0 再 commit tx3；靠 **NewMap 后 Pause tx3** + **ack 后 Resume** 保证。
3. **区块邻接（不要求）**：三笔之间可夹普通交易；但 **tx1 与 tx3 不得同块**（否则 Pause 过晚）。

“夹杂普通交易”不影响通过；**tx3 早于 tx2 上链** 或 **tx1 与 tx3 同块** 视为序验收失败。

---

## 9. 幂等性与一致性检查建议

### 9.1 幂等性（防重复上链）

- 检查 `Sx_transaction.csv` 中同一探针 `txid` 的出现次数应为 1；
- 若出现 >1，视为潜在重复提交问题，需回查转发/重试路径。

### 9.2 回滚/重放一致性（有依赖场景）

若 old/new/old 之间存在余额依赖，应关注：

- nonce 是否单调推进；
- sync 应答后源/目标状态是否一致收敛；
- 重放同一探针批次时，是否出现重复记账或状态分叉。

当前实现依赖迁移上下文与标签校验降低风险，但严格“可重放一致性”仍建议加离线核对脚本（后续工作）。

---

## 10. 启动示例

```bat
set SYNC_PROBE=1
start_2shard_2node.bat selectedTxs_300K.csv MVSS
```

若希望“只测前两个账户”：

- `SyncProbeMaxAccounts=2` 或 client 传 `--syncProbeMaxAccounts 2`。

---

## 11. 当前局限

- `Mig1Time` 由源片 `handleNewMap` 本地生成，Phase B 延迟仍是启发式参数；
- `MigrateBeforeInject` 路径尚未纳入同等探针流程；
- 序验收仍建议人工查 `blockHeight` 或后续脚本化；
- sync 日志可观测性已增强，但建议后续补充“按轮次汇总”的自动分析脚本。
