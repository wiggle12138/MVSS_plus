# MVSS-Delta 代码实现讲解

本文面向区块链分片方向同行，说明三件事：

1. MVSS 在解决什么问题、核心机制是什么。  
2. MVSS-Delta 相比 MVSS 在同步阶段改进了什么（含滑动窗口设计）。  
3. 当前仓库如何把策略落到代码，以及预期会优化哪些指标。

---

## 1. 问题背景：为什么需要 MVSS

在账户迁移（source shard -> target shard）期间，传统全锁/半锁方案常见问题是：

- 迁移相关交易被大量锁住，迁移窗口内延迟上升；
- 源片与目标片并行处理时，如果没有统一顺序与状态桥接，容易出现 old/new 交易交错导致的乱序；
- 同步如果总是传整状态，跨分片带宽与处理开销偏大。

MVSS 的核心目标是：在保证顺序一致性的前提下，尽量减少锁和阻塞。

---

## 2. MVSS 核心机制（Phase1）

可以把 MVSS 看成“分流 + 同步桥接”：

- **交易分流**  
  对迁移账户相关交易按时间切分为 `TX_old` 和 `TX_new`。  
  `TX_old` 由源片继续处理；`TX_new` 从源片重定向到目标片。

- **顺序约束**  
  源片维护迁移账户上下文（如 `OrderList`、`LastCN`），用 nonce 与重定向标签约束重放/双花。

- **交错场景同步**  
  当检测到 old-new-old 交错时，触发同步阶段，源/目标通过 `TXsync` 进行状态桥接。

在本仓库里，MVSS 主线入口集中在：

- 策略入口：`params/migration_strategy.go`
- 分流与同步主逻辑：`pbft/mvss_sync.go`
- 迁移总线：`TXmig1 -> TXmig2 -> Announce -> CaP`

---

## 3. MVSS-Delta 相比 MVSS 改进在哪里

### 3.1 同步消息内容变化

MVSS（全量 sync）在交错同步时倾向于传状态快照（`StateOld/StateNew`）。  
MVSS-Delta 改为发送增量消息 `TXsyncDelta`，核心字段包括：

- `DeltaBalance`
- `DeltaNonce`
- `StartN/EndN`
- `PrevHash/DeltaHash`（形成 delta 链）
- `Ack`

也就是说，MVSS-Delta 的重点是把“传整状态”改为“传状态变化量 + 链式校验”。

### 3.2 滑动窗口设计（论文策略）

论文里的 MVSS-Delta 还包含滑动窗口聚合思路：在窗口内聚合多次小变化后再同步，降低消息数量与带宽。

当前仓库状态是：

- **已实现**：Delta 消息结构、收发、校验、失败中止语义（MVP）。
- **未完整实现**：协议层滑动窗口聚合与自适应窗口控制（后续可在同步发送前引入窗口聚合器）。

### 3.3 失败处理语义

按当前实验策略，Delta 校验失败时：

- 不回退到全量 `TXsync`；
- 直接 `abort` 当前账户的 Delta 同步路径；
- 记录失败原因，方便实验分析。

---

## 4. 代码如何实现策略设计

## 4.1 策略分叉

- `IsMVSS()`：进入 MVSS 迁移总线（分流/重定向/迁移流程）。  
- `IsMVSSDelta()`：只决定同步阶段走 Delta。

即：迁移骨架相同，同步阶段分叉。

## 4.2 Delta 协议对象与消息通道

- `core/txsyncdelta.go`：定义 `TXsyncDelta` 与 `CalcDeltaHash()`。  
- `pbft/cmd.go`：新增 `cTXsyncDelta` 与 `SyncDeltaMsg`。  
- `pbft/pbft.go`：网络消息分发中接入 `handleTXsyncDelta`。

## 4.3 同步发送与接收

- `pbft/mvss_sync.go`  
  在 `mvssTriggerSyncIfNeeded` 中：
  - `MVSS` 走 `TrySendTXsync`；
  - `MVSS-Delta` 构造 `TXsyncDelta` 并走 `TrySendTXsyncDelta`。

- `pbft/mvss_delta.go`  
  负责 Delta 路径：
  - `TrySendTXsyncDelta`
  - `handleTXsyncDelta`
  - `mvssApplyDeltaSync`
  - `mvssOnDeltaAck`
  - `mvssAbortDelta`

## 4.4 状态合并与中止

- `account/mvss_ctx.go`  
  新增迁移上下文与缓存：
  - `LastDeltaHash`
  - `MigPendingDelta`
  - `MigAbortReason`

- `chain/blockchain.go`  
  目标片出块时：
  - `MVSS-Delta` 路径读取 `TakeMigPendingDelta` 应用增量；
  - 非法增量触发中止标记。

## 4.5 论文分析日志

- `pbft/sync_logger.go`  
  新增 `S*_sync.csv`，字段：
  - `ts,event,mode,addr,start_n,end_n,ok,reason,bytes`

日志打点覆盖：

- `send`
- `recv`
- `apply`
- `ack`
- `abort`

用于直接统计同步次数、成功率、失败原因、字节规模。

---

## 5. 预期效果：哪些指标会优化

### 5.1 与 MVSS 对比时的预期

- **Sync 字节开销**：下降（最直接）  
  由整状态传输变为增量传输，目标指标是同步带宽降低。

- **交错场景同步延迟**：有望下降  
  单次同步载荷更小，编码/传输/应用成本降低。

- **高并发迁移下吞吐**：有望提升  
  同步负担变轻后，对交易处理的干扰减小。

### 5.2 不会变化或变化不明显的指标

- **未触发 sync 的场景**  
  `MVSS` 与 `MVSS-Delta` 主流程几乎一致，差异通常很小。

- **锁相关指标**（如锁交易比例）  
  主要由 MVSS 分流策略决定，Delta 本身不直接改变锁策略。

---

## 6. 实验观察建议

如果一次运行没有触发 interleave，同步改进很难体现。建议：

- 增大并发与迁移密度，提高交错概率；
- 对比 `MVSS` 与 `MVSS-Delta` 的 `S*_sync.csv`；
- 优先看：
  - `mode=delta` 的消息条数与 `bytes`；
  - `ok/reason`（失败路径）；
  - 与 `*_block.csv` 的迁移阶段时延联动分析。

这样可以更清晰地看到 Delta 机制是否真的带来同步层收益。
