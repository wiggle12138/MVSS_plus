# MVSS 实现与代码对照（本仓库）

本文档说明**基于多版本状态同步的跨分片账户迁移**在本仿真代码中的落点：哪些文件承担哪类职责，以及论文/设计中的概念与**具体字段、配置项**的对应关系。

现在的共识逻辑是简化的，“单节点即共识完成”

> 说明：本仓库是**分片 + 迁移 + 中继**的仿真实现；部分论文级机制（如显式 `TX_sync` 消息类型、账户 nonce 防双花、独立「重定向标签」字段）在代码中**未完整实现**或**由其它字段组合表达**，下文会逐项标明。

---

## 一、总览：功能 → 主要文件

| 功能域 | 主要文件 | 简述 |
|--------|----------|------|
| 全局开关与网络表 | `params/config.go` | 分片数、注入速率、迁移/锁策略、`NodeTable`、`ShardTable` |
| 账户状态与分片映射 | `account/account_state.go` | `AccountState`、`Account2Shard`、`Not_Lock_Acc` 等 |
| 交易与区块结构 | `core/transaction.go`、`core/block.go` | 交易字段、区块内 mig/ann/ns |
| 迁移与证明对象 | `core/txmig1.go`、`txmig2.go`、`txann.go`、`txns.go`、`txrelay.go`、`proofdb.go` | 各类迁移/中继消息体 |
| 交易池与「老/新」分流 | `core/txpool.go` | 打包、`Not_Locking_TX_Pools` 等与时间戳比较 |
| 链与状态执行 | `chain/blockchain.go` | 出块、状态树更新、各 pool 挂链 |
| 共识与跨分片消息 | `pbft/pbft.go`、`pbft/pbftsingle.go` | 提交、relay、mig、announce、CaP |
| Announce 后 pending 协调 | `pbft/handleanns.go` | `handleAnns`、与 `TXns`/CaP 发送衔接 |
| 停迁移 / epoch 路径 | `pbft/PBFTforMigrate.go` | `SendOut`、余额+pending 批量迁移、`SendSure` |
| 客户端注入与映射 | `pbft/client.go` | 读 CSV、注入、`SendNewAddr2Shard` |
| 节点入口 | `shard/shard.go`、`test/test_shard.go` | 启动 PBFT、参数解析 |

---

## 二、配置与网络：对应哪些字段

### `params/config.go`

| 字段 / 变量 | 作用（与 MVSS 叙述的关系） |
|-------------|---------------------------|
| `Shard_num` | 分片数量 |
| `Inject_speed` | 客户端注入 TPS |
| `Not_Lock_Acc_When_Migrating` | **多版本 / 非整链锁死**模式开关（注释为「多版本并发」） |
| `Lock_Acc_When_Migrating` | 迁移期**全锁**账户（对比基线） |
| `Stop_When_Migrating` | 迁移时是否停块（epoch 式重映射路径） |
| `Not_Lock_immediately` | 是否在块边界立刻把队列里交易扫入锁池 |
| `RelayLock` | 与接收方锁相关的 relay 复制策略（`Relay_Lock` 交易字段配合） |
| `NodeTable` | `map[ShardID]map[NodeID]TCP地址`，跨分片 TCP 目标 |
| `ShardTable` / `ShardTableInt2Str` | 分片 ID 与整型下标互转 |

---

## 三、账户与映射：对应哪些字段

### `account/account_state.go`

| 字段 / 变量 | 含义 |
|-------------|------|
| `AccountState.Balance` | 账户余额（状态树里编码存储） |
| `AccountState.Migrate` / `Location` | 迁移相关标记（与链上执行路径配合） |
| `AccountState` 内被注释的 `Nonce` | **未启用**；不作为防双花执行依据 |
| `Account2Shard` | 账户 → 分片下标（逻辑视图，可被迁移/客户端更新） |
| `AccountInOwnShard` | 当前节点所属分片是否「持有」该账户 |
| `Not_Lock_Acc` | 迁出账户在「不整链锁」模式下的一种**进行中标记**（与 `Not_Locking_TX_Pools` 配合） |
| `Lock_Acc` | 全锁模式下迁出账户标记 |
| `Outing_Acc_Before_Announce` / `Outing_Acc_After_Announce` | Fine-tuned 半锁路径下的迁出阶段标记 |
| `BalanceBeforeOut` | 迁出前余额快照（用于部分变更/日志场景） |
| `Addr2Shard(addr)` | 查表或按地址后缀取模得到默认分片 |

---

## 四、交易结构：字段与设计概念的对应

### `core/transaction.go` — `Transaction`

| 字段 | 与 MVSS 叙述的对应关系 |
|------|------------------------|
| `Sender` / `Recipient` | 交易双方地址（字节） |
| `TxHash` | 交易哈希（relay 证明里用） |
| `RequestTime` | 首次进入系统时间（毫秒级，用于日志与「老」侧时间参照） |
| `Second_RequestTime` | 跨分片第二段（如 relay 到目标分片）到达时间 |
| `TXmig1_Time` | 与该账户相关的 **TXmig1** 上链时间；与 `RequestTime`/`Second_RequestTime` 比较实现**迁移前/后**语义分界 |
| `TXmig2_Time` | 迁入完成相关时间戳 |
| `CommitTime` / `LockTime` / `UnlockTime` 等 | 执行与锁/解锁时刻（统计与实验） |
| `IsRelay` | **跨分片后半段 / 需 relay 到目标分片** 的标记（接近「重定向到新分片执行」的工程表达） |
| `Relay_Lock` | 与 `RelayLock` 配置配合的接收方侧 relay 变体 |
| `SenLock` / `RecLock` / `HalfLock` | 发送方/接收方/半锁语义上的标记 |
| `Sen_Suppose_on_chain` / `Rec_Suppose_on_chain` | 与延迟锁包等实验逻辑相关 |
| **无独立「重定向标签」字段** | 论文中的「标签」在本实现中由 **`IsRelay` + 分片映射 + 池路径** 等组合体现 |

---

## 五、交易分流与池化：`core/txpool.go`

| 功能 | 位置 / 说明 |
|------|-------------|
| 主队列 | `Tx_pool.Queue` |
| 按分片 relay 暂存 | `Relay_Pools` + `Relaypoollock` |
| 非全锁模式下「挂起」的交易 | `Not_Locking_TX_Pools[addr]`（键为 hex 账户串） |
| 全锁模式 | `Locking_TX_Pools` |
| 半锁（Fine-tuned） | `Outing_Before_Announce_TX_Pools` / `Outing_After_Announce_TX_Pools` |
| **老/新时间分界** | `FetchTxs2Pack`、`LockTX`：典型判断如 `Not_Lock_Acc[from] && !IsRelay && !Relay_Lock && (TXmig1_Time < RequestTime)` 与接收方侧 `TXmig1_Time < Second_RequestTime` |

---

## 六、迁移与跨分片消息体：`core/` 各文件

| 文件 | 类型 / 职责 |
|------|-------------|
| `txmig1.go` | `TXmig1`：迁出请求（地址、源/目标分片、请求时间等） |
| `txmig2.go` | `TXmig2`：迁入包（含证明、状态、余额等，与 `pbft.TryTXmig1` 发出的 `Mig2` 消息对应） |
| `txann.go` | `TXann`：账户已在某分片的公告（证明 + 状态） |
| `txns.go` | `TXns`：与 announce 后状态/变更同步相关的结构（与 `ChangesAndPendings` 中的 `TXnss` 对应） |
| `txrelay.go` | `TXrelay`：跨分片 relay 载荷（原始交易 + 交易树证明 + 接收方状态证明） |
| `proofdb.go` | `ProofDB`：Merkle 证明收集，供 `TryRelay` / mig 证明使用 |

### 区块头中的聚合

`core/block.go` — `Block` 除 `Transactions` 外，常见字段包括：

- `TXmig1s`、`TXmig2s`、`Anns`、`NSs`：与迁移流水线对应（打包进块、参与 `getUpdatedTreeOfState` / mig 树）。

---

## 七、PBFT 层：消息与函数对应

### `pbft/cmd.go`

| 命令常量 | 典型载荷结构 | 作用 |
|----------|--------------|------|
| `cRelay` | `Relay`（`[]*TXrelay` + 源 `ShardID`） | 跨分片中继交易 |
| `cTXmig1` | `Mig2`（命名历史原因：承载 `TXmig2` 列表） | 源分片向目标分片发迁入包 |
| `cAnnounce` | `Announce`（`[]*TXann`） | 账户迁入公告 |
| `cCaP` | `ChangesAndPendings`（`TXnss` + `ChangeAndPending` 映射） | 余额变化 + pending 等到目标分片 |
| `cClient` | `TxFromClient` | 客户端交易 |
| `cNewMap` | `NaM` | 新账户→分片映射 |
| 其它 | `cBalanceAndPending`、`cSure`、`cEpochCh` 等 | 停迁移 / epoch 重映射路径 |

### `pbft/pbft.go`（与 `pbftsingle.go`）

| 函数 / 区域 | 功能 |
|-------------|------|
| `TryRelay` / `handleRelay` | 构建并发送 `TXrelay`；目标分片入池、必要时继续转发 |
| `TryTXmig1` / `handleMig2` | 迁出 → 迁入包发送与接收 |
| `TryAnnounce` / `handleAnnounce` | 公告广播与本地 `TXann_pool` 等处理 |
| `TrySendChangesAndPendings` / `handleChangesAndPendings` | `TXns` + pending 等到目标分片 |
| `commit` / `commit1`（`pbftsingle.go`） | 块提交后触发 relay、mig、announce、日志写 CSV |

### `pbft/handleanns.go`

| 函数 | 功能 |
|------|------|
| `handleAnns` | 处理 `TXann`：更新 `Account2Shard`、释放锁表项、从 `Not_Locking_TX_Pools`/`Locking_TX_Pools` 中按规则放回 `Queue` 或转移；在需要时组装 `TXns` 并 `TrySendChangesAndPendings` |

### `pbft/PBFTforMigrate.go`

| 函数 | 功能 |
|------|------|
| `SendOut` | epoch 重映射后，按目标分片聚合 `BalancesAndPendings`（余额 + pending） |
| `handleBalancesAndPendings` | 目标分片接收迁入账户状态与交易 |
| `mpropose` / `spropose` | 映射变更与账户状态类 PBFT 请求（与 `Stop_When_Migrating` 路径绑定） |

---

## 八、链与状态：`chain/blockchain.go`

| 区域 | 功能 |
|------|------|
| `NewBlockChain` | 初始化 `Tx_pool`、`TXmig1_pool`、`TXmig2_pool`、`TXann_pool`、`TXns_pool`、LevelDB trie |
| `GenerateBlock` | 从各 pool 取 mig1/mig2/ann/ns 与普通交易组块 |
| `AddBlock` | 持久化、更新状态树、更新 `Account2Shard` / `AccountInOwnShard`（与块内 mig2/ann 等一致） |
| `getUpdatedTreeOfState` | 执行交易与迁移相关状态变更 |

---

## 九、论文概念 ↔ 代码：显式对照表

| 论文 / 设计表述 | 本仓库实现情况 | 主要落点 |
|-----------------|----------------|----------|
| 交易分流、非整链锁死 | **有**（受 `Not_Lock_Acc_When_Migrating` 等控制） | `params/config.go`、`core/txpool.go`、`account/account_state.go` |
| 老交易 vs 新交易时间分界 | **有**（`TXmig1_Time` vs `RequestTime` / `Second_RequestTime`） | `core/txpool.go`、`core/transaction.go` |
| 新交易重定向到目标分片 | **部分有**（`IsRelay` + `TryRelay`/`handleRelay`） | `pbft/pbft.go`、`core/txrelay.go`、`core/transaction.go` |
| 独立「重定向标签」字段 | **无** | 见 `Transaction` 表；语义分散在 `IsRelay`/`Relay_Lock` 与分片逻辑 |
| 多轮 `TX_sync` 源↔目标状态桥 | **无同名协议** | 无 `TX_sync` 类型；接近能力由 `TXmig1/2`、`TXann`、`TXns`、`ChangesAndPendings`、`BalancesAndPendings` 等组合承担 |
| 源分片 nonce 防双花 | **无**（状态里 Nonce 注释掉；交易无 nonce） | `account/account_state.go`、`core/transaction.go` |
| 目标分片「标签 + nonce」拒收重放 | **无完整等价校验** | `handleRelay` 等以分片归属与锁池为主，非独立密码学标签 |

---

## 十、阅读顺序建议（抓 MVSS 主路径）

1. `params/config.go` — 确认当前跑的是 `Not_Lock` / `Lock` / 半锁哪条路径。  
2. `core/transaction.go` + `core/txpool.go` — 时间与池化规则。  
3. `pbft/pbftsingle.go`（或 `pbft.go` 中 commit 块路径）— 块后 relay / mig / ann。  
4. `pbft/handleanns.go` — announce 后 pending 与 `TXns` 发送。  
5. `chain/blockchain.go` — 状态与映射如何随块更新。

---

*文档生成自当前仓库快照；若后续增加 `TX_sync`、nonce、显式重定向标签，建议在本文件追加一节「增量变更对照」。*
