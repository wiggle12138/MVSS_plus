# MVSS-main 工作区代码导读

本文档用于快速说明当前工作区中各目录/文件的作用，帮助你理解系统结构和代码入口。
在引言简单介绍一下我们研究的关注背景

真实系统里，「客户端顺序 ≠ 上链顺序」存在吗？
存在，而且很常见——但要分两层说：

1. 一般区块链（非迁移）
用户先发的 tx 不一定先被打包（gas 竞价、mempool 排队、出块容量）
同一区块内多笔 tx 的 打包顺序由验证者决定，不等于用户点击发送的顺序
分片系统里更明显：跨片 relay 更慢，后发的片内 tx 可能先确认
所以：全局上链顺序 ≠ 客户端发起顺序 是常态。

2. 你们论文关心的（更特殊）
不是泛泛的「谁先谁后」，而是：

账户迁移窗口内，同一账户的多笔关联交易，必须按 客户端逻辑顺序 更新余额/nonce；但 tx 会 源源不断到达，且 老片、新片、relay 路径并行，就会出现 时间戳交错（old→new→old）。

这在 分片 + 状态迁移 的系统里是真实问题（Monoxide、各类 account-based sharding 都要处理 pending / migration）。
正常不迁移时，同一账户通常只归一个片管，片内 PBFT 串行执行，顺序问题相对小；迁移期 才是你们 MVSS 要解决的核心场景。

策略	迁移期思路
SOTA-Lock
迁出账户 整账户 + 全部关联交易 锁住 → 迁完再批量处理（CaP）
Fine-tuned-Lock
只锁 Payer（迁出账户当 扣款方/sender 的 tx）；Payee（别人给它打钱）在规则允许下 继续执行
MVSS / Delta
不锁；按 客户端发起时间 相对 TXmig1 分 老 tx / 新 tx → 老的在 源片，新的 重定向到新片；必要时 TXsync / TXsyncDelta 保序

MVSS / Delta 的设计逻辑
迁移不能停流量：相关 tx 仍在到达
不想全锁 → 按 客户端发起时间 分流：
RequestTime ≤ TXmig1 → 老 tx，源片执行
RequestTime > TXmig1 → 新 tx，目标片执行（重定向 + nonce/RedirectTag 防重放）
不能真并行乱序：逻辑上必须先老后新（至少不能把「逻辑上更晚的新 tx」在「逻辑上更早的老 tx」之前生效），否则余额/nonce 错
若出现 old→new→old 交错（逻辑序与到达/片间时序不一致）→ 触发 Stage3 状态同步（MVSS 全量 sync，Delta 增量 sync）
MVSS-Delta 与 MVSS 控制面相同（分流、DetectInterleave、Pause），只在 Stage3 数据面 用增量同步减带宽。


SOTA-Lock
迁移期 干脆不执行 关联交易 → 用「暂停」避免乱序扣款
代价：RLT 高、吞吐差，但 不容易在执行层出 nonce/余额 错

Fine-tuned-Lock
扣款方（Payer）锁，加款方（Payee）可能继续
迁移期仍有部分 tx 在老片执行 → 更容易 出现「到达顺序 / 上链顺序 ≠ 客户端顺序」→ RDT 高
若半锁规则不完整，扣款顺序 确实可能出问题（论文把它当作锁基线的弱点）

MVSS / Delta
不锁，但按 客户端发起时间 分老/新 tx，老片处理老的、新片处理新的
若出现 old→new→old 时间戳交错，不能并行乱执行，必须 TXsync / Delta 先对齐状态再扣款
目标：既不整账户锁死，又不在错误顺序下扣款


正常运行时期由pbft共识来负责交易顺序（nonce校验），但是到了迁移时期，如果不想锁，那么两个分片同时负责这一个账户状态，就需要进行同步。
迁移窗口里，同一账户会短暂处于 「两个分片都在处理它的 tx」 的状态：
老 tx 还在 源片 池里 / 打包；
新 tx 已被 重定向到目标片；
跨片 relay 有延迟。
这时：

两池并行、两路 commit，全局只有 nonce 不够——因为 nonce 只管「执行时顺次消耗」，管不了「哪一片先执行、哪一片后同步状态」；
还会出现 old→new→old 时间戳交错：逻辑上应先老后新，到达/上链却可能乱。
论文要解决的，就是这个迁移窗口里的同账户顺序正确性——不是泛泛的「区块链都会乱序」。

讲故事的顺序


## 1. 项目整体定位

这是一个基于 Go 实现的**分片区块链仿真系统**，核心关注：

- 分片内区块生成与共识（当前主流程是单节点快速提交路径，也保留 PBFT 多节点流程代码）。
- 跨分片交易 relay。
- 账户迁移（TXmig1/TXmig2/Announce/NS）及迁移期不同锁策略。
- 动态重分片算法（PageRank/CLPA/LBF/METIS）驱动账户映射调整。
- CSV 驱动的数据集注入与实验日志输出。

模块名在 `go.mod` 中是 `blockEmulator`。

## 2. 启动入口与运行方式

### `main.go`

- 程序入口极简：调用 `test.Test_shard()`。
- 也就是说，实际启动逻辑都在 `test` 包中。

### `test/test_shard.go`

- 是**真正的运行入口**（通过命令行参数控制模式和角色）：
  - `--client/-c`：作为客户端运行（交易注入、迁移触发、收集回包）。
  - 非 client：作为分片节点运行（启动网络监听、提案与执行）。
- 解析参数并写入 `params.Config`，然后：
  - client 模式调用 `pbft.RunClient(testFile)`；
  - node 模式调用 `shard.NewShardNode()` 并等待停止信号。

### `shard/shard.go`

- 创建 `ShardNode`，内部持有一个 `pbft.Pbft` 实例。
- 启动 TCP 监听；
- 若当前是主节点 `N0`，启动 `Propose()` 周期性提案流程。

## 3. 配置与网络拓扑

### `params/config.go`

- 全局配置中心，定义 `ChainConfig`，包括：
  - 分片数量、节点标识、区块间隔、各类池容量；
  - 交易注入速率；
  - 迁移策略开关（停止迁移、锁账户、不锁账户、立即锁、跨链等）；
  - 算法相关开关（`PorC` 等）。
- 定义了全局网络表：
  - `NodeTable`：分片 -> 节点 -> 地址；
  - `ShardTable` 与 `ShardTableInt2Str`：分片编号映射。
- 还包含初始账户与余额配置。

## 4. 共识与跨分片主流程（`pbft/`）

`pbft` 目录是系统最核心的业务编排层，覆盖消息协议、网络、提案提交、迁移、客户端协同。

### 4.1 关键文件

- `pbft/pbft.go`：主控文件（消息分发、提案循环、提交执行、relay、迁移、announce、epoch change 等）。
- `pbft/pbftsingle.go`：单节点快速路径（`propose1/commit1`），实际运行中常走这条路径。
- `pbft/PBFTforMigrate.go`：迁移阶段的特殊流程（`mpropose/spropose/SendOut/handleBalancesAndPendings/SendSure`）。
- `pbft/hanleTxFromClient.go`：接收客户端交易后，按本分片/他分片和锁策略分流。
- `pbft/client.go`：客户端逻辑（注入交易、收集回复、触发迁移、运行算法、广播新映射）。
- `pbft/cmd.go`：协议消息结构体与命令常量（`cPrePrepare/cCommit/cRelay/...`）。
- `pbft/tcp.go`：节点侧 TCP 监听与消息读取（长度前缀防粘包）。

### 4.2 端到端业务链路（简化）

1. 客户端读取数据集并注入交易到各分片主节点。
2. 分片主节点周期性出块（打包普通交易 + 迁移相关交易）。
3. 节点执行提交：
   - 更新本地链和状态树；
   - 统计并记录日志；
   - 跨分片交易做 relay；
   - 迁移交易生成证明并发送到目标分片；
   - 通知各分片/客户端映射变化（announce）。
4. 客户端根据提交回包与 pending 集合，运行重分片算法并下发新映射。
5. 系统在迁移完成或满足停止条件后收敛并退出。

### 4.3 关于“PBFT”与“单节点”

- 代码中同时存在完整 PBFT 三阶段消息流程（PrePrepare/Prepare/Commit）和单节点提交路径。
- 当前默认节点表每分片常只启 `N0`，因此运行时通常依赖 `pbftsingle.go` 的快速提交逻辑。

## 5. 链与状态执行（`chain/`）

### `chain/blockchain.go`

- `BlockChain` 结构聚合：
  - 当前区块头；
  - 本地存储；
  - go-ethereum trie DB（状态树）；
  - 交易池和迁移相关池。
- 关键能力：
  - `NewBlockChain`：初始化链、加载或创建创世块；
  - `GenerateBlock`：从各池提取交易构造新块；
  - `AddBlock`：持久化区块并执行状态转移；
  - `getUpdatedTreeOfState`：核心状态更新逻辑；
  - `IsBlockValid`：基础合法性检查。

## 6. 数据结构与内存池（`core/`）

`core` 定义了区块系统内的“数据模型 + 各类交易池 + 编解码/哈希”。

### 6.1 区块与交易模型

- `transaction.go`：普通交易 `Transaction`。
- `block.go`：`BlockHeader` 与 `Block`。
- `txrelay.go`：跨分片 relay 交易封装。
- `txmig1.go`：迁移第一阶段（迁出请求）。
- `txmig2.go`：迁移第二阶段（携带证明与状态）。
- `txann.go`：迁移公告。
- `txns.go`：迁移后新状态（或变更）数据。
- `proofdb.go`：Merkle 证明封装。

### 6.2 内存池

- `txpool.go`：普通交易池，包含：
  - 主队列；
  - 各种锁定/半锁定/迁移阶段子池；
  - 打包提取 `FetchTxs2Pack` 和锁逻辑 `LockTX`。
- `txmig1pool.go` / `txmig2pool.go` / `txannpool.go` / `txnspool.go`：
  - 分别管理迁移相关四类队列。

## 7. 账户映射与状态（`account/`）

### `account/account_state.go`

- `AccountState` 定义账户状态（余额、迁移标记、位置）。
- 全局维护：
  - `Account2Shard`：账户到分片映射；
  - `AccountInOwnShard`：当前分片本地账户集；
  - 各类迁移期锁表（全锁、不锁、迁出前后状态等）。
- `Addr2Shard`：若映射不存在，则按地址后缀做默认分片计算并写回映射表。

### `account/account.go`

- 提供地址/密钥相关辅助函数（生成地址、公钥哈希等）。

## 8. 持久化层（`storage/`）

### `storage/storage.go`

- 基于 BoltDB 存储：
  - 区块；
  - 区块头；
  - 最新区块哈希。
- 提供增删查接口（`AddBlock/GetBlock/GetBlockHeader/...`）。
- 状态树本体不在这里存，而在 `chain` 里通过 go-ethereum trie + leveldb 管理。

## 9. 分片算法模块（`algorithm/`）

用于根据交易图计算新账户分片映射。

- `tx2graph.go`：将交易集转换为图表示（用于 PageRank 等）。
- `algorithm.go`：
  - `Pagerank`；
  - `Algorithm2/MigrationAlgorithm`（实验性或基线逻辑）。
- `graph.go`：图结构（点、边、权重）。
- `allocation.go`：根据得分进行分配。
- `partition_CLPA.go`：CLPA 划分实现。
- `partition_LBF.go`：LBF 划分实现。
- `partition_METIS.go`：METIS 划分流程（写图文件 -> 调外部程序 -> 读回分区结果）。

## 10. METIS 外部工具（`METIS/`）

- `partition.cpp`：调用 METIS 库做图分区，输入图文件、输出每个节点所属分片。
- `test.cpp`、`graph.txt`、`partition.txt` 等：实验/样例文件。
- `algorithm/partition_METIS.go` 会调用 `METIS/partition` 可执行文件。

## 11. 网络与通用工具（`utils/`）

### `utils/utils.go`

- `TcpDial`：复用连接 + 长度前缀发送消息（防粘包）。
- 其他常用方法：
  - `Addr2Shard`（工具层版本）；
  - `Min`；
  - `Int2hexString`；
  - `RandInt0To3`（用于引入随机等待）。

## 12. 自定义 Trie（`trie/`）

`trie/` 目录实现了一个自定义 MPT-like 结构（叶子、扩展、分支、nibbles 等）。

- 主要文件：`trie.go`、`trie_node.go`、`trie_branch.go`、`trie_leaf.go` 等。
- 当前主业务状态树多数使用的是 go-ethereum 的 trie（见 `chain` / `pbft` 中 `github.com/ethereum/go-ethereum/trie`）。
- 该目录更像独立实现或历史实现，仍可用于实验或对比。

## 13. 测试与实验脚本（`test/`）

`test` 目录并非标准 `go test` 风格单元测试，而是偏“实验启动器/场景脚本”：

- `test_shard.go`：核心启动入口（最重要）。
- `test_client.go`：简单客户端入口封装。
- `test_blockChain.go`、`test_pool.go`、`test_db.go` 等：模块级实验代码。

## 14. 日志与运行产物

- `log/`：运行时输出的大量 CSV（区块、交易、迁移、算法时延等）。
- `record/`（运行时会生成/使用）：trie leveldb 等持久化数据。
- 根目录已有 `log/migration.csv` 被版本控制跟踪，表示你当前仓库也记录实验结果文件。

## 15. 推荐阅读顺序

如果你刚接手这个项目，建议按下面顺序读：

1. `test/test_shard.go`（明确启动参数和角色）
2. `params/config.go`（理解全局开关）
3. `pbft/pbft.go` + `pbft/pbftsingle.go`（主流程）
4. `chain/blockchain.go`（出块与状态更新）
5. `core/*.go`（数据结构和池）
6. `pbft/client.go`（客户端协同与迁移触发）
7. `algorithm/*.go`（重分片算法细节）

---

如果你愿意，我下一步可以再给你补一版：

- **“运行命令速查”**（如何起多个分片/客户端）；
- **“关键配置开关说明表”**（每个布尔开关对行为的影响）；
- **“迁移流程时序图”**（从 TXmig1 到 TXann/NS 的完整链路）。
