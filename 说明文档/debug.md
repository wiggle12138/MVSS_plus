5.12

1. 空区块问题：在tcp接收并生成交易池处打日志，pbft/handleTxFromClient.go

2. 成功接收处理交易入池，但是不产生区块。定位propose1和commit1，是时间判断的问题，在收到交易注入要重置epoch1才能跳出死循环

3. 跳出死循环，又碰到Panic() getUpdatedTreeOfState；来自数据集字段问题，读取错误的列加入创世状态树。在pbft/client.go中修改 func Get_Initial_Map 852行左右



5.13 

4. 优化启动逻辑，第一次分发交易之前不产生无意义空区块

pbft/pbft.go

给 Pbft 增加了 bootstrapSeqID（记录节点启动后的首个区块高度）。

在 Propose() 里增加“首块门控”逻辑

start_2shard_2node.bat

更新了注释，说明当前已做“首块等待首批交易”的优化；脚本中的等待仅用于监听就绪。

5. 优化日志创建鲁棒性，分片节点也可以创建日志文件



5.23

6. 增加注入交易数量，触发账户迁移，修改一些账户迁移过程中未初始化map、TCP断联、长时间占用锁等bug



5.24



5.25 Phase1（MVSS+ 论文主线，仅 StrategyMVSSPlus 分支生效）

7. 启用 AccountState.Nonce 与 Transaction.Nonce/RedirectTag，创世账户 nonce=0；MVSS+ 出块执行时校验 nonce 并递增，预期防双花。

8. TXmig1 增加 Sync/OrderList/LastCN；handleNewMap 在 MVSS+ 下构建迁移上下文与排序表，预期对齐论文 TX_ini。

9. 新增 core/TXsync、pbft/cTXsync 与 mvss_sync.go，实现 TXsync 收发与双向回传，预期 Stage 3 状态桥接可用。

10. 新增 account/mvss_ctx.go（FSM、交错检测、MigPendingState），预期时间戳交错时可暂停老交易并触发同步。

11. txpool FetchTxs2Pack 在 MVSS+ 下老交易继续打包、新交易跳过、FSM 暂停交易不打包，预期分流替代锁池。

12. handleTxFromClient/handleRelay 在 MVSS+ 下重定向新交易并校验 RedirectTag+nonce，预期防重放且新交易由目标分片处理。

13. params.IsMVSSPlus() 作为策略分叉入口；MVSS/lock/finetuned 等基线路径不变，预期对比实验可切换 -m 参数。

5.25 fix
14. 修复 MVSS+ 死锁：handleNewMap 不再持 Account2ShardLock 调用 mvssBuildMigCtx；handleTxFromClient 先 Addr2Shard 再抢 Tx_pool 锁，预期迁移触发后不再卡住 propose1。
15. mvssBuildMigCtx 账户不在状态树时降级为默认状态而非 panic，预期节点不会因迁移账户缺状态而崩溃断连。

5.25 strategy

16. 迁移策略重命名（params/migration_strategy.go）：原 `MVSS`→`original`（工程近似、未完整论文），原 `MVSS+`→`MVSS`（论文 Phase1 主线）；新增 `MVSS-Delta`（Phase2 增量 sync 占位，bool 同 MVSS）。入口 `IsMVSS()` / `IsMVSSDelta()`，`IsMVSSPlus()` 过渡期等同 `IsMVSS()`；config 默认 `original`，bat 默认 `MVSS`；命令行 `MVSS+` 仍解析为 `MVSS`。

5.26 strategy baseline

17. 基线策略显式别名：`lock`→canonical `SOTA-Lock`（Fine-tuned Lock 论文 SOTA Lock 全锁，**非 LB-Chain**）；`finetuned`→`Fine-tuned-Lock`（INFOCOM'24 半锁，可直接作对比实验）。新增 `IsSOTALock()` / `IsFineTunedLock()`；`lock`/`finetuned` 仍为 Parse 别名。参数说明合并为唯一文档 `说明文档/参数配置.md`（含 MigrationStrategy 与实验矩阵），删除 `实验配置结构更改.md`。

5.27 MVSS-Delta MVP

18. 新增 MVSS-Delta 同步数据结构与消息通道：`core/txsyncdelta.go`、`pbft/cmd.go(cTXsyncDelta/SyncDeltaMsg)`、`pbft/mvss_delta.go`。实现 `send/recv/apply/ack` 最小闭环与 delta 哈希校验（含 PrevHash 链）。

19. 同步路径按策略分叉：`IsMVSS()` 负责迁移总线；`IsMVSSDelta()` 时 Stage3 仅走 `TXsyncDelta`，`MVSS` 保持 `TXsync` 原路径。`handleTXsync` 在 Delta 模式下不再处理，避免混用两条同步路径。

20. 失败即中止（无 fallback）：新增 `MigAbortReason` 与 `MigPendingDelta`（`account/mvss_ctx.go`），delta 校验失败直接 `abort`，并清理 pending delta/上下文状态；迁移完成时统一清理 abort 与 delta 缓存。

21. 新增轻量论文分析日志：`pbft/sync_logger.go`，每分片生成 `log/S*_sync.csv`，字段为 `ts,event,mode,addr,start_n,end_n,ok,reason,bytes`；在 `send/recv/apply/ack/abort` 打点，按批次 flush（每 32 条或 abort 立即 flush）以降低性能影响。

5.28 client_ts 双时间戳

22. `core/transaction.go` 新增 `ClientTimestamp` 与 `OrderTimestamp()`；逻辑排序用 client_ts，到达先后仍用 `RequestTime`。

23. `pbft/client.go` 读 CSV 列 9（`parseClientTimestamp`），写入 `Transaction.ClientTimestamp`；无 client_ts 时注入回退为 `RequestTime`；`is300KStyleDatasetPath()` 识别 `mvss_*.csv` 等 benchmark 文件。

24. `account/mvss_ctx.go`：`DetectInterleave` 按 client_ts 排序，用 `RequestTime` 判 old/new；`RegisterOrder` 写入 `OrderList`。

25. `pbft/mvss_sync.go`：`mvssBuildMigCtx` / `mvssRedirectNewTx` 改用 `OrderTimestamp()` 与 `RegisterOrder()`。

26. `test/test_shard.go` 增加与 client 一致的 `is300KStyleDatasetPath()`，分片节点正确解析 benchmark CSV。

benchmark 数据集

27. 新增 `scripts/generate_mvss_dataset.py`：从 `selectedTxs_300K.csv` 生成 `datasets/mvss_interleave_benchmark.csv`（列 9 = client_ts）及 `.meta.json`。

28. Episode 设计：每迁移账户 2 笔 old + 1 笔 new，client_ts 为 T / T+100 / T+200，CSV 行序控制到达先后，预期触发 old-new-old 交错。

29. 支持 `--calibrate-log` 从 `log/S*_block.csv` 反推 `mig_index`；`--mig-accounts-file log/S0_mig1.csv` 取 episode 账户与 PageRank 迁出列表对齐。

30. CLPA 偏置 tx 的 recipient 须为 300K 真实地址（非脚本随机地址），预期避免创世树缺账户导致 `getUpdatedTreeOfState` panic。

31. 换 benchmark 数据集后须删 `*_blockchain_db*` 与 `record/triedb/`，预期分片节点按新 CSV 重建 `Init_addrs` 与创世状态树。

6.4 MVSS-Delta 探针 Stage3

32. 目标片 delta apply 时探针 tx2 尚未入池，PhaseB 晚到 new 仍带源片 nonce 导致执行 skip；在 `handleTxFromClient` 入池后若 FSM≥SyncApplied 再调 `mvssPromoteMigNewTxsToHead` 按链上 nonce 重编号并提至队首。

33. `sync.csv` 出现 short write 非环境变量未传递（`SYNC_PROBE=1` 已生效可见 init ok），而是 `writeSyncLog` 释放锁后并发写 `csv.Writer`（`go handleTXsyncDelta` 多 goroutine）；改为持锁完成 Write+Flush，bat 显式向节点窗口注入 `SYNC_PROBE=1`。

