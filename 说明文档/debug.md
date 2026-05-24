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

