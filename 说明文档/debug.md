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
