package pbft

import (
	"blockEmulator/account"
	"blockEmulator/algorithm"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	StartTime  int64
	all_finish chan int
	// max_commit                            int
	// max_commit_lock                       sync.Mutex
	max_commit_block                      int
	max_commit_block_lock                 sync.Mutex
	commit_tx_set                         []*core.Transaction
	commit_tx_set_lock                    sync.Mutex
	pending_tx_set                        []*core.Transaction
	pending_tx_set_lock                   sync.Mutex
	migrationlog                          *csv.Writer
	migration_count                       int
	migration_count_lock                  sync.Mutex
	sendtxlock                            sync.Mutex
	num_of_unfinished_migration           int
	num_of_unfinished_migration_lock      sync.Mutex
	num_of_shard_not_sending_pending      int
	num_of_shard_not_sending_pending_lock sync.Mutex
	num_of_unfinished_migrated_TXs        int
	num_of_unfinished_migrated_TXs_lock   sync.Mutex
	zero_count                            int
	zero_count_lock                       sync.Mutex
	onlyOnce_lock                         sync.Mutex
	// injectDone==1 表示 InjectTXS 已全部发完；此前不因「空块」计数触发停止，避免启动/注入阶段的空块风暴。
	injectDone uint32
	// 按分片统计连续空块回复；仅当 injectDone 后且每个活跃分片 streak 均 >=5 时停止（避免某分片长期无交易误停全局）。
	emptyStreakByShard map[string]int
)

func RunClient(path string) {
	// 客户端节点
	// txs := LoadTxsWithShard(path)
	// tx_cnt := CountTx(path)
	// finished := make([]bool, tx_cnt)
	// finished_cnt := 0
	all_finish = make(chan int)
	// max_commit = params.Config.Max_Commit
	max_commit_block = params.Config.Max_Commit_Block * params.Config.Shard_num
	migration_count = 0
	num_of_unfinished_migration = 0
	num_of_unfinished_migrated_TXs = 0
	num_of_shard_not_sending_pending = params.Config.Shard_num
	zero_count = 0
	atomic.StoreUint32(&injectDone, 0)
	zero_count_lock.Lock()
	emptyStreakByShard = make(map[string]int)
	for _, id := range params.ActiveShardIDs() {
		emptyStreakByShard[id] = 0
	}
	zero_count_lock.Unlock()

	account.InitGlobals()
	go listener()
	// Brief yield so the client listener binds before shard nodes may dial 127.0.0.1:8800.
	time.Sleep(300 * time.Millisecond)

	// 确保log目录存在
	if _, err := os.Stat("./log"); os.IsNotExist(err) {
		err = os.MkdirAll("./log", 0755)
		if err != nil {
			log.Panic("Encountered an error: " + err.Error())
		}
	}

	csvFile, err := os.Create("./log/" + "migration.csv")
	if err != nil {
		log.Panic("Encountered an error: " + err.Error())
	}
	// defer csvFile.Close()
	migrationlog = csv.NewWriter(csvFile)
	migrationlog.Write([]string{"timestamp", "pagerank_time"})
	migrationlog.Flush()

	txs := []*core.Transaction{}
	if params.Config.ClientSendTX {
		txs = Get_Initial_Map_And_TXS(path, account.Account2Shard)
		if params.Config.MaxInjectTxs > 0 && len(txs) > params.Config.MaxInjectTxs {
			txs = txs[:params.Config.MaxInjectTxs]
			fmt.Printf("[RunClient] MaxInjectTxs=%d，已截断为前 %d 笔交易\n", params.Config.MaxInjectTxs, len(txs))
		}
		new_addr2shard := map[string]int{}
		new_addrs := []string{}
		algorithmbegin := time.Now().UnixMilli()
		if params.Config.MigrateBeforeInject {
			if params.Config.PorC == "PageRank" {
				//pagerank
				graph, addrs := algorithm.Pagerank_Tx2graph_And_Addrs(txs)
				Numbda := 0.85
				iters := 20
				points := algorithm.Pagerank(graph, addrs, account.Account2Shard, Numbda, iters, params.Config.Shard_num)
				new_addr2shard = algorithm.Allocate(points)
				for _, acc := range addrs {
					if account.Account2Shard[acc] != new_addr2shard[acc] {
						new_addrs = append(new_addrs, acc)
					} else {
						delete(new_addr2shard, acc)
					}
				}
			}

			if params.Config.PorC == "CLPA" {
				//clpa
				clapstate := new(algorithm.CLPAState)
				belta := 0.5
				iterarion := 10
				clapstate.Init_CLPAState(belta, iterarion, params.Config.Shard_num)
				for _, tx := range txs {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					clapstate.AddEdge(s, r)
				}
				addrs_modified, map_modified := clapstate.CLPA_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != int(map_modified[acc]) {
						new_addr2shard[acc] = int(map_modified[acc])
						new_addrs = append(new_addrs, acc)
					}
				}
			}

			if params.Config.PorC == "LBF" {
				//lbf
				lbfstate := new(algorithm.LBFState)
				alpha := 0.5
				lbfstate.Init_LBFState(alpha, params.Config.Shard_num)
				for _, tx := range txs {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					if tx.IsRelay || tx.Relay_Lock {
						// 只有 r指向s，相当于r的权重+1，s不加
						lbfstate.AddEdge(s, r, 1)
					}
					lbfstate.AddEdge(s, r, 0)
				}
				addrs_modified, map_modified := lbfstate.LBF_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != map_modified[acc] {
						new_addr2shard[acc] = map_modified[acc]
						new_addrs = append(new_addrs, acc)
					}
				}
			}

			if params.Config.PorC == "METIS" {
				metisstate := new(algorithm.METISState)
				alpha := 0.5
				metisstate.Init_METISState(alpha, params.Config.Shard_num)
				for _, tx := range txs {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					metisstate.AddEdge(s, r, 0)
				}
				addrs_modified, map_modified := metisstate.METIS_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != map_modified[acc] {
						new_addr2shard[acc] = map_modified[acc]
						new_addrs = append(new_addrs, acc)
					}
				}
			}
		}
		algorithmend := time.Now().UnixMilli()

		// for addr, shard := range new_addr2shard {
		// 	account.Account2Shard[addr] = shard
		// }
		SendNewAddr2Shard(new_addrs, new_addr2shard)
		s := fmt.Sprintf("%v %v", algorithmend-StartTime, algorithmend-algorithmbegin)
		migrationlog.Write(strings.Split(s, " "))
		migrationlog.Flush()
		time.Sleep(10000 * time.Millisecond)
	} else {
		Get_Initial_Map(path, account.Account2Shard)
	}

	// //10W笔交易注入完停止
	// var counttime func(int64) = func(t int64) {
	// 	//打包所有类型交易
	// 	for time.Now().Unix()-t != int64(60*3000/params.Config.Inject_speed+10) {
	// 	}

	// 	//只打包片内交易
	// 	// for time.Now().Unix() - t != int64(400000 / params.Config.Inject_speed + 10) {
	// 	// }

	// all_finish <- 1
	// }

	// if params.Config.Algorithm {
	// 	读取前5W笔交易
	// 	用算法决定新的映射，得到 new_addr2shard，然后 'SendNewAddr2Shard(new_addr2shard)'
	// }

	time.Sleep(5000 * time.Millisecond)
	StartTime = time.Now().Unix()
	Sendtime(StartTime)
	if params.Config.ClientSendTX {
		for time.Now().Unix()-StartTime != 2 {

		}
		go InjectTXS(txs)
	} else {
		// 无客户端注入时不会执行 InjectTXS，仍需允许按空块规则结束（否则 injectDone 永远为 0）。
		atomic.StoreUint32(&injectDone, 1)
	}

	// go counttime(StartTime)

	// if params.Config.Stop_When_Migrating { //若迁移时要暂停
	// 	go waitread()
	// }

	<-all_finish
	for _, shardID := range params.ActiveShardIDs() {
		nodes := params.NodeTable[shardID]
		if nodes == nil {
			continue
		}
		for nodeID, addr := range nodes {
			fmt.Printf("客户端向分片%v的节点%v发送终止运行消息\n", shardID, nodeID)
			m := jointMessage(cStop, nil)
			utils.TcpDial(m, addr)
		}
	}

	fmt.Printf("停止时间：%v\n", time.Now().Unix()-StartTime)
	// time.Sleep(10000 * time.Millisecond)

	// time.Sleep(15 * time.Second)
	// utils.TxDelayCsv()

}

// 消息监听器
func listener() {
	listen, err := net.Listen("tcp", params.ClientAddr)
	if err != nil {
		log.Panic(err)
	}
	fmt.Printf("客户端开启监听，地址：%s\n", params.ClientAddr)
	defer listen.Close()

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Panic(err)
		}
		go connHandler(conn)
		// time.Sleep(10 * time.Millisecond)
		// b, err := ioutil.ReadAll(conn)
		// if err != nil {
		// 	log.Panic(err)
		// }
		// conn.Close()
		// handle(b)
	}
}

// // 读取连接数据
// func connHandler(conn net.Conn) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)
// 	for {
// 		b, err := reader.ReadBytes('\n')
// 		switch err {
// 		case nil:
// 			handle(b)
// 		case io.EOF:
// 			log.Println("client closed the connection by terminating the process")
// 			return
// 		default:
// 			log.Printf("error: %v\n", err)
// 			return
// 		}
// 	}
// }

// 读取连接数据，利用长度前缀防粘包
func connHandler(conn net.Conn) {
	defer conn.Close()
	for {
		// 创建一个字节切片来存储消息长度前缀
		lengthPrefix := make([]byte, 4)

		// 读取消息长度前缀
		if _, err := conn.Read(lengthPrefix); err != nil {
			if err == io.EOF {
				return
			}
			log.Printf("Error reading message length prefix: %v", err)
			return
		}

		// 将消息长度前缀解析为一个无符号整数
		length := binary.BigEndian.Uint32(lengthPrefix)

		// 创建一个字节切片来存储消息内容
		message := make([]byte, length)

		// 读取消息内容
		if _, err := io.ReadFull(conn, message); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return
			}
			log.Printf("Error reading message: %v", err)
			return
		}

		handle(message)
	}
}

// 消息处理器
func handle(data []byte) {
	cmd, content := splitMessage(data) //将接收到的数据拆分为命令和内容两部分
	switch command(cmd) {
	// case cReply:
	// 	ids := new([]int)
	// 	err := json.Unmarshal(content, ids)
	// 	if err != nil {
	// 		log.Panic(err)
	// 	}
	// 	for _, id := range *ids {
	// 		if !finished[id] {
	// 			finished[id] = true
	// 			finished_cnt += 1
	// 			//收到全部交易上链就停止
	// 			if finished_cnt == 100000 {
	// 				all_finish <- 1
	// 				break
	// 			}

	// 			// if finished_cnt == 4000 {
	// 			// 	all_finish <- 1
	// 			// 	break
	// 			// }
	// 		}
	// 	}
	case cReply: //解析内容为Txs_and_Num_of_New_State结构。更新未完成迁移的数量和迁移的交易数量。如果所有迁移完成，设置最大提交块。更新零块计数，如果连续收到5个空块，停止系统运行。更新提交交易集合，并处理迁移逻辑
		txs_and_numofNS := new(Txs_and_Num_of_New_State)
		//type Txs_and_Num_of_New_State struct {
		//Txs              []*core.Transaction
		//BlockSize        int
		//Num_of_New_State int
		//}
		err := json.Unmarshal(content, txs_and_numofNS)
		if err != nil {
			log.Panic(err)
		}
		txs := txs_and_numofNS.Txs
		blocksize := txs_and_numofNS.BlockSize

		max_commit_block_lock.Lock()
		num_of_unfinished_migration_lock.Lock()
		num_of_unfinished_migrated_TXs_lock.Lock()

		num_of_unfinished_migration -= txs_and_numofNS.Num_of_New_State
		if num_of_unfinished_migration < 0 {
			log.Panic("剩余未迁移账户数量为负数，出错啦！")
		} else if num_of_unfinished_migration == 0 && num_of_unfinished_migrated_TXs == 0 {
			fmt.Println("所有迁移都完成啦！")
			if max_commit_block < 0 {
				max_commit_block = params.Config.Max_Commit_Block * params.Config.Shard_num
			}
		}

		num_of_unfinished_migrated_TXs_lock.Unlock()
		num_of_unfinished_migration_lock.Unlock()
		max_commit_block_lock.Unlock()

		// max_commit_lock.Lock()
		// fmt.Printf("距离10W上链交易长度：%v\n\n", max_commit-len(txs))
		// max_commit_lock.Unlock()

		max_commit_block_lock.Lock()
		fmt.Printf("距离下一次迁移长度：%v\n\n", max_commit_block)
		max_commit_block_lock.Unlock()

		zero_count_lock.Lock()
		shard := txs_and_numofNS.ShardID
		injDone := atomic.LoadUint32(&injectDone) == 1
		if !injDone {
			// 注入未完成：不因空块累计停止；非空块仍清零对应分片 streak（兼容旧节点未带 ShardID 时走下方 legacy）。
			if shard != "" && blocksize != 0 {
				if _, ok := emptyStreakByShard[shard]; !ok {
					emptyStreakByShard[shard] = 0
				}
				emptyStreakByShard[shard] = 0
			}
		} else if shard == "" {
			// 旧节点未填 ShardID：沿用全局连续空回复计数
			if blocksize == 0 {
				zero_count++
				if zero_count == 5*params.Config.Shard_num {
					fmt.Println("收到连续空块回复（旧协议无 ShardID），停止系统运行！")
					all_finish <- 1
				}
			} else {
				zero_count = 0
			}
		} else {
			if _, ok := emptyStreakByShard[shard]; !ok {
				emptyStreakByShard[shard] = 0
			}
			if blocksize == 0 {
				emptyStreakByShard[shard]++
			} else {
				emptyStreakByShard[shard] = 0
			}
			allAtThreshold := true
			for _, id := range params.ActiveShardIDs() {
				if emptyStreakByShard[id] < 5 {
					allAtThreshold = false
					break
				}
			}
			if allAtThreshold {
				fmt.Printf("每个活跃分片已连续 5 个空块（inject 已完成），停止系统运行。emptyStreakByShard=%+v\n", emptyStreakByShard)
				all_finish <- 1
			}
		}
		zero_count_lock.Unlock()

		// max_commit_lock.Lock()
		max_commit_block_lock.Lock()
		commit_tx_set_lock.Lock()
		num_of_unfinished_migration_lock.Lock()
		num_of_unfinished_migrated_TXs_lock.Lock()
		if max_commit_block == 0 {
			fmt.Printf("\n 每个分片出%d啦！ \n", params.Config.Max_Commit_Block)
		}
		for _, tx := range txs {

			// max_commit--
			commit_tx_set = append(commit_tx_set, tx)

			onlyOnce_lock.Lock()
			// if params.Config.PorC != "MONOXIDE" && params.Config.OnlyOnce > 0 && max_commit <= 0 && num_of_unfinished_migration == 0 && num_of_unfinished_migrated_TXs == 0 {
			if params.Config.PorC != "MONOXIDE" && params.Config.OnlyOnce > 0 && max_commit_block <= 0 && num_of_unfinished_migration == 0 && num_of_unfinished_migrated_TXs == 0 {
				num_of_unfinished_migration = 1
				num_of_unfinished_migrated_TXs = 1
				params.Config.OnlyOnce--
				SendMigrateWanted()
			}
			onlyOnce_lock.Unlock()
		}
		max_commit_block--

		num_of_unfinished_migrated_TXs_lock.Unlock()
		num_of_unfinished_migration_lock.Unlock()
		commit_tx_set_lock.Unlock()
		// max_commit_lock.Unlock()
		max_commit_block_lock.Unlock()
	case cAnnounce:
		announce := new(Announce)
		err := json.Unmarshal(content, announce)
		if err != nil {
			log.Panic(err)
		}
		fmt.Printf("client已接收到分片%v发来通知 \n", announce.ShardID)
		sendtxlock.Lock()
		account.Account2ShardLock.Lock()
		for _, v := range announce.TXanns {
			account.Account2Shard[v.Address] = params.ShardTable[announce.ShardID]
		}
		account.Account2ShardLock.Unlock()
		sendtxlock.Unlock()
	case cPendingTXs:
		pendingtxs := []*core.Transaction{}
		err := json.Unmarshal(content, &pendingtxs)
		if err != nil {
			log.Panic(err)
		}

		var tx_set []*core.Transaction
		pending_tx_set_lock.Lock()
		num_of_shard_not_sending_pending_lock.Lock()
		pending_tx_set = append(pending_tx_set, pendingtxs...)
		num_of_shard_not_sending_pending--
		if num_of_shard_not_sending_pending == 0 {
			fmt.Println("所有分片都发送了pending！\n")
			tx_set = append([]*core.Transaction(nil), pending_tx_set...)
			pending_tx_set = []*core.Transaction{}
			num_of_shard_not_sending_pending = params.Config.Shard_num
		}
		num_of_shard_not_sending_pending_lock.Unlock()
		pending_tx_set_lock.Unlock()

		if tx_set != nil {
			go runMigrationFromPending(tx_set)
		}

	case cNumOfMigratedTXsAddrs:
		var NumOfMigratedTXsAddrs int
		err := json.Unmarshal(content, &NumOfMigratedTXsAddrs)
		if err != nil {
			log.Panic(err)
		}

		num_of_shard_not_sending_pending_lock.Lock()
		num_of_unfinished_migrated_TXs_lock.Lock()
		num_of_unfinished_migrated_TXs -= NumOfMigratedTXsAddrs
		if num_of_shard_not_sending_pending == 0 {
			fmt.Println("所有pending交易都迁移完成！\n")
		}
		num_of_unfinished_migrated_TXs_lock.Unlock()
		num_of_shard_not_sending_pending_lock.Unlock()

	case cUnchangedState:
		var UnchangedState int
		err := json.Unmarshal(content, &UnchangedState)
		if err != nil {
			log.Panic(err)
		}

		num_of_unfinished_migration_lock.Lock()
		num_of_unfinished_migration -= UnchangedState
		num_of_unfinished_migration_lock.Unlock()

	default:
		log.Panic()
	}

}

func runMigrationFromPending(tx_set []*core.Transaction) {
	new_addr2shard := map[string]int{}
	new_addrs := []string{}
	algorithmbegin := time.Now().UnixMilli()
	if params.Config.PorC == "PageRank" {
				//pagerank
				graph, addrs := algorithm.Pagerank_Tx2graph_And_Addrs(tx_set)
				Numbda := 0.85
				iters := 20
				points := algorithm.Pagerank(graph, addrs, account.Account2Shard, Numbda, iters, params.Config.Shard_num)
				new_addr2shard = algorithm.Allocate(points)
				for _, acc := range addrs {
					if account.Account2Shard[acc] != new_addr2shard[acc] {
						new_addrs = append(new_addrs, acc)
					} else {
						delete(new_addr2shard, acc)
					}
				}
			}

			if params.Config.PorC == "CLPA" {
				//clpa
				clapstate := new(algorithm.CLPAState)
				belta := 0.5
				iterarion := 10
				clapstate.Init_CLPAState(belta, iterarion, params.Config.Shard_num)
				for _, tx := range tx_set {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					clapstate.AddEdge(s, r)
				}
				addrs_modified, map_modified := clapstate.CLPA_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != int(map_modified[acc]) {
						new_addr2shard[acc] = int(map_modified[acc])
						new_addrs = append(new_addrs, acc)
					}
				}
			}

			if params.Config.PorC == "LBF" {
				//lbf
				lbfstate := new(algorithm.LBFState)
				alpha := 0.5
				lbfstate.Init_LBFState(alpha, params.Config.Shard_num)
				for _, tx := range tx_set {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					if tx.IsRelay || tx.Relay_Lock {
						// 只有 r指向s，相当于r的权重+1，s不加
						lbfstate.AddEdge(s, r, 1)
					}
					lbfstate.AddEdge(s, r, 0)
				}
				addrs_modified, map_modified := lbfstate.LBF_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != map_modified[acc] {
						new_addr2shard[acc] = map_modified[acc]
						new_addrs = append(new_addrs, acc)
					}
				}
			}

			if params.Config.PorC == "METIS" {
				metisstate := new(algorithm.METISState)
				alpha := 0.5
				metisstate.Init_METISState(alpha, params.Config.Shard_num)
				for _, tx := range tx_set {
					from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
					if from == to {
						continue
					}
					s := algorithm.Vertex{Addr: from}
					r := algorithm.Vertex{Addr: to}
					metisstate.AddEdge(s, r, 0)
				}
				addrs_modified, map_modified := metisstate.METIS_Partition()
				for _, acc := range addrs_modified {
					if account.Account2Shard[acc] != map_modified[acc] {
						new_addr2shard[acc] = map_modified[acc]
						new_addrs = append(new_addrs, acc)
					}
				}
			}

			if params.Config.Stop_When_Migrating {
				for addr, shard := range new_addr2shard {
					account.Account2Shard[addr] = shard
				}
			}
	algorithmend := time.Now().UnixMilli()
	num_of_unfinished_migration_lock.Lock()
	num_of_unfinished_migrated_TXs_lock.Lock()
	num_of_unfinished_migration = len(new_addrs)
	num_of_unfinished_migrated_TXs = len(new_addrs)
	num_of_unfinished_migrated_TXs_lock.Unlock()
	num_of_unfinished_migration_lock.Unlock()

	SendNewAddr2Shard(new_addrs, new_addr2shard)
	s := fmt.Sprintf("%v %v", algorithmend-StartTime, algorithmend-algorithmbegin)
	migrationlog.Write(strings.Split(s, " "))
	migrationlog.Flush()

	commit_tx_set_lock.Lock()
	commit_tx_set = []*core.Transaction{}
	commit_tx_set_lock.Unlock()
}

// func CountTx(path string) int {
// 	file, err := os.Open(path)
// 	if err != nil {
// 		log.Panic()
// 	}
// 	defer file.Close()
// 	r := csv.NewReader(file)
// 	_, err = r.Read()
// 	if err != nil {
// 		log.Panic()
// 	}
// 	cnt := 0
// 	for {
// 		_, err := r.Read()
// 		// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
// 		if err != nil && err != io.EOF {
// 			log.Panic()
// 		}
// 		if err == io.EOF {
// 			break
// 		}
// 		cnt += 1
// 	}
// 	return cnt
// }

func Sendtime(t int64) {
	// 将该分片的最新平均交易费用发送到所有分片
	time, err := json.Marshal(t)
	if err != nil {
		log.Panic(err)
	}
	m := jointMessage(cLLT, time)
	for _, shardID := range params.ActiveShardIDs() {
		nodes := params.NodeTable[shardID]
		if nodes == nil {
			continue
		}
		for _, node := range nodes {
			utils.TcpDial(m, node)
			fmt.Printf("客户端发送初始时间：%v\n", t)
		}
	}
	fmt.Println()
}

// func waitread() {
// 	for {
// 		reader := bufio.NewReader(os.Stdin)
// 		fmt.Println("请输入数字1发送换epoch指令，按回车结束")
// 		fmt.Print("-> ")
// 		//遇到换行符就停
// 		text, err := reader.ReadString('\n')
// 		//删除掉最后的换行符和回车
// 		text = strings.TrimSuffix(text, "\n")
// 		//在Windows中按回车会出现\r，也要删除。即使没有\r也只会返回原本的text
// 		text = strings.TrimSuffix(text, "\r")
// 		if err != nil {
// 			panic(fmt.Errorf("发生致命错误：%w \n", err))
// 		}

// 		switch text {
// 		case "1":
// 			SendEpochChange()
// 		case "2":
// 			for shardID, nodes := range params.NodeTable {
// 				for nodeID, addr := range nodes {
// 					fmt.Printf("客户端向分片%v的节点%v发送终止运行消息\n", shardID, nodeID)
// 					m := jointMessage(cStop, nil)
// 					utils.TcpDial(m, addr)
// 				}
// 			}
// 			all_finish <- 1

// 		default:
// 			fmt.Println("只能输入数字1")
// 		}
// 	}
// }

func SendEpochChange() {
	// str := "epochchange"
	// ec, err := json.Marshal(str)
	// if err != nil {
	// 	log.Panic(err)
	// }

	message := jointMessage(cEpochCh, nil)

	for _, k := range params.ActiveShardIDs() {
		v := params.NodeTable[k]
		if v == nil {
			continue
		}
		target_leader := v["N0"]
		fmt.Printf("正在向分片%v的主节点发送epochCHANGE消息\n", k)
		go utils.TcpDial(message, target_leader)
	}
}

func SendMigrateWanted() {
	pending_tx_set_lock.Lock()
	pending_tx_set = []*core.Transaction{}
	pending_tx_set_lock.Unlock()
	num_of_shard_not_sending_pending_lock.Lock()
	num_of_shard_not_sending_pending = params.Config.Shard_num
	num_of_shard_not_sending_pending_lock.Unlock()

	message := jointMessage(cMigrateWanted, nil)

	for _, k := range params.ActiveShardIDs() {
		v := params.NodeTable[k]
		if v == nil {
			continue
		}
		target_leader := v["N0"]
		fmt.Printf("正在向分片%v的主节点发送MigrateWanted消息\n", k)
		go utils.TcpDial(message, target_leader)
	}
}

// waitForShardLeaders 轮询各活跃分片 N0 的监听端口，避免客户端早于分片进程就绪拨号。
func waitForShardLeaders(timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	ids := params.ActiveShardIDs()
	for time.Now().Before(deadline) {
		allUp := true
		for _, shard := range ids {
			v := params.NodeTable[shard]
			if v == nil {
				continue
			}
			addr := v["N0"]
			c, err := net.DialTimeout("tcp", addr, 800*time.Millisecond)
			if err != nil {
				allUp = false
				break
			}
			_ = c.Close()
		}
		if allUp {
			return
		}
		time.Sleep(400 * time.Millisecond)
	}
	log.Printf("waitForShardLeaders: 超时 %v，仍继续发送映射（部分 N0 可能尚未就绪）", timeout)
}

func SendNewAddr2Shard(new_addrs []string, new_addr2shard map[string]int) {
	if len(new_addrs) == 0 && len(new_addr2shard) == 0 {
		return
	}
	waitForShardLeaders(120 * time.Second)

	nam := &NaM{new_addrs, new_addr2shard}
	na, err := json.Marshal(&nam)
	if err != nil {
		log.Panic(err)
	}

	message := jointMessage(cNewMap, na)
	migration_count++
	fmt.Printf("\n次数：%v  时间：%v\n", migration_count, time.Now().Unix()-StartTime)
	for _, k := range params.ActiveShardIDs() {
		v := params.NodeTable[k]
		if v == nil {
			continue
		}
		target_leader := v["N0"]
		fmt.Printf("正在向分片%v的主节点发送新的映射\n", k)
		go utils.TcpDial(message, target_leader)
	}
}

func Get_Initial_Map(path string, Account2Shard map[string]int) {

	file, err := os.Open(path)
	if err != nil {
		log.Panic()
	}
	defer file.Close()

	dataset_flag := 0
	if path == "0to999999_BlockTransaction.csv" || path == "300W.csv" || path == "100W.csv" || path == "50W.csv" || path == "20W.csv" || path == "200W.csv" {
		dataset_flag = 1
	}
	if path == "selectedTxs_300K.csv" {
		dataset_flag = 2
	}

	r := csv.NewReader(file)
	_, err = r.Read()
	if err != nil {
		log.Panic()
	}
	for i := 0; i < 1000000; i++ {
		// for i := 0; i < 500000; i++ {
		row, err := r.Read()
		// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
		if err != nil && err != io.EOF {
			log.Panic()
		}
		if err == io.EOF {
			break
		}
		var senderstr, recipientstr string
		if dataset_flag == 1 {
			if len(row) < 8 {
				continue
			}
			if len(row[3]) < 2 || len(row[4]) < 2 {
				continue
			}
			if row[5] != "None" || row[6] == "1" || row[7] == "1" || len(row[4][2:]) != 40 || len(row[3][2:]) != 40 || row[4] == row[3] {
				continue
			}
			senderstr, recipientstr = row[3][2:], row[4][2:]
		} else if dataset_flag == 2 {
			if len(row) < 9 || len(row[3]) < 2 || len(row[4]) < 2 {
				continue
			}
			senderstr, recipientstr = row[3][2:], row[4][2:]
		} else {
			if len(row) < 3 || len(row[1]) < 2 || len(row[2]) < 2 {
				continue
			}
			senderstr, recipientstr = row[1][2:], row[2][2:]
		}

		//将初始账户的映射弄好
		Account2Shard[senderstr] = utils.Addr2Shard(senderstr)
		Account2Shard[recipientstr] = utils.Addr2Shard(recipientstr)
	}
}

func Get_Initial_Map_And_TXS(path string, Account2Shard map[string]int) []*core.Transaction {
	txs := make([]*core.Transaction, 0)
	txid := 0
	dataset_flag := 0
	if path == "0to999999_BlockTransaction.csv" || path == "300W.csv" || path == "100W.csv" || path == "50W.csv" || path == "20W.csv" || path == "200W.csv" {
		dataset_flag = 1
		fmt.Printf("特殊格式文件 %s\n", path)
	}
	if path == "selectedTxs_300K.csv" {
		dataset_flag = 2
		fmt.Printf("300K 格式文件 %s\n", path)
	}

	file, err := os.Open(path)
	if err != nil {
		log.Panic()
	}
	defer file.Close()

	r := csv.NewReader(file)
	_, err = r.Read()
	if err != nil {
		log.Panic()
	}

	//循环按行读取交易
	for i := 0; i < 1000000; i++ {
		// for i := 0; i < 500000; i++ {
		row, err := r.Read()
		// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
		if err != nil && err != io.EOF {
			log.Panic()
		}
		if err == io.EOF {
			break
		}

		value := new(big.Int)
		var senderstr, recipientstr string
		var sender, recipient []byte
		var ok bool

		// 先检查是否为特殊格式
		if dataset_flag == 1 {
			if len(row) < 9 {
				continue
			}
			if row[5] != "None" || row[6] == "1" || row[7] == "1" || len(row[4][2:]) != 40 || len(row[3][2:]) != 40 || row[4] == row[3] {
				fmt.Printf("特殊格式交易检查通过 %v\n", row)
				continue
			}
			senderstr, recipientstr = row[3][2:], row[4][2:]
			sender, _ = hex.DecodeString(row[3][2:])
			recipient, _ = hex.DecodeString(row[4][2:])
			value, ok = value.SetString(row[8], 10)
			if !ok {
				log.Panic()
			}
		} else if dataset_flag == 2 {
			//300K 格式处理
			if len(row) < 9 {
				continue
			}
			// 检查地址格式是否正确
			if len(row[3]) < 2 || len(row[4]) < 2 {
				fmt.Printf("300K交易地址格式错误 %v\n", row)
				continue
			}
			// 打印前几行的交易信息
			if txid < 3 {
				fmt.Printf("300K交易处理 %v\n", row)
			}
			senderstr, recipientstr = row[3][2:], row[4][2:]
			sender, _ = hex.DecodeString(row[3][2:])
			recipient, _ = hex.DecodeString(row[4][2:])
			value, ok = value.SetString(row[8], 10)
			if !ok {
				fmt.Printf("300K交易金额解析错误 %v\n", row)
				continue
			}
		} else {
			// 普通格式处理
			if len(row) < 3 {
				continue
			}
			if len(row[1]) < 2 || len(row[2]) < 2 {
				continue
			}
			senderstr, recipientstr = row[1][2:], row[2][2:]
			sender, _ = hex.DecodeString(row[1][2:])
			recipient, _ = hex.DecodeString(row[2][2:])
			// 设置默认值
			value.SetInt64(0)
		}

		txs = append(txs, &core.Transaction{
			Sender:             sender,
			Recipient:          recipient,
			Value:              value,
			Id:                 txid,
			RequestTime:        -1,
			Second_RequestTime: -1,
			TXmig1_Time:        -1,
			TXmig2_Time:        -1,
			CommitTime:         -1,
			LockTime:           -1,
			UnlockTime:         -1,
		})
		txid++

		//将初始账户的映射弄好
		Account2Shard[senderstr] = utils.Addr2Shard(senderstr)
		Account2Shard[recipientstr] = utils.Addr2Shard(recipientstr)
	}
	fmt.Printf("成功读取 %d 笔交易\n", len(txs))
	return txs
}

func InjectTXS(txs []*core.Transaction) {
	cnt := 0
	for {
		inject_speed := params.Config.Inject_speed
		time.Sleep(2 * time.Second)
		upperBound := utils.Min(cnt+2*inject_speed, len(txs))

		sendtxlock.Lock()
		account.Account2ShardLock.Lock()
		to_send := make([][]*core.Transaction, params.Config.Shard_num)

		for i := cnt; i < upperBound; i++ {
			addr := hex.EncodeToString(txs[i].Sender)
			senderSID := account.Account2Shard[addr]
			txs[i].RequestTime = time.Now().UnixMilli()
			txs[i].TxHash = txs[i].Hash()
			to_send[senderSID] = append(to_send[senderSID], txs[i])
		}
		account.Account2ShardLock.Unlock()
		sendtxlock.Unlock()

		for _, k := range params.ActiveShardIDs() {
			v := params.NodeTable[k]
			if v == nil {
				continue
			}
			sid := params.ShardTable[k]
			ktxs := to_send[sid]
			if len(ktxs) != 0 {
				target_leader := v["N0"]
				c := TxFromClient{
					Txs: ktxs,
				}
				bc, err := json.Marshal(c)
				if err != nil {
					log.Panic(err)
				}

				// fmt.Printf("正在向分片%v的主节点发送交易\n", k)
				message := jointMessage(cClient, bc)
				go utils.TcpDial(message, target_leader)
			}
		}

		cnt = upperBound

		if cnt == len(txs) {
			fmt.Println()
			// fmt.Println("注入100000")
			// fmt.Println("注入10000")
			fmt.Printf("注入%v\n", cnt)
			fmt.Println()
			atomic.StoreUint32(&injectDone, 1)
			break
		}
	}
}
