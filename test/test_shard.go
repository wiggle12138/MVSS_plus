package test

import (
	"blockEmulator/params"
	"blockEmulator/pbft"
	"blockEmulator/shard"
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	flag "github.com/spf13/pflag"
)

var (
	node           *shard.ShardNode
	shard_num      int
	shardID        string
	malicious_num  int
	nodeID         string
	testFile       string
	isClient       bool
	maxInjectTxs       int // 0 = unlimited; client only: cap loaded/injected txs for smoke tests
	injectStartTx      int // 0-based offset; client only
	migrationStrategy   string
	enableSyncProbe     bool
	syncProbeMaxAccounts int
	syncProbeDelayMs    int
	syncProbeAccount    string
	// requestlog    *csv.Writer
	EndTime int64
)

func is300KStyleDatasetPath(path string) bool {
	base := filepath.Base(path)
	if base == "selectedTxs_300K.csv" {
		return true
	}
	if strings.HasPrefix(base, "mvss_") && strings.HasSuffix(base, ".csv") {
		return true
	}
	if strings.HasSuffix(base, "_ts.csv") {
		return true
	}
	return false
}

func Test_shard() {
	flag.IntVarP(&shard_num, "shard_num", "S", 1, "indicate that how many shards are deployed")
	flag.StringVarP(&shardID, "shardID", "s", "", "id of the shard to which this node belongs, for example, S0")
	flag.IntVarP(&malicious_num, "malicious_num", "f", 1, "indicate the maximum of malicious nodes in one shard")
	flag.StringVarP(&nodeID, "nodeID", "n", "", "id of this node, for example, N0")
	flag.StringVarP(&testFile, "testFile", "t", "", "path of the input test file")
	flag.BoolVarP(&isClient, "client", "c", false, "whether this node is a client")
	flag.IntVar(&maxInjectTxs, "maxInjectTxs", 0, "client only: inject at most this many txs (0=all). For quick block tests.")
	flag.IntVar(&injectStartTx, "injectStartTx", 0, "client only: start injecting from this 0-based tx offset.")
	flag.StringVarP(&migrationStrategy, "migrationStrategy", "m", "", "migration: original|MVSS|MVSS-Delta|SOTA-Lock|Fine-tuned-Lock|stop_epoch (aliases: lock, finetuned, MVSS+; empty=config default)")
	flag.BoolVar(&enableSyncProbe, "enableSyncProbe", false, "client only: inject MVSS sync probe txs on migration (MVSS/MVSS-Delta)")
	flag.IntVar(&syncProbeMaxAccounts, "syncProbeMaxAccounts", 0, "client only: max outgoing accounts per migration probe round (0=config default 3)")
	flag.IntVar(&syncProbeDelayMs, "syncProbeDelayMs", 0, "client only: PhaseB delay ms after NewMap (0=config: 2*Block_interval)")
	flag.StringVar(&syncProbeAccount, "syncProbeAccount", "", "client only: force probe on this hex addr if it migrates out")

	flag.Parse() //解析命令行参数

	applyMigrationConfig := func(cfg *params.ChainConfig) {
		if migrationStrategy != "" {
			cfg.MigrationStrategy = params.ParseMigrationStrategy(migrationStrategy)
		}
		params.ApplyMigrationStrategy(cfg)
		fmt.Printf("MigrationStrategy=%s (Stop=%v Lock=%v NotLock=%v)\n",
			cfg.MigrationStrategy, cfg.Stop_When_Migrating, cfg.Lock_Acc_When_Migrating, cfg.Not_Lock_Acc_When_Migrating)
	}

	if isClient {
		if testFile == "" {
			log.Panic("参数不正确！")
		}
		// 修改全局变量 Config，之后其他地方会调用
		config := params.Config
		config.NodeID = nodeID
		config.ShardID = shardID
		config.Malicious_num = int(malicious_num)
		config.Shard_num = int(shard_num)
		if maxInjectTxs > 0 {
			config.MaxInjectTxs = maxInjectTxs
		}
		if injectStartTx > 0 {
			config.InjectStartTx = injectStartTx
		}
		if enableSyncProbe {
			config.EnableSyncProbe = true
		}
		if syncProbeMaxAccounts > 0 {
			config.SyncProbeMaxAccounts = syncProbeMaxAccounts
		}
		if syncProbeDelayMs > 0 {
			config.SyncProbeDelayMs = syncProbeDelayMs
		}
		if syncProbeAccount != "" {
			config.SyncProbeAccount = syncProbeAccount
		}
		applyMigrationConfig(config)
		if config.EnableSyncProbe {
			fmt.Printf("SyncProbe enabled maxAccounts=%d delayMs=%d account=%q (MVSS=%v)\n",
				config.SyncProbeMaxAccounts, config.SyncProbeDelayMs, config.SyncProbeAccount, params.IsMVSS())
		}
		pbft.RunClient(testFile)
		return
	}
	if shard_num == 1 {
		shardID = "S0"
	}
	if shardID == "" || nodeID == "" || testFile == "" {
		log.Panic("参数不正确！")
	}

	//下面是分片节点的逻辑

	// 修改全局变量 Config，之后其他地方会调用
	config := params.Config
	config.NodeID = nodeID
	config.ShardID = shardID
	config.Malicious_num = int(malicious_num)
	config.Shard_num = int(shard_num)
	config.Path = testFile
	applyMigrationConfig(config)
	// for i := 0; i < 184379; i++ {
	// 	params.Init_addrs = append(params.Init_addrs, utils.Int2hexString(i))
	// }

	file, err := os.Open(config.Path)
	if err != nil {
		log.Panic()
	}
	// defer file.Close()

	r := csv.NewReader(file)
	_, err = r.Read()
	if err != nil {
		log.Panic()
	}

	// 初始化读取所有账户
	isExist := make(map[string]bool)
	for i := 0; i < 1000000; i++ {
		// for i:=0; i<500000; i++{
		row, err := r.Read()
		// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
		if err != nil && err != io.EOF {
			log.Panic()
		}
		if err == io.EOF {
			break
		}
		var senderstr, recipientstr string
		if config.Path == "0to999999_BlockTransaction.csv" || config.Path == "300W.csv" || config.Path == "100W.csv" || config.Path == "20W.csv" || config.Path == "50W.csv" || config.Path == "200W.csv" {
			// 特殊数据集格式：地址在第 4/5 列（索引 3/4）
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
		} else if config.Path == "selectedTxs_300K.csv" || is300KStyleDatasetPath(config.Path) {
			// 与 pbft/client.go Get_Initial_Map_And_TXS 中 dataset_flag==2 一致：地址在列 3/4，金额在列 8
			if len(row) < 9 {
				continue
			}
			if len(row[3]) < 2 || len(row[4]) < 2 {
				continue
			}
			senderstr, recipientstr = row[3][2:], row[4][2:]
		} else {
			// 普通数据集格式：地址在第 2/3 列（索引 1/2）
			if len(row) < 3 {
				continue
			}
			if len(row[1]) < 2 || len(row[2]) < 2 {
				continue
			}
			senderstr, recipientstr = row[1][2:], row[2][2:]
		}

		if !isExist[senderstr] {
			isExist[senderstr] = true
			params.Init_addrs = append(params.Init_addrs, senderstr)
		}
		if !isExist[recipientstr] {
			isExist[recipientstr] = true
			params.Init_addrs = append(params.Init_addrs, recipientstr)
		}
	}
	isExist = nil

	// if config.NodeID == "N0" {
	// 	// config.Path = testFile
	// 	// csvFile, err := os.Create("./log/" + shardID + "_requesttime.csv")
	// 	// if err != nil {
	// 	// 	log.Panic(err)
	// 	// }
	// 	// requestlog = csv.NewWriter(csvFile)
	// 	// requestlog.Write([]string{"txid", "waiting_time", "is_ctx", "1st_queueing_time", "2nd_queueing_time"})
	// 	// requestlog.Flush()
	// }

	//NewShardNode创建分片节点

	if _, ok := params.NodeTable[shardID][nodeID]; ok {
		node = shard.NewShardNode()
	} else {
		log.Fatal("无此节点编号！")
	}

	file.Close()

	<-node.P.Stop
	fmt.Printf("节点收到终止节点消息，停止运行\n")

	// node.P.Node.CurChain.StatusTrie.PrintState()
	// fmt.Println(account.Account2Shard)
	// fmt.Println(account.AccountInOwnShard)
	// for _,v := range node.P.Node.CurChain.Tx_pool.Queue {
	// 	v.PrintTx()
	// }
}
