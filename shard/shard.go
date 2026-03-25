//Node改为ShardNode结构体，相应的NewNode改为NewShardNode函数
//test_shard.go中也做相应更改

package shard

import (
	"blockEmulator/params"
	"blockEmulator/pbft"
	"fmt"
)

type ShardNode struct {
	P *pbft.Pbft
}

func NewShardNode() *ShardNode {
	n := &ShardNode{}

	// 初始化PBFT实例
	n.P = pbft.NewPBFT()

	// 启动TCP监听
	go n.P.TcpListen()

	// 如果是主节点，启动区块提案
	if params.Config.NodeID == "N0" {
		go n.P.Propose()

		// 根据配置决定交易注入方式
		if !params.Config.ClientSendTX {
			// 节点自身向交易池注入跨分片交易
			// 注：具体的交易注入逻辑由PBFT层处理？
			fmt.Println("节点将自行注入跨分片交易")
		} else {
			fmt.Println("交易将由外部客户端发送")
		}
	}

	fmt.Printf("节点 %s 已启动，所属分片: %s\n", params.Config.NodeID, params.Config.ShardID)

	return n
}
