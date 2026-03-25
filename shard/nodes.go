// definition of node and shard
//在MVSS中应该是废弃，使用shard.go,Node改名为ShardNode结构体

package shard

import (
	"fmt"
)

type Node struct {
	NodeID  uint64
	ShardID uint64
	IPaddr  string
}

func (n *Node) PrintNode() {
	v := []interface{}{
		n.NodeID,
		n.ShardID,
		n.IPaddr,
	}
	fmt.Printf("%v\n", v)
}
