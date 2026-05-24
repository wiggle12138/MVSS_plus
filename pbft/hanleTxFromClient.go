package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"time"
)

func (p *Pbft) handleTxFromClient(content []byte) {
	// 处理客户端发送的交易，放入交易池
	txsFromClient := new(TxFromClient)
	err := json.Unmarshal(content, txsFromClient)
	if err != nil {
		log.Panic(err)
	}
	incoming := len(txsFromClient.Txs)
	fmt.Printf("[handleTxFromClient] %s %s recv client batch, tx_count=%d\n",
		params.Config.ShardID, p.Node.nodeID, incoming)

	tx2 := make([]*core.Transaction, 0)
	p.Node.CurChain.Tx_pool.Lock.Lock()
	j := 0
	self_shardID := params.ShardTable[params.Config.ShardID]
	if !params.Config.Not_Lock_immediately {
		for _, tx := range txsFromClient.Txs {
			senderStr := hex.EncodeToString(tx.Sender)
			// 必须用 Addr2Shard：分片节点进程里 Account2Shard 往往未预填，直接读 map 缺键会得到 0，误判为分片0。
			senderSID := account.Addr2Shard(senderStr)
			if senderSID == self_shardID {
				from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
				if !params.Config.Stop_When_Migrating && params.Config.Not_Lock_Acc_When_Migrating {
					account.Not_Lock_Acc_Lock.Lock()
					if account.Not_Lock_Acc[from] {
						tx.LockTime = time.Now().UnixMilli()
						tx.SenLock = true
						p.Node.CurChain.Tx_pool.Not_Locking_TX_Pools[from] = append(p.Node.CurChain.Tx_pool.Not_Locking_TX_Pools[from], tx)
						account.Not_Lock_Acc_Lock.Unlock()
						continue
					}
					if account.Not_Lock_Acc[to] {
						tx.LockTime = time.Now().UnixMilli()
						tx.RecLock = true
						p.Node.CurChain.Tx_pool.Not_Locking_TX_Pools[to] = append(p.Node.CurChain.Tx_pool.Not_Locking_TX_Pools[to], tx)
						account.Not_Lock_Acc_Lock.Unlock()
						continue
					}
					account.Not_Lock_Acc_Lock.Unlock()
				}
				//全锁
				if !params.Config.Stop_When_Migrating && params.Config.Lock_Acc_When_Migrating {
					account.Lock_Acc_Lock.Lock()
					if account.Lock_Acc[from] {
						tx.LockTime = time.Now().UnixMilli()
						tx.SenLock = true
						p.Node.CurChain.Tx_pool.Locking_TX_Pools[from] = append(p.Node.CurChain.Tx_pool.Locking_TX_Pools[from], tx)
						account.Lock_Acc_Lock.Unlock()
						continue
					}
					if account.Lock_Acc[to] {
						tx.LockTime = time.Now().UnixMilli()
						tx.RecLock = true
						p.Node.CurChain.Tx_pool.Locking_TX_Pools[to] = append(p.Node.CurChain.Tx_pool.Locking_TX_Pools[to], tx)
						account.Lock_Acc_Lock.Unlock()
						continue
					}
					account.Lock_Acc_Lock.Unlock()
				}
				//半锁
				if !params.Config.Stop_When_Migrating && !params.Config.Lock_Acc_When_Migrating && !params.Config.Not_Lock_Acc_When_Migrating {
					account.Outing_Acc_Before_Announce_Lock.Lock()
					if account.Outing_Acc_Before_Announce[from] {
						tx.LockTime = time.Now().UnixMilli()
						p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools[from] = append(p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools[from], tx)
						account.Outing_Acc_Before_Announce_Lock.Unlock()
						continue
					}
					account.Outing_Acc_Before_Announce_Lock.Unlock()
				}
				txsFromClient.Txs[j] = tx
				j++
			} else {
				tx2 = append(tx2, tx)
			}
		}
	}

	if params.Config.Not_Lock_immediately {
		for _, tx := range txsFromClient.Txs {
			senderStr := hex.EncodeToString(tx.Sender)
			senderSID := account.Addr2Shard(senderStr)
			if senderSID == self_shardID {
				txsFromClient.Txs[j] = tx
				j++
			} else {
				tx2 = append(tx2, tx)
			}
		}
	}

	txsFromClient.Txs = txsFromClient.Txs[:j]
	p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, txsFromClient.Txs...)

	if len(tx2) != 0 {
		p.TrySendTX(tx2)
	}
	queued := len(txsFromClient.Txs)
	qLen := len(p.Node.CurChain.Tx_pool.Queue)
	fmt.Printf("[handleTxFromClient] %s %s append to Tx_pool.Queue: local_queued=%d forward_other_shard=%d Queue_total_len=%d\n",
		params.Config.ShardID, p.Node.nodeID, queued, len(tx2), qLen)

	p.Node.CurChain.Tx_pool.Lock.Unlock()
}
