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
	txsFromClient := new(TxFromClient)
	err := json.Unmarshal(content, txsFromClient)
	if err != nil {
		log.Panic(err)
	}
	incoming := len(txsFromClient.Txs)
	fmt.Printf("[handleTxFromClient] %s %s recv client batch, tx_count=%d\n",
		params.Config.ShardID, p.Node.nodeID, incoming)

	tx2 := make([]*core.Transaction, 0)
	self_shardID := params.ShardTable[params.Config.ShardID]

	// 先在无 Tx_pool 锁下解析分片归属，避免与 handleNewMap 抢 Account2ShardLock 死锁
	type localItem struct {
		tx       *core.Transaction
		localTx  bool
		redirect bool
	}
	pending := make([]localItem, 0, len(txsFromClient.Txs))
	for _, tx := range txsFromClient.Txs {
		senderStr := hex.EncodeToString(tx.Sender)
		senderSID := account.Addr2Shard(senderStr)
		recSID := account.Addr2Shard(hex.EncodeToString(tx.Recipient))
		localTx := senderSID == self_shardID ||
			(params.IsMVSSPlus() && recSID == self_shardID && len(tx.RedirectTag) > 0)
		item := localItem{tx: tx, localTx: localTx}
		if localTx && params.IsMVSSPlus() {
			if recSID == self_shardID && len(tx.RedirectTag) > 0 {
				tx.Second_RequestTime = time.Now().UnixMilli()
			}
			if !mvssValidateIncomingTx(tx) {
				continue
			}
			if p.mvssRedirectNewTx(tx) {
				item.redirect = true
			}
		}
		pending = append(pending, item)
	}

	p.Node.CurChain.Tx_pool.Lock.Lock()
	j := 0
	if !params.Config.Not_Lock_immediately {
		for _, item := range pending {
			if !item.localTx {
				tx2 = append(tx2, item.tx)
				continue
			}
			if item.redirect {
				continue
			}
			tx := item.tx
			from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
			if !params.Config.Stop_When_Migrating && params.Config.Not_Lock_Acc_When_Migrating && !params.IsMVSSPlus() {
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
		}
	}

	if params.Config.Not_Lock_immediately {
		for _, item := range pending {
			if !item.localTx {
				tx2 = append(tx2, item.tx)
				continue
			}
			if item.redirect {
				continue
			}
			txsFromClient.Txs[j] = item.tx
			j++
		}
	}

	txsFromClient.Txs = txsFromClient.Txs[:j]
	p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, txsFromClient.Txs...)

	if len(tx2) != 0 {
		p.TrySendTX(tx2)
	}
	fmt.Printf("[handleTxFromClient] %s %s append to Tx_pool.Queue: local_queued=%d forward_other_shard=%d Queue_total_len=%d\n",
		params.Config.ShardID, p.Node.nodeID, len(txsFromClient.Txs), len(tx2), len(p.Node.CurChain.Tx_pool.Queue))

	p.Node.CurChain.Tx_pool.Lock.Unlock()
}
