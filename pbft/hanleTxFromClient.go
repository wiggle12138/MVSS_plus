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
	promoteAfterEnqueue := make(map[string]struct{})

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
		probe := isSyncProbeTxID(tx.Id)
		localTx := senderSID == self_shardID ||
			(params.IsMVSSPlus() && recSID == self_shardID && len(tx.RedirectTag) > 0)
		// 目标片：已重定向的新 tx（sender 为迁入账户）
		if !localTx && params.IsMVSSPlus() && len(tx.RedirectTag) > 0 {
			if ctx, ok := account.GetMigCtx(senderStr); ok && ctx.TargetShard == self_shardID {
				localTx = true
			}
		}
		if probe {
			fmt.Printf("[SyncProbe][Ingress] shard=%s node=%s tx=%d senderSID=%d recSID=%d local=%v tag=%d isRelay=%v\n",
				params.Config.ShardID, p.Node.nodeID, tx.Id, senderSID, recSID, localTx, len(tx.RedirectTag), tx.IsRelay)
		}
		item := localItem{tx: tx, localTx: localTx}
		if localTx && params.IsMVSSPlus() {
			if recSID == self_shardID && len(tx.RedirectTag) > 0 {
				tx.Second_RequestTime = time.Now().UnixMilli()
			}
			if !mvssValidateIncomingTx(tx) {
				if probe {
					fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 校验失败丢弃: tag=%d nonce=%d req=%d secondReq=%d\n",
						params.Config.ShardID, tx.Id, len(tx.RedirectTag), tx.Nonce, tx.RequestTime, tx.Second_RequestTime)
				}
				continue
			}
			mvssNormalizeTargetNewTx(tx, self_shardID)
			if p.mvssRedirectNewTx(tx) {
				item.redirect = true
				if probe {
					fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 已重定向，源片不入池\n", params.Config.ShardID, tx.Id)
				}
			} else if item.localTx {
				if ctx, ok := account.GetMigCtx(senderStr); ok && ctx != nil &&
					ctx.TargetShard == self_shardID && account.IsTXNew(ctx.Mig1Time, tx.RequestTime) &&
					ctx.FSM >= account.MigFSMSyncApplied {
					// delta apply 早于 PhaseB tx2 到达时，须在入池后再次 promote 对齐 nonce
					promoteAfterEnqueue[senderStr] = struct{}{}
				}
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
				if isSyncProbeTxID(item.tx.Id) {
					fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 非本片，走 TrySendTX\n", params.Config.ShardID, item.tx.Id)
				}
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
			if isSyncProbeTxID(tx.Id) {
				fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 本片入池(延迟锁路径)\n", params.Config.ShardID, tx.Id)
			}
			j++
		}
	}

	if params.Config.Not_Lock_immediately {
		for _, item := range pending {
			if !item.localTx {
				tx2 = append(tx2, item.tx)
				if isSyncProbeTxID(item.tx.Id) {
					fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 非本片，走 TrySendTX\n", params.Config.ShardID, item.tx.Id)
				}
				continue
			}
			if item.redirect {
				continue
			}
			txsFromClient.Txs[j] = item.tx
			if isSyncProbeTxID(item.tx.Id) {
				fmt.Printf("[SyncProbe][Ingress] shard=%s tx=%d 本片入池(立即入池路径)\n", params.Config.ShardID, item.tx.Id)
			}
			j++
		}
	}

	txsFromClient.Txs = txsFromClient.Txs[:j]
	// 探针：tx1 插队首便于 prefix-old 先打包；tx3 接在队尾（仍早于 PhaseB 的 tx2，但不与 tx1 同批抢块）
	var probe1Head, probe3Tail, normalTail []*core.Transaction
	for _, tx := range txsFromClient.Txs {
		if isSyncProbeTxID(tx.Id) {
			slot := (tx.Id - core.SyncProbeIDBase) % core.SyncProbeIDStride
			if slot == 1 {
				probe1Head = append(probe1Head, tx)
			} else if slot == 3 {
				probe3Tail = append(probe3Tail, tx)
			} else {
				probe1Head = append(probe1Head, tx)
			}
		} else {
			normalTail = append(normalTail, tx)
		}
	}
	if len(probe1Head) > 0 {
		p.Node.CurChain.Tx_pool.Queue = append(probe1Head, p.Node.CurChain.Tx_pool.Queue...)
	}
	if len(probe3Tail) > 0 || len(normalTail) > 0 {
		p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, append(probe3Tail, normalTail...)...)
	} else if len(probe1Head) == 0 {
		p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, normalTail...)
	}

	if len(tx2) != 0 {
		p.TrySendTX(tx2)
	}
	fmt.Printf("[handleTxFromClient] %s %s append to Tx_pool.Queue: local_queued=%d forward_other_shard=%d Queue_total_len=%d\n",
		params.Config.ShardID, p.Node.nodeID, len(txsFromClient.Txs), len(tx2), len(p.Node.CurChain.Tx_pool.Queue))

	p.Node.CurChain.Tx_pool.Lock.Unlock()
	for addr := range promoteAfterEnqueue {
		p.mvssMaybePromoteAfterNewEnqueued(addr)
	}
}
