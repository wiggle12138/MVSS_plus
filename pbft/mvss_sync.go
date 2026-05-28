package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/trie"
)

// mvssBuildMigCtx 在收到新映射时为迁出账户建立 MVSS+ 上下文与 TXmig1 元数据。
func (p *Pbft) mvssBuildMigCtx(addr string, toShard int, mig1Time int64) *core.TXmig1 {
	hexAddr, _ := hex.DecodeString(addr)
	st, err := trie.New(trie.TrieID(common.BytesToHash(p.Node.CurChain.CurrentBlock.Header.StateRoot)), p.Node.CurChain.Triedb)
	if err != nil {
		log.Panic(err)
	}
	enc := st.Get(hexAddr)
	state := &account.AccountState{Balance: new(big.Int), Nonce: 0, Migrate: -1}
	if enc != nil {
		state = account.DecodeAccountState(enc)
	} else {
		fmt.Printf("[MVSS+] 账户 %s 不在状态树，使用默认状态继续迁移\n", addr)
	}

	orderList := make(map[int]int64)
	arrivalList := make(map[int]int64)
	nextNonce := state.Nonce
	pending := make([]*core.Transaction, 0)
	p.Node.CurChain.Tx_pool.Lock.Lock()
	for _, tx := range p.Node.CurChain.Tx_pool.Queue {
		from := hex.EncodeToString(tx.Sender)
		to := hex.EncodeToString(tx.Recipient)
		if from != addr && to != addr {
			continue
		}
		if tx.RequestTime <= 0 {
			continue
		}
		if account.IsTXNew(mig1Time, tx.RequestTime) {
			continue
		}
		pending = append(pending, tx)
	}
	p.Node.CurChain.Tx_pool.Lock.Unlock()

	// 按客户端逻辑时间戳排序后分配 nonce
	for i := 0; i < len(pending); i++ {
		for j := i + 1; j < len(pending); j++ {
			if pending[i].OrderTimestamp() > pending[j].OrderTimestamp() {
				pending[i], pending[j] = pending[j], pending[i]
			}
		}
	}
	for _, tx := range pending {
		tx.Nonce = nextNonce
		nextNonce++
		orderList[tx.Id] = tx.OrderTimestamp()
		arrivalList[tx.Id] = tx.RequestTime
	}

	syncNeeded := len(pending) > 0
	ctx := &account.MigAccountCtx{
		TargetShard:   toShard,
		Mig1Time:      mig1Time,
		LastCN:        state.Nonce,
		SyncNeeded:    syncNeeded,
		MigNonce:      state.Nonce,
		NextNonce:     nextNonce,
		OrderList:     orderList,
		ArrivalList:   arrivalList,
		PausedTxIDs:   make(map[int]bool),
		FSM:           account.MigFSMActive,
		LastDeltaHash: nil,
	}
	account.SetMigCtx(addr, ctx)

	return &core.TXmig1{
		Address:      addr,
		FromshardID:  params.ShardTable[params.Config.ShardID],
		ToshardID:    toShard,
		Request_Time: mig1Time,
		Sync:         syncNeeded,
		OrderList:    orderList,
		LastCN:       state.Nonce,
	}
}

// mvssRedirectNewTx 源分片将迁移后新交易重定向到目标分片。
func (p *Pbft) mvssRedirectNewTx(tx *core.Transaction) bool {
	if tx.RequestTime <= 0 {
		tx.RequestTime = time.Now().UnixMilli()
	}
	from := hex.EncodeToString(tx.Sender)
	ctx, ok := account.GetMigCtx(from)
	if !ok || !account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
		return false
	}
	tx.Nonce = ctx.NextNonce
	ctx.NextNonce++
	ctx.RegisterOrder(tx.Id, tx.OrderTimestamp(), tx.RequestTime)
	account.DetectInterleave(ctx)

	tx.RedirectTag = core.RedirectTag(from, ctx.Mig1Time, ctx.MigNonce)
	tx.IsRelay = true
	target := params.ShardTableInt2Str[ctx.TargetShard]
	if target == "" {
		return false
	}
	leader := params.NodeTable[target]["N0"]
	payload, err := json.Marshal(TxFromClient{Txs: []*core.Transaction{tx}})
	if err != nil {
		log.Panic(err)
	}
	go utils.TcpDial(jointMessage(cClient, payload), leader)
	return true
}

// mvssValidateIncomingTx 目标分片校验重定向新交易的标签与 nonce。
func mvssValidateIncomingTx(tx *core.Transaction) bool {
	to := hex.EncodeToString(tx.Recipient)
	ctx, ok := account.GetMigCtx(to)
	if !ok {
		// 迁入账户可能以 recipient 身份接收
		from := hex.EncodeToString(tx.Sender)
		ctx, ok = account.GetMigCtx(from)
	}
	if !ok {
		return true // 非迁移账户，走原逻辑
	}
	if len(tx.RedirectTag) == 0 {
		return false // 无标签视为重放
	}
	if !core.ValidRedirectTag(tx.RedirectTag, to, ctx.Mig1Time, ctx.MigNonce) {
		from := hex.EncodeToString(tx.Sender)
		if !core.ValidRedirectTag(tx.RedirectTag, from, ctx.Mig1Time, ctx.MigNonce) {
			return false
		}
	}
	if tx.Nonce <= ctx.LastCN {
		return false // nonce 已消耗，防双花/重放
	}
	return true
}

// TrySendTXsync 向目标分片发送 TXsync。
func (p *Pbft) TrySendTXsync(syncs []*core.TXsync, targetShardID int) {
	if len(syncs) == 0 {
		return
	}
	target := params.ShardTableInt2Str[targetShardID]
	if target == "" {
		return
	}
	leader := params.NodeTable[target]["N0"]
	msg := SyncMsg{TXsyncs: syncs, ShardID: params.Config.ShardID}
	bc, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	for _, s := range syncs {
		if s == nil {
			continue
		}
		writeSyncLog("send", "sync", s.Address, s.StartN, s.EndN, true, "", len(bc))
	}
	go utils.TcpDial(jointMessage(cTXsync, bc), leader)
}

func (p *Pbft) handleTXsync(content []byte) {
	if params.IsMVSSDelta() {
		// MVSS-Delta 仅走 TXsyncDelta 路径。
		return
	}
	msg := new(SyncMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	writeSyncLog("recv", "sync", "", 0, 0, true, "", len(content))
	fmt.Printf("[MVSS+] %s 收到分片 %s 的 TXsync，条数=%d\n",
		params.Config.ShardID, msg.ShardID, len(msg.TXsyncs))

	for _, s := range msg.TXsyncs {
		p.mvssApplySync(s)
	}
	// 目标分片处理完新交易后回传状态（简化：直接回传 StateNew）
	reply := make([]*core.TXsync, 0, len(msg.TXsyncs))
	for _, s := range msg.TXsyncs {
		if s.StateNew == nil {
			continue
		}
		reply = append(reply, &core.TXsync{
			Address:     s.Address,
			FromShard:   params.Config.ShardID,
			StateOld:    s.StateNew,
			StateNew:    s.StateNew,
			StartN:      s.StartN,
			EndN:        s.EndN,
			RequestTime: time.Now().UnixMilli(),
		})
		writeSyncLog("ack_send", "sync", s.Address, s.StartN, s.EndN, true, "", 0)
	}
	if len(reply) == 0 {
		return
	}
	// 回传给源分片
	srcShard := msg.ShardID
	leader := params.NodeTable[srcShard]["N0"]
	out := SyncMsg{TXsyncs: reply, ShardID: params.Config.ShardID}
	bc, err := json.Marshal(out)
	if err != nil {
		log.Panic(err)
	}
	go utils.TcpDial(jointMessage(cTXsync, bc), leader)
}

// mvssApplySync 记录 TXsync 带来的目标分片账户状态，待下次出块合并。
func (p *Pbft) mvssApplySync(s *core.TXsync) {
	if s == nil || s.StateNew == nil {
		writeSyncLog("apply", "sync", "", 0, 0, false, "state_new 为空", 0)
		return
	}
	ns := account.DecodeAccountState(s.StateNew.Encode())
	ns.Migrate = -1
	ns.Location = params.ShardTable[params.Config.ShardID]
	account.SetMigPendingState(s.Address, ns)
	writeSyncLog("apply", "sync", s.Address, s.StartN, s.EndN, true, "", 0)
}

// mvssTriggerSyncIfNeeded 块提交后若检测到交错则向目标分片发送 TXsync。
func (p *Pbft) mvssTriggerSyncIfNeeded(block *core.Block, st *trie.Trie) {
	if !params.IsMVSS() {
		return
	}
	for _, tx := range block.Transactions {
		from := hex.EncodeToString(tx.Sender)
		if ctx, ok := account.GetMigCtx(from); ok {
			account.DetectInterleave(ctx)
		}
	}
	account.MigCtxLock.RLock()
	addrs := make([]string, 0, len(account.MigCtx))
	for addr, ctx := range account.MigCtx {
		if ctx.SyncNeeded && ctx.FSM == account.MigFSMPauseOld {
			addrs = append(addrs, addr)
		}
	}
	account.MigCtxLock.RUnlock()

	for _, addr := range addrs {
		ctx, _ := account.GetMigCtx(addr)
		if ctx == nil {
			continue
		}
		if aborted, reason := account.IsMigAborted(addr); aborted {
			fmt.Printf("[MVSS-Delta] 账户 %s 已中止，跳过同步: %s\n", addr, reason)
			continue
		}
		hexAddr, _ := hex.DecodeString(addr)
		enc := st.Get(hexAddr)
		if enc == nil {
			continue
		}
		stateNew := account.DecodeAccountState(enc)
		stateOld := &account.AccountState{
			Nonce:    ctx.LastCN,
			Migrate:  ctx.TargetShard,
			Location: params.ShardTable[params.Config.ShardID],
		}
		account.BalanceBeforeOutLock.Lock()
		if b, ok := account.BalanceBeforeOut[addr]; ok {
			stateOld.Balance = new(big.Int).Set(b)
		} else {
			stateOld.Balance = new(big.Int).Set(stateNew.Balance)
		}
		account.BalanceBeforeOutLock.Unlock()
		mpNew := &core.ProofDB{}
		_ = st.Prove(hexAddr, 0, mpNew)
		if params.IsMVSSDelta() {
			deltaBalance := new(big.Int).Sub(stateNew.Balance, stateOld.Balance)
			deltaNonce := int64(stateNew.Nonce) - int64(ctx.LastCN)
			if deltaNonce < 0 {
				mvssAbortDelta(addr, "delta nonce 为负")
				continue
			}
			delta := &core.TXsyncDelta{
				Address:      addr,
				FromShard:    params.Config.ShardID,
				DeltaBalance: deltaBalance,
				DeltaNonce:   deltaNonce,
				StartN:       ctx.LastCN,
				EndN:         stateNew.Nonce,
				PrevHash:     append([]byte(nil), ctx.LastDeltaHash...),
				RequestTime:  time.Now().UnixMilli(),
				Ack:          false,
			}
			delta.DeltaHash = delta.CalcDeltaHash()
			ctx.LastDeltaHash = append([]byte(nil), delta.DeltaHash...)
			ctx.FSM = account.MigFSMSyncOut
			p.TrySendTXsyncDelta([]*core.TXsyncDelta{delta}, ctx.TargetShard)
			continue
		}
		sync := &core.TXsync{
			Address:     addr,
			FromShard:   params.Config.ShardID,
			StateOld:    stateOld,
			StateNew:    stateNew,
			MPNew:       mpNew,
			StartN:      ctx.LastCN,
			EndN:        stateNew.Nonce,
			RequestTime: time.Now().UnixMilli(),
		}
		ctx.FSM = account.MigFSMSyncOut
		p.TrySendTXsync([]*core.TXsync{sync}, ctx.TargetShard)
	}
}

// mvssOnAnnounceDone 迁移完成后清理上下文。
func mvssOnAnnounceDone(addr string) {
	account.DeleteMigCtx(addr)
	account.DeleteMigPendingDelta(addr)
	account.ClearMigAbort(addr)
}
