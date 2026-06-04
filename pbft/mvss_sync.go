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
		SourceShard:   params.ShardTable[params.Config.ShardID],
		TargetShard:   toShard,
		Mig1Time:      mig1Time,
		LastCN:        state.Nonce,
		SyncNeeded:    syncNeeded,
		MigNonce:      state.Nonce,
		NextNonce:     nextNonce,
		OrderList:     orderList,
		ArrivalList:   arrivalList,
		CommittedTx:   make(map[int]bool),
		PausedTxIDs:   make(map[int]bool),
		FSM:           account.MigFSMActive,
		LastDeltaHash: nil,
	}
	account.SetMigCtx(addr, ctx)
	// 探针：池内已有 tx1+tx3 时，用逻辑 tx2 槽位提前 DetectInterleave，Pause suffix tx3（早于真实 Phase B）。
	mvssProbeEarlyPauseSuffixOld(addr, mig1Time, ctx)

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

// mvssProbeEarlyPauseSuffixOld 在 NewMap（mvssBuildMigCtx）后立刻暂停 suffix-old（tx3）。
// 用逻辑 ClientTS=200 的 tx2 槽位触发 DetectInterleave，不发送交易；真实 tx2 仍由 Phase B 注入并重定向。
func mvssProbeEarlyPauseSuffixOld(addr string, mig1Time int64, ctx *account.MigAccountCtx) {
	if ctx == nil || !params.IsMVSS() {
		return
	}
	has1, has3 := false, false
	accountIdx := -1
	for id := range ctx.OrderList {
		if !core.IsSyncProbeTxID(id) {
			continue
		}
		accountIdx = (id - core.SyncProbeIDBase) / core.SyncProbeIDStride
		slot := (id - core.SyncProbeIDBase) % core.SyncProbeIDStride
		if slot == 1 {
			has1 = true
		}
		if slot == 3 {
			has3 = true
		}
	}
	if !has1 || !has3 || accountIdx < 0 {
		return
	}
	if ctx.FSM == account.MigFSMSyncOut {
		return
	}
	logicalTx2 := syncProbeID(accountIdx, 2)
	ctx.RegisterOrder(logicalTx2, syncProbeCTS2, mig1Time+1)
	if account.DetectInterleave(ctx) {
		fmt.Printf("[SyncProbe] %s 账户 %s NewMap 后提前 Pause suffix-old（逻辑 tx2=%d），等待 PhaseB 真实 tx2\n",
			params.Config.ShardID, addr, logicalTx2)
		for txID := range ctx.PausedTxIDs {
			fmt.Printf("[SyncProbe]   Paused txId=%d\n", txID)
		}
	}
}

// mvssTriggerProbeSyncAfterMig1 兼容旧开关：等同 mvssProbeEarlyPauseSuffixOld（不再单独触发 sync）。
func mvssTriggerProbeSyncAfterMig1(p *Pbft, addr string, mig1Time int64, ctx *account.MigAccountCtx) {
	mvssProbeEarlyPauseSuffixOld(addr, mig1Time, ctx)
}

// mvssShouldKeepTxOnSourceDuringAnnounce Announce 收池时保留 Stage3 尚未完成的 suffix-old / Paused 交易在源片。
func mvssShouldKeepTxOnSourceDuringAnnounce(addr string, tx *core.Transaction) bool {
	if tx == nil || !params.IsMVSS() {
		return false
	}
	ctx, ok := account.GetMigCtx(addr)
	if !ok || ctx == nil {
		return false
	}
	selfShard := params.ShardTable[params.Config.ShardID]
	if ctx.TargetShard == selfShard {
		return false
	}
	return ctx.ShouldSourcePackOutgoing(tx.Id, tx.OrderTimestamp(), tx.RequestTime)
}

// mvssNormalizeTargetNewTx 目标片收到迁户 new 后按本地交易执行（清除误设的 IsRelay）。
func mvssNormalizeTargetNewTx(tx *core.Transaction, selfShardID int) {
	if tx == nil || !params.IsMVSS() {
		return
	}
	from := hex.EncodeToString(tx.Sender)
	ctx, ok := account.GetMigCtx(from)
	if !ok || ctx.TargetShard != selfShardID {
		return
	}
	if !account.IsTXNew(ctx.Mig1Time, tx.RequestTime) && len(tx.RedirectTag) == 0 {
		return
	}
	if tx.IsRelay {
		tx.IsRelay = false
		if core.IsSyncProbeTxID(tx.Id) {
			fmt.Printf("[SyncProbe][Normalize] shard=%s tx=%d 目标片清除 IsRelay，按本地 new 执行\n",
				params.Config.ShardID, tx.Id)
		}
	}
}

// mvssRedirectNewTx 源分片将迁移后新交易重定向到目标分片。
func (p *Pbft) mvssRedirectNewTx(tx *core.Transaction) bool {
	if tx.RequestTime <= 0 {
		tx.RequestTime = time.Now().UnixMilli()
	}
	probe := isSyncProbeTxID(tx.Id)
	from := hex.EncodeToString(tx.Sender)
	ctx, ok := account.GetMigCtx(from)
	if !ok || !account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
		if probe {
			fmt.Printf("[SyncProbe][Redirect] tx=%d 跳过重定向: 无MigCtx或非new(txRT=%d)\n", tx.Id, tx.RequestTime)
		}
		return false
	}
	// 仅迁出片执行重定向；目标片收到已重定向 tx 应入池执行
	if params.ShardTable[params.Config.ShardID] == ctx.TargetShard {
		if probe {
			fmt.Printf("[SyncProbe][Redirect] tx=%d 目标片本地接收，不在源片重定向\n", tx.Id)
		}
		return false
	}
	tx.Nonce = ctx.NextNonce
	ctx.NextNonce++
	ctx.RegisterOrder(tx.Id, tx.OrderTimestamp(), tx.RequestTime)
	if account.DetectInterleave(ctx) {
		fmt.Printf("[MVSS+] 账户 %s 检测到交错，txId=%d\n", from, tx.Id)
	}

	tx.RedirectTag = core.RedirectTag(from, ctx.Mig1Time, ctx.MigNonce)
	// 仅跨分片转发时标记 relay；目标片 normalize 后会清除，以便本地执行 nonce/扣款。
	tx.IsRelay = true
	target := params.ShardTableInt2Str[ctx.TargetShard]
	if target == "" {
		if probe {
			fmt.Printf("[SyncProbe][Redirect] tx=%d 失败: target shard 为空\n", tx.Id)
		}
		return false
	}
	leader := params.NodeTable[target]["N0"]
	payload, err := json.Marshal(TxFromClient{Txs: []*core.Transaction{tx}})
	if err != nil {
		log.Panic(err)
	}
	if probe {
		fmt.Printf("[SyncProbe][Redirect] tx=%d 源片=%s -> 目标片=%s leader=%s nonce=%d\n",
			tx.Id, params.Config.ShardID, target, leader, tx.Nonce)
	}
	go utils.TcpDial(jointMessage(cClient, payload), leader)
	return true
}

// mvssValidateIncomingTx 校验迁移期交易：目标片要求带 RedirectTag；源片待发重定向的新 tx 可无标签。
func mvssValidateIncomingTx(tx *core.Transaction) bool {
	from := hex.EncodeToString(tx.Sender)
	to := hex.EncodeToString(tx.Recipient)

	// 源片：迁出账户发起、尚未打标的新交易（将由 mvssRedirectNewTx 打标并转发）
	if ctx, ok := account.GetMigCtx(from); ok && account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
		if len(tx.RedirectTag) == 0 {
			return true
		}
		if !core.ValidRedirectTag(tx.RedirectTag, from, ctx.Mig1Time, ctx.MigNonce) {
			return false
		}
		if tx.Nonce <= ctx.LastCN {
			return false
		}
		return true
	}

	// 目标片：迁入账户作为收款方收到重定向交易，必须带有效标签
	ctx, ok := account.GetMigCtx(to)
	if !ok {
		ctx, ok = account.GetMigCtx(from)
	}
	if !ok {
		return true
	}
	if len(tx.RedirectTag) == 0 {
		return false
	}
	if !core.ValidRedirectTag(tx.RedirectTag, to, ctx.Mig1Time, ctx.MigNonce) {
		if !core.ValidRedirectTag(tx.RedirectTag, from, ctx.Mig1Time, ctx.MigNonce) {
			return false
		}
	}
	if tx.Nonce <= ctx.LastCN {
		return false
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

	// 与 propose1/commit1 串行：避免 ApplyMVSSAccountState 与 AddBlock 的 trie.Commit 并发写 Triedb。
	p.sequenceLock.Lock()
	defer p.sequenceLock.Unlock()

	srcShardID, ok := params.ShardTable[msg.ShardID]
	if !ok {
		return
	}

	for _, s := range msg.TXsyncs {
		if s == nil {
			continue
		}
		ctx, hasCtx := account.GetMigCtx(s.Address)
		// 迁出片收到目标片 ack（携带执行 new 后的 Stateʲ）
		if hasCtx && ctx.TargetShard == srcShardID {
			p.mvssApplySyncOnSource(s)
			account.MigCtxLock.Lock()
			account.ResumeAfterSyncAck(ctx)
			account.MigCtxLock.Unlock()
			p.mvssPromoteSuffixOldAfterAck(s.Address, ctx)
			writeSyncLog("ack_recv", "sync", s.Address, s.StartN, s.EndN, true, mvssSyncStateReason(s.StateNew), 0)
			fmt.Printf("[MVSS+] %s 收到目标片 %s 对账户 %s 的 sync 应答\n",
				params.Config.ShardID, msg.ShardID, s.Address)
			continue
		}

		// 迁入片：apply State_ini，待 new 上链后再块后回传 ack
		p.mvssApplySyncInbound(s)
	}
}

// mvssApplySyncInbound 目标片 apply 源片发来的 State_ini。
func (p *Pbft) mvssApplySyncInbound(s *core.TXsync) {
	if s == nil || s.StateNew == nil {
		writeSyncLog("apply", "sync", "", 0, 0, false, "state_new 为空", 0)
		return
	}
	ns := account.DecodeAccountState(s.StateNew.Encode())
	ns.Migrate = -1
	ns.Location = params.ShardTable[params.Config.ShardID]
	p.Node.CurChain.ApplyMVSSAccountState(s.Address, ns)
	account.MigCtxLock.Lock()
	if ctx, ok := account.MigCtx[s.Address]; ok && ctx != nil {
		ctx.FSM = account.MigFSMSyncApplied
	}
	account.MigCtxLock.Unlock()
	p.mvssPromoteMigNewTxsToHead(s.Address)
	writeSyncLog("apply", "sync", s.Address, s.StartN, s.EndN, true, mvssSyncStateReason(ns), 0)
}

// mvssMaybePromoteAfterNewEnqueued 目标片 delta/sync apply 后晚到的 new 入池：对齐链上 nonce 并提至队首。
func (p *Pbft) mvssMaybePromoteAfterNewEnqueued(addr string) {
	if !params.IsMVSS() || addr == "" {
		return
	}
	ctx, ok := account.GetMigCtx(addr)
	if !ok || ctx == nil || ctx.TargetShard != params.ShardTable[params.Config.ShardID] {
		return
	}
	if ctx.FSM < account.MigFSMSyncApplied {
		return
	}
	p.mvssPromoteMigNewTxsToHead(addr)
}

// mvssPromoteMigNewTxsToHead apply State_ini 后将该账户的 new 交易移到队首，并按链上 nonce 重编号。
func (p *Pbft) mvssPromoteMigNewTxsToHead(addr string) {
	if p == nil || p.Node.CurChain == nil {
		return
	}
	ctx, ok := account.GetMigCtx(addr)
	if !ok || ctx == nil {
		return
	}
	nextNonce := ctx.LastCN
	if chainNonce, okN := p.Node.CurChain.GetAccountNonce(addr); okN {
		nextNonce = chainNonce
	}
	pool := p.Node.CurChain.Tx_pool
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	var head, tail []*core.Transaction
	for _, tx := range pool.Queue {
		from := hex.EncodeToString(tx.Sender)
		if from == addr && account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
			tx.Nonce = nextNonce
			nextNonce++
			head = append(head, tx)
		} else {
			tail = append(tail, tx)
		}
	}
	if len(head) == 0 {
		return
	}
	pool.Queue = append(head, tail...)
	account.MigCtxLock.Lock()
	if c, ok := account.MigCtx[addr]; ok && c != nil {
		c.NextNonce = nextNonce
	}
	account.MigCtxLock.Unlock()
	fmt.Printf("[MVSS+] 目标片 %s 账户 %s apply 后将 %d 笔 new 提至队首（nonce 自 %d）\n",
		params.Config.ShardID, addr, len(head), nextNonce-uint64(len(head)))
}

// mvssPromoteSuffixOldAfterAck 源片 ack 后将 suffix-old（如 tx3）提至队首，并按当前状态 nonce 重编号。
func (p *Pbft) mvssPromoteSuffixOldAfterAck(addr string, ctx *account.MigAccountCtx) {
	if p == nil || p.Node.CurChain == nil || ctx == nil || ctx.FirstNewTxID <= 0 {
		if ctx != nil {
			fmt.Printf("[MVSS+] 源片 %s 账户 %s ack 后跳过 promote（FirstNew=%d）\n",
				params.Config.ShardID, addr, ctx.FirstNewTxID)
		}
		return
	}
	hexAddr, err := hex.DecodeString(addr)
	if err != nil {
		return
	}
	st, err := trie.New(trie.TrieID(common.BytesToHash(p.Node.CurChain.CurrentBlock.Header.StateRoot)), p.Node.CurChain.Triedb)
	if err != nil {
		return
	}
	enc := st.Get(hexAddr)
	startNonce := ctx.LastCN
	if enc != nil {
		startNonce = account.DecodeAccountState(enc).Nonce
	}
	nextNonce := startNonce
	pool := p.Node.CurChain.Tx_pool
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	fmt.Printf("[MVSS+] 源片 %s 账户 %s ack 后 promote suffix-old 开始 Queue=%d FirstNew=%d\n",
		params.Config.ShardID, addr, len(pool.Queue), ctx.FirstNewTxID)

	match := func(tx *core.Transaction) bool {
		if tx == nil || hex.EncodeToString(tx.Sender) != addr {
			return false
		}
		return ctx.ShouldSourcePackOutgoing(tx.Id, tx.OrderTimestamp(), tx.RequestTime)
	}

	var head, tail []*core.Transaction
	for _, tx := range pool.Queue {
		if match(tx) {
			tx.Nonce = nextNonce
			nextNonce++
			head = append(head, tx)
		} else {
			tail = append(tail, tx)
		}
	}
	// 侧池中的 suffix-old（Announce 前可能尚未回到主池）
	if params.Config.Not_Lock_Acc_When_Migrating {
		account.Not_Lock_Acc_Lock.Lock()
		side := pool.Not_Locking_TX_Pools[addr]
		var keep []*core.Transaction
		for _, tx := range side {
			if match(tx) {
				tx.Nonce = nextNonce
				nextNonce++
				head = append(head, tx)
			} else {
				keep = append(keep, tx)
			}
		}
		if len(keep) > 0 {
			pool.Not_Locking_TX_Pools[addr] = keep
		} else {
			delete(pool.Not_Locking_TX_Pools, addr)
		}
		account.Not_Lock_Acc_Lock.Unlock()
	}
	if len(head) == 0 {
		fmt.Printf("[MVSS+] 源片 %s 账户 %s ack 后未寻得 suffix-old（Queue=%d FirstNew=%d）\n",
			params.Config.ShardID, addr, len(pool.Queue), ctx.FirstNewTxID)
		return
	}
	pool.Queue = append(head, tail...)
	fmt.Printf("[MVSS+] 源片 %s 账户 %s ack 后将 %d 笔 suffix-old 提至队首（nonce 自 %d）\n",
		params.Config.ShardID, addr, len(head), startNonce)
}

// mvssApplySyncOnSource 源片 apply 目标片回传的 Stateʲ。
func (p *Pbft) mvssApplySyncOnSource(s *core.TXsync) {
	if s == nil || s.StateNew == nil {
		return
	}
	ns := account.DecodeAccountState(s.StateNew.Encode())
	ns.Location = params.ShardTable[params.Config.ShardID]
	p.Node.CurChain.ApplyMVSSAccountState(s.Address, ns)
}

func (p *Pbft) mvssSendSyncForAddr(addr string, ctx *account.MigAccountCtx, st *trie.Trie) {
	if ctx == nil || ctx.FSM == account.MigFSMSyncOut {
		return
	}
	hexAddr, _ := hex.DecodeString(addr)
	enc := st.Get(hexAddr)
	if enc == nil {
		fmt.Printf("[MVSS+] 账户 %s 即时同步跳过: 状态树无该账户\n", addr)
		return
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
			return
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
		// 源片：LastDeltaHash 在收到目标片 ack 后更新，避免重发时 PrevHash 与目标链不一致
		account.MigCtxLock.Lock()
		ctx.FSM = account.MigFSMSyncOut
		account.MigCtxLock.Unlock()
		enqueueOutboundStateIniDelta(p, delta, ctx.TargetShard)
		return
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

// mvssOnBlockCommitted 块提交后：标记已提交、按论文序发 sync/ack（全量或 Delta 分叉）。
func (p *Pbft) mvssOnBlockCommitted(block *core.Block, st *trie.Trie) {
	if !params.IsMVSS() {
		return
	}
	selfShard := params.ShardTable[params.Config.ShardID]

	// TXmig2 上链后 flush 早到的 delta（账户此时才写入状态树）
	if params.IsMVSSDelta() {
		for _, m2 := range block.TXmig2s {
			if m2 != nil && m2.Address != "" {
				p.mvssFlushPendingTargetDeltas(m2.Address)
			}
		}
	}

	for _, tx := range block.Transactions {
		from := hex.EncodeToString(tx.Sender)
		if ctx, ok := account.GetMigCtx(from); ok {
			account.MarkTxCommitted(from, tx.Id)
			account.DetectInterleave(ctx)
		}
	}

	account.MigCtxLock.RLock()
	snapshot := make(map[string]*account.MigAccountCtx, len(account.MigCtx))
	for addr, ctx := range account.MigCtx {
		if ctx != nil {
			snapshot[addr] = ctx
		}
	}
	account.MigCtxLock.RUnlock()

	for addr, ctx := range snapshot {
		if ctx == nil {
			continue
		}
		if aborted, reason := account.IsMigAborted(addr); aborted {
			fmt.Printf("[MVSS+] 账户 %s 已中止，跳过块后同步: %s\n", addr, reason)
			continue
		}
		// 目标片：new 上链后回传 Stateʲ / Delta ack
		if ctx.TargetShard == selfShard {
			if ctx.FSM == account.MigFSMSyncApplied && ctx.PendingSyncAck {
				fmt.Printf("[MVSS+] 目标片 %s 账户 %s 块 %d 触发 sync ack (new 已执行)\n",
					params.Config.ShardID, addr, block.Header.Number)
				if params.IsMVSSDelta() {
					p.mvssSendDeltaAck(addr, ctx, st, block)
				} else {
					p.mvssSendSyncAck(addr, ctx, st)
				}
			} else if ctx.FSM == account.MigFSMSyncApplied {
				fmt.Printf("[MVSS+] 目标片 %s 账户 %s 块 %d SyncApplied 但尚无 new 上链，等待打包\n",
					params.Config.ShardID, addr, block.Header.Number)
			}
			continue
		}
		// 源片：prefix old 已提交后发 State_ini
		if ctx.TargetShard != selfShard && ctx.SyncNeeded && ctx.FSM == account.MigFSMPauseOld &&
			p.mvssPrefixOldReady(addr, ctx) {
			fmt.Printf("[MVSS+] 源片 %s 账户 %s prefix old 就绪，发送 State_ini sync\n",
				params.Config.ShardID, addr)
			p.mvssSendSyncForAddr(addr, ctx, st)
		} else if ctx.TargetShard != selfShard && ctx.SyncNeeded && ctx.FSM == account.MigFSMPauseOld {
			fmt.Printf("[MVSS+] 源片 %s 账户 %s 等待 prefix old 提交 (FirstNew=%d Committed=%v)\n",
				params.Config.ShardID, addr, ctx.FirstNewTxID, ctx.CommittedTx)
		}
	}

	// 源片：同块内多账户 prefix-old 就绪的 State_ini delta 合并为一条 SyncDeltaMsg 发送
	if params.IsMVSSDelta() {
		p.mvssFlushAllOutboundStateIniAtBlockEnd()
	}
}

func (p *Pbft) mvssPrefixOldReady(addr string, ctx *account.MigAccountCtx) bool {
	if ctx == nil {
		return false
	}
	if ctx.PrefixOldAllCommitted() {
		return true
	}
	p.Node.CurChain.Tx_pool.Lock.Lock()
	inPool := func(txID int) bool {
		return p.Node.CurChain.Tx_pool.ContainsTxID(txID)
	}
	ready := ctx.PrefixOldReady(inPool)
	p.Node.CurChain.Tx_pool.Lock.Unlock()
	return ready
}

func mvssBlockHasMigNewTx(block *core.Block, addr string, ctx *account.MigAccountCtx) bool {
	for _, tx := range block.Transactions {
		if hex.EncodeToString(tx.Sender) != addr {
			continue
		}
		if account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
			return true
		}
	}
	return false
}

// mvssSendSyncAck 目标片在 new 提交后回传执行后状态（非 echo）。
func (p *Pbft) mvssSendSyncAck(addr string, ctx *account.MigAccountCtx, st *trie.Trie) {
	if ctx == nil || ctx.FSM != account.MigFSMSyncApplied {
		return
	}
	hexAddr, err := hex.DecodeString(addr)
	if err != nil {
		return
	}
	enc := st.Get(hexAddr)
	if enc == nil {
		fmt.Printf("[MVSS+] 目标片 sync ack 跳过: 账户 %s 不在状态树\n", addr)
		return
	}
	stateNew := account.DecodeAccountState(enc)
	mpNew := &core.ProofDB{}
	_ = st.Prove(hexAddr, 0, mpNew)
	sync := &core.TXsync{
		Address:     addr,
		FromShard:   params.Config.ShardID,
		StateNew:    stateNew,
		MPNew:       mpNew,
		StartN:      ctx.LastCN,
		EndN:        stateNew.Nonce,
		RequestTime: time.Now().UnixMilli(),
	}
	account.MigCtxLock.Lock()
	ctx.FSM = account.MigFSMAckSent
	ctx.PendingSyncAck = false
	account.MigCtxLock.Unlock()
	p.TrySendTXsync([]*core.TXsync{sync}, ctx.SourceShard)
	writeSyncLog("ack_send", "sync", addr, sync.StartN, sync.EndN, true, mvssSyncStateReason(stateNew), 0)
}

func mvssSyncStateReason(st *account.AccountState) string {
	if st == nil {
		return ""
	}
	return fmt.Sprintf("nonce=%d", st.Nonce)
}

// mvssOnAnnounceDone 迁移完成后清理上下文；Stage3 同步未完成时保留 MigCtx。
func mvssOnAnnounceDone(addr string) {
	if ctx, ok := account.GetMigCtx(addr); ok && ctx != nil && params.IsMVSS() {
		switch ctx.FSM {
		case account.MigFSMPauseOld, account.MigFSMSyncOut,
			account.MigFSMWaitSyncIni, account.MigFSMSyncApplied:
			fmt.Printf("[MVSS+] 账户 %s Stage3 进行中(FSM=%d)，Announce 后暂缓清理 MigCtx\n", addr, ctx.FSM)
			return
		}
	}
	account.DeleteMigCtx(addr)
	account.DeleteMigPendingDelta(addr)
	account.ClearMigAbort(addr)
}
