package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/trie"
)

// 目标片 handleMig2 前到达的 delta 暂存于此，MigCtx 就绪后 flush。
var (
	pendingTargetDeltaMu sync.Mutex
	pendingTargetDelta   = map[string][]*core.TXsyncDelta{}

	// 源片 State_ini delta 出站聚合：按目标分片编号索引，Ack 仍单笔直发。
	outboundStateIniMu     sync.Mutex
	outboundStateIniByTarget = map[int][]*core.TXsyncDelta{}
	outboundStateIniStart  = map[int]time.Time{}
	outboundStateIniTimer  = map[int]*time.Timer{}
)

func deltaAggregateWindow() time.Duration {
	if params.Config == nil || params.Config.DeltaAggregateWindowMs <= 0 {
		return 0
	}
	return time.Duration(params.Config.DeltaAggregateWindowMs) * time.Millisecond
}

// enqueueOutboundStateIniDelta 将 prefix-old 就绪的 State_ini delta 入队，由块末或窗口 flush。
func enqueueOutboundStateIniDelta(p *Pbft, d *core.TXsyncDelta, targetShardID int) {
	if p == nil || d == nil || d.Ack {
		return
	}
	outboundStateIniMu.Lock()
	for _, ex := range outboundStateIniByTarget[targetShardID] {
		if ex != nil && ex.Address == d.Address && ex.StartN == d.StartN && ex.EndN == d.EndN {
			outboundStateIniMu.Unlock()
			return
		}
	}
	firstInBatch := len(outboundStateIniByTarget[targetShardID]) == 0
	if firstInBatch {
		outboundStateIniStart[targetShardID] = time.Now()
	}
	outboundStateIniByTarget[targetShardID] = append(
		outboundStateIniByTarget[targetShardID], cloneTXsyncDelta(d))
	outboundStateIniMu.Unlock()

	window := deltaAggregateWindow()
	if window > 0 && firstInBatch {
		tid := targetShardID
		timer := time.AfterFunc(window, func() {
			p.mvssFlushOutboundStateIniDeltas(tid, true)
		})
		outboundStateIniMu.Lock()
		if old := outboundStateIniTimer[tid]; old != nil {
			old.Stop()
		}
		outboundStateIniTimer[tid] = timer
		outboundStateIniMu.Unlock()
	}
}

func stopOutboundStateIniTimer(targetShardID int) {
	if t := outboundStateIniTimer[targetShardID]; t != nil {
		t.Stop()
	}
	delete(outboundStateIniTimer, targetShardID)
}

// mvssFlushOutboundStateIniDeltas 发送并清空指定目标片的出站 State_ini delta 批次。
func (p *Pbft) mvssFlushOutboundStateIniDeltas(targetShardID int, force bool) {
	outboundStateIniMu.Lock()
	batch := outboundStateIniByTarget[targetShardID]
	if len(batch) == 0 {
		outboundStateIniMu.Unlock()
		return
	}
	if !force {
		if window := deltaAggregateWindow(); window > 0 {
			if start, ok := outboundStateIniStart[targetShardID]; ok && time.Since(start) < window {
				outboundStateIniMu.Unlock()
				return
			}
		}
	}
	list := append([]*core.TXsyncDelta(nil), batch...)
	delete(outboundStateIniByTarget, targetShardID)
	delete(outboundStateIniStart, targetShardID)
	stopOutboundStateIniTimer(targetShardID)
	outboundStateIniMu.Unlock()

	fmt.Printf("[MVSS-Delta] %s 聚合 flush State_ini delta → 目标片 %d，条数=%d\n",
		params.Config.ShardID, targetShardID, len(list))
	p.TrySendTXsyncDelta(list, targetShardID)
}

// mvssFlushAllOutboundStateIniAtBlockEnd 块提交末尾 flush；window=0 时 force 合并同块多账户。
func (p *Pbft) mvssFlushAllOutboundStateIniAtBlockEnd() {
	outboundStateIniMu.Lock()
	targets := make([]int, 0, len(outboundStateIniByTarget))
	for tid := range outboundStateIniByTarget {
		targets = append(targets, tid)
	}
	outboundStateIniMu.Unlock()
	force := deltaAggregateWindow() == 0
	for _, tid := range targets {
		p.mvssFlushOutboundStateIniDeltas(tid, force)
	}
}

func cloneTXsyncDelta(d *core.TXsyncDelta) *core.TXsyncDelta {
	if d == nil {
		return nil
	}
	out := *d
	if d.DeltaBalance != nil {
		out.DeltaBalance = new(big.Int).Set(d.DeltaBalance)
	}
	if d.PrevHash != nil {
		out.PrevHash = append([]byte(nil), d.PrevHash...)
	}
	if d.DeltaHash != nil {
		out.DeltaHash = append([]byte(nil), d.DeltaHash...)
	}
	return &out
}

func enqueuePendingTargetDelta(d *core.TXsyncDelta) {
	if d == nil || d.Ack {
		return
	}
	pendingTargetDeltaMu.Lock()
	defer pendingTargetDeltaMu.Unlock()
	for _, existing := range pendingTargetDelta[d.Address] {
		if existing != nil && existing.StartN == d.StartN && existing.EndN == d.EndN {
			return
		}
	}
	pendingTargetDelta[d.Address] = append(pendingTargetDelta[d.Address], cloneTXsyncDelta(d))
	fmt.Printf("[MVSS-Delta] 目标片 %s 暂存 delta 账户 %s [%d,%d)（MigCtx 未就绪）\n",
		params.Config.ShardID, d.Address, d.StartN, d.EndN)
}

func dequeuePendingTargetDeltas(addr string) []*core.TXsyncDelta {
	pendingTargetDeltaMu.Lock()
	defer pendingTargetDeltaMu.Unlock()
	list := pendingTargetDelta[addr]
	delete(pendingTargetDelta, addr)
	return list
}

// TrySendTXsyncDelta 向目标分片发送 TXsyncDelta。
func (p *Pbft) TrySendTXsyncDelta(deltas []*core.TXsyncDelta, targetShardID int) {
	if len(deltas) == 0 {
		return
	}
	target := params.ShardTableInt2Str[targetShardID]
	if target == "" {
		return
	}
	leader := params.NodeTable[target]["N0"]
	msg := SyncDeltaMsg{TXsyncDeltas: deltas, ShardID: params.Config.ShardID}
	bc, err := json.Marshal(msg)
	if err != nil {
		log.Panic(err)
	}
	isAck := false
	for _, d := range deltas {
		if d != nil && d.Ack {
			isAck = true
			break
		}
	}
	if len(deltas) > 1 && !isAck {
		writeSyncLog("send", "delta", "", 0, 0, true, fmt.Sprintf("batch=%d", len(deltas)), len(bc))
	} else {
		for _, d := range deltas {
			if d == nil {
				continue
			}
			ev := "send"
			if d.Ack {
				ev = "ack_send"
			}
			writeSyncLog(ev, "delta", d.Address, d.StartN, d.EndN, true, "", len(bc))
		}
	}
	go utils.TcpDial(jointMessage(cTXsyncDelta, bc), leader)
}

func (p *Pbft) handleTXsyncDelta(content []byte) {
	msg := new(SyncDeltaMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	recvReason := ""
	if len(msg.TXsyncDeltas) > 1 {
		recvReason = fmt.Sprintf("batch=%d", len(msg.TXsyncDeltas))
	}
	writeSyncLog("recv", "delta", "", 0, 0, true, recvReason, len(content))
	fmt.Printf("[MVSS-Delta] %s 收到分片 %s 的 TXsyncDelta，条数=%d\n",
		params.Config.ShardID, msg.ShardID, len(msg.TXsyncDeltas))

	p.sequenceLock.Lock()
	defer p.sequenceLock.Unlock()
	fmt.Printf("[MVSS-Delta] %s 已获取 sequenceLock，开始处理 delta\n", params.Config.ShardID)

	srcShardID, ok := params.ShardTable[msg.ShardID]
	if !ok {
		return
	}
	selfShard := params.ShardTable[params.Config.ShardID]

	for _, d := range msg.TXsyncDeltas {
		if d == nil {
			continue
		}
		ctx, hasCtx := account.GetMigCtx(d.Address)

		if d.Ack {
			if hasCtx && ctx != nil && ctx.TargetShard == srcShardID {
				if p.mvssOnDeltaAck(d) {
					account.MigCtxLock.Lock()
					account.ResumeAfterSyncAck(ctx)
					account.MigCtxLock.Unlock()
					p.mvssPromoteSuffixOldAfterAck(d.Address, ctx)
					writeSyncLog("ack_recv", "delta", d.Address, d.StartN, d.EndN, true,
						mvssDeltaReason(d.EndN), 0)
					fmt.Printf("[MVSS-Delta] %s 收到目标片 %s 对账户 %s 的 delta ack\n",
						params.Config.ShardID, msg.ShardID, d.Address)
				} else {
					mvssAbortDelta(d.Address, "delta ack 校验失败")
					writeSyncLog("ack_recv", "delta", d.Address, d.StartN, d.EndN, false, "delta ack 校验失败", 0)
				}
			}
			continue
		}

		if hasCtx && ctx != nil && ctx.TargetShard == selfShard {
			if !p.mvssApplyDeltaInbound(d) {
				mvssAbortDelta(d.Address, "delta 校验失败")
				writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, false, "delta 校验失败", 0)
			}
			continue
		}

		// MigCtx 尚未建立（delta 早于 TXmig2 块），暂存待 flush
		if params.IsMVSSDelta() {
			enqueuePendingTargetDelta(d)
			fmt.Printf("[MVSS-Delta] %s 暂存 delta 账户 %s（尚无 MigCtx）\n",
				params.Config.ShardID, d.Address)
		}
	}
}

// mvssFlushPendingTargetDeltas 应用早到的 delta；调用方须已持有 sequenceLock（如 commit1 内）。
func (p *Pbft) mvssFlushPendingTargetDeltas(addr string) {
	if !params.IsMVSSDelta() {
		return
	}
	list := dequeuePendingTargetDeltas(addr)
	for _, d := range list {
		if d == nil {
			continue
		}
		if !p.mvssApplyDeltaInbound(d) {
			mvssAbortDelta(d.Address, "delta 校验失败")
			writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, false, "delta 校验失败", 0)
		}
	}
}

// mvssApplyDeltaInbound 目标片校验并即时 apply 源片 delta。
func (p *Pbft) mvssApplyDeltaInbound(d *core.TXsyncDelta) bool {
	if d == nil || d.DeltaBalance == nil {
		return false
	}
	if aborted, _ := account.IsMigAborted(d.Address); aborted {
		return false
	}
	ctx, ok := account.GetMigCtx(d.Address)
	if !ok || ctx == nil {
		return false
	}
	if d.DeltaNonce < 0 || d.EndN < d.StartN {
		return false
	}
	if uint64(d.DeltaNonce) != d.EndN-d.StartN {
		return false
	}
	calculated := d.CalcDeltaHash()
	if !bytes.Equal(calculated, d.DeltaHash) {
		fmt.Printf("[MVSS-Delta] 账户 %s delta 哈希不匹配\n", d.Address)
		return false
	}
	nonce, okN := p.Node.CurChain.GetAccountNonce(d.Address)
	if okN && nonce >= d.EndN {
		// 源片重传的同一段 delta，目标片已 apply 过
		account.MigCtxLock.Lock()
		if ctx.FSM == account.MigFSMWaitSyncIni {
			ctx.FSM = account.MigFSMSyncApplied
		}
		if ctx.LastCN < d.EndN {
			ctx.LastCN = d.EndN
		}
		if len(d.DeltaHash) > 0 {
			ctx.LastDeltaHash = append([]byte(nil), d.DeltaHash...)
		}
		account.MigCtxLock.Unlock()
		writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, true, "duplicate", 0)
		fmt.Printf("[MVSS-Delta] 目标片 %s 账户 %s 忽略重复 delta [%d,%d) nonce=%d\n",
			params.Config.ShardID, d.Address, d.StartN, d.EndN, nonce)
		return true
	}
	if !okN {
		// TXmig2 块尚未 commit，账户未入状态树，暂存待 mig2 块后再 apply
		enqueuePendingTargetDelta(d)
		return true
	}
	if nonce < d.StartN {
		// 目标片账户已存在但 nonce 仍落后（常见于 TXmig2 尚未把 LastCN 写入状态树），先暂存等待后续块提交后重试。
		fmt.Printf("[MVSS-Delta] 账户 %s 暂缓 apply delta: StartN=%d 链上 nonce=%d\n", d.Address, d.StartN, nonce)
		enqueuePendingTargetDelta(d)
		return true
	}
	if nonce != d.StartN {
		fmt.Printf("[MVSS-Delta] 账户 %s StartN=%d 与链上 nonce=%d 不一致\n", d.Address, d.StartN, nonce)
		return false
	}
	// 目标片已有链时校验 PrevHash；首条 delta 允许源片 PrevHash 非空（源片重试场景）
	if len(ctx.LastDeltaHash) > 0 && !bytes.Equal(ctx.LastDeltaHash, d.PrevHash) {
		fmt.Printf("[MVSS-Delta] 账户 %s PrevHash 链断裂\n", d.Address)
		return false
	}
	if !p.Node.CurChain.ApplyMVSSAccountDelta(d.Address, d.DeltaBalance, d.DeltaNonce) {
		writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, false, "delta 落盘失败", 0)
		return false
	}
	account.MigCtxLock.Lock()
	ctx.LastDeltaHash = append([]byte(nil), d.DeltaHash...)
	ctx.LastCN = d.EndN
	ctx.FSM = account.MigFSMSyncApplied
	account.MigCtxLock.Unlock()
	p.mvssPromoteMigNewTxsToHead(d.Address)
	writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, true, mvssDeltaReason(d.EndN), 0)
	fmt.Printf("[MVSS-Delta] 目标片 %s 账户 %s apply delta [%d,%d)\n",
		params.Config.ShardID, d.Address, d.StartN, d.EndN)
	return true
}

// mvssSendDeltaAck 目标片 new 上链后回传执行后的 delta ack。
func (p *Pbft) mvssSendDeltaAck(addr string, ctx *account.MigAccountCtx, st *trie.Trie, block *core.Block) {
	if ctx == nil || ctx.FSM != account.MigFSMSyncApplied {
		return
	}
	stateNew, ok := p.Node.CurChain.GetAccountState(addr)
	if !ok || stateNew == nil {
		fmt.Printf("[MVSS-Delta] 目标片 ack 跳过: 账户 %s 不在状态树\n", addr)
		return
	}
	startN := ctx.LastCN
	endN := stateNew.Nonce
	if endN <= startN && block != nil && mvssBlockHasMigNewTx(block, addr, ctx) && endN > 0 {
		// new 已在本块提交，但 LastCN 可能因时序提前推进；按“至少 1 笔 new”回传 ack，避免 Stage3 卡死。
		corrected := endN - 1
		fmt.Printf("[MVSS-Delta] 目标片 ack 校正: 账户 %s start=%d end=%d -> start=%d\n",
			addr, startN, endN, corrected)
		startN = corrected
	}
	if endN <= startN {
		fmt.Printf("[MVSS-Delta] 目标片 ack 跳过: 账户 %s nonce 未推进 start=%d end=%d\n", addr, startN, endN)
		return
	}
	balanceBeforeNew := new(big.Int).Set(stateNew.Balance)
	for _, tx := range block.Transactions {
		if hex.EncodeToString(tx.Sender) != addr {
			continue
		}
		if !account.IsTXNew(ctx.Mig1Time, tx.RequestTime) {
			continue
		}
		if tx.Value != nil {
			balanceBeforeNew.Add(balanceBeforeNew, tx.Value)
		}
		break
	}
	deltaBalance := new(big.Int).Sub(stateNew.Balance, balanceBeforeNew)
	deltaNonce := int64(endN - startN)
	delta := &core.TXsyncDelta{
		Address:      addr,
		FromShard:    params.Config.ShardID,
		DeltaBalance: deltaBalance,
		DeltaNonce:   deltaNonce,
		StartN:       startN,
		EndN:         endN,
		PrevHash:     append([]byte(nil), ctx.LastDeltaHash...),
		RequestTime:  time.Now().UnixMilli(),
		Ack:          true,
	}
	delta.DeltaHash = delta.CalcDeltaHash()

	account.MigCtxLock.Lock()
	ctx.FSM = account.MigFSMAckSent
	ctx.PendingSyncAck = false
	ctx.LastDeltaHash = append([]byte(nil), delta.DeltaHash...)
	ctx.LastCN = endN
	account.MigCtxLock.Unlock()

	p.TrySendTXsyncDelta([]*core.TXsyncDelta{delta}, ctx.SourceShard)
}

// mvssOnDeltaAck 源片校验并 apply 目标片 ack delta。
func (p *Pbft) mvssOnDeltaAck(d *core.TXsyncDelta) bool {
	if d == nil || !d.Ack || d.DeltaBalance == nil {
		return false
	}
	ctx, ok := account.GetMigCtx(d.Address)
	if !ok || ctx == nil {
		return false
	}
	if d.DeltaNonce < 0 || d.EndN < d.StartN {
		return false
	}
	if uint64(d.DeltaNonce) != d.EndN-d.StartN {
		return false
	}
	calculated := d.CalcDeltaHash()
	if !bytes.Equal(calculated, d.DeltaHash) {
		return false
	}
	if !p.Node.CurChain.ApplyMVSSAccountDelta(d.Address, d.DeltaBalance, d.DeltaNonce) {
		return false
	}
	account.MigCtxLock.Lock()
	ctx.LastDeltaHash = append([]byte(nil), d.DeltaHash...)
	ctx.LastCN = d.EndN
	account.MigCtxLock.Unlock()
	return true
}

func mvssAbortDelta(addr, reason string) {
	account.MarkMigAbort(addr, reason)
	account.DeleteMigPendingDelta(addr)
	if ctx, ok := account.GetMigCtx(addr); ok && ctx != nil {
		ctx.SyncNeeded = false
		ctx.FSM = account.MigFSMActive
		ctx.PausedTxIDs = make(map[int]bool)
	}
	fmt.Printf("[MVSS-Delta] 账户 %s 同步失败，已中止迁移: %s\n", addr, reason)
	writeSyncLog("abort", "delta", addr, 0, 0, false, reason, 0)
}

func mvssDeltaReason(endN uint64) string {
	return fmt.Sprintf("nonce=%d", endN)
}
