package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"time"
)

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
	for _, d := range deltas {
		if d == nil {
			continue
		}
		writeSyncLog("send", "delta", d.Address, d.StartN, d.EndN, true, "", len(bc))
	}
	go utils.TcpDial(jointMessage(cTXsyncDelta, bc), leader)
}

func (p *Pbft) handleTXsyncDelta(content []byte) {
	msg := new(SyncDeltaMsg)
	if err := json.Unmarshal(content, msg); err != nil {
		log.Panic(err)
	}
	writeSyncLog("recv", "delta", "", 0, 0, true, "", len(content))
	fmt.Printf("[MVSS-Delta] %s 收到分片 %s 的 TXsyncDelta，条数=%d\n",
		params.Config.ShardID, msg.ShardID, len(msg.TXsyncDeltas))
	ackList := make([]*core.TXsyncDelta, 0, len(msg.TXsyncDeltas))
	for _, d := range msg.TXsyncDeltas {
		if d == nil {
			continue
		}
		if d.Ack {
			if !p.mvssOnDeltaAck(d) {
				mvssAbortDelta(d.Address, "delta ack 校验失败")
				writeSyncLog("ack", "delta", d.Address, d.StartN, d.EndN, false, "delta ack 校验失败", 0)
			} else {
				writeSyncLog("ack", "delta", d.Address, d.StartN, d.EndN, true, "", 0)
			}
			continue
		}
		if !p.mvssApplyDeltaSync(d) {
			mvssAbortDelta(d.Address, "delta 校验失败")
			writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, false, "delta 校验失败", 0)
			continue
		}
		writeSyncLog("apply", "delta", d.Address, d.StartN, d.EndN, true, "", 0)
		ackList = append(ackList, &core.TXsyncDelta{
			Address:      d.Address,
			FromShard:    params.Config.ShardID,
			DeltaBalance: new(big.Int).Set(d.DeltaBalance),
			DeltaNonce:   d.DeltaNonce,
			StartN:       d.StartN,
			EndN:         d.EndN,
			PrevHash:     append([]byte(nil), d.PrevHash...),
			DeltaHash:    append([]byte(nil), d.DeltaHash...),
			RequestTime:  time.Now().UnixMilli(),
			Ack:          true,
		})
	}
	if len(ackList) == 0 {
		return
	}
	targetShardID, ok := params.ShardTable[msg.ShardID]
	if !ok {
		return
	}
	p.TrySendTXsyncDelta(ackList, targetShardID)
}

func (p *Pbft) mvssApplyDeltaSync(d *core.TXsyncDelta) bool {
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
		return false
	}
	if len(ctx.LastDeltaHash) > 0 && !bytes.Equal(ctx.LastDeltaHash, d.PrevHash) {
		return false
	}
	account.SetMigPendingDelta(d.Address, &account.MigPendingDelta{
		DeltaBalance: new(big.Int).Set(d.DeltaBalance),
		DeltaNonce:   d.DeltaNonce,
		StartN:       d.StartN,
		EndN:         d.EndN,
		PrevHash:     append([]byte(nil), d.PrevHash...),
		DeltaHash:    append([]byte(nil), d.DeltaHash...),
		RequestTime:  d.RequestTime,
	})
	ctx.LastDeltaHash = append([]byte(nil), d.DeltaHash...)
	ctx.LastCN = d.EndN
	return true
}

func (p *Pbft) mvssOnDeltaAck(d *core.TXsyncDelta) bool {
	if d == nil {
		return false
	}
	ctx, ok := account.GetMigCtx(d.Address)
	if !ok || ctx == nil {
		return false
	}
	calculated := d.CalcDeltaHash()
	if !bytes.Equal(calculated, d.DeltaHash) {
		return false
	}
	if len(ctx.LastDeltaHash) > 0 && !bytes.Equal(ctx.LastDeltaHash, d.DeltaHash) {
		return false
	}
	ctx.FSM = account.MigFSMActive
	ctx.SyncNeeded = false
	ctx.LastCN = d.EndN
	ctx.PausedTxIDs = make(map[int]bool)
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
