package account

import "sync"

// 迁移账户 FSM 状态（论文 Stage 3 交错同步）。
const (
	MigFSMActive = iota
	MigFSMPauseOld
	MigFSMSyncOut
)

// MigAccountCtx 单账户迁移上下文（仅 MVSS+ 使用）。
type MigAccountCtx struct {
	TargetShard int
	Mig1Time    int64
	LastCN      uint64
	SyncNeeded  bool
	MigNonce    uint64 // 重定向标签盐值
	NextNonce   uint64 // 下一笔待分配 nonce
	OrderList   map[int]int64
	PausedTxIDs map[int]bool
	FSM         int
}

var (
	MigCtxLock sync.RWMutex
	MigCtx     map[string]*MigAccountCtx
)

func InitMigCtx() {
	MigCtxLock.Lock()
	defer MigCtxLock.Unlock()
	if MigCtx == nil {
		MigCtx = make(map[string]*MigAccountCtx)
	}
}

func GetMigCtx(addr string) (*MigAccountCtx, bool) {
	MigCtxLock.RLock()
	defer MigCtxLock.RUnlock()
	ctx, ok := MigCtx[addr]
	return ctx, ok
}

func SetMigCtx(addr string, ctx *MigAccountCtx) {
	MigCtxLock.Lock()
	defer MigCtxLock.Unlock()
	if MigCtx == nil {
		MigCtx = make(map[string]*MigAccountCtx)
	}
	MigCtx[addr] = ctx
}

func DeleteMigCtx(addr string) {
	MigCtxLock.Lock()
	defer MigCtxLock.Unlock()
	delete(MigCtx, addr)
}

// IsTXNew 判定是否为迁移后到达的新交易（RequestTime 晚于 TXmig1 时刻）。
func IsTXNew(mig1Time, requestTime int64) bool {
	return mig1Time > 0 && requestTime > mig1Time
}

// DetectInterleave 检测时间戳交错：存在 old-new-old 模式则返回 true。
func DetectInterleave(ctx *MigAccountCtx) bool {
	if ctx == nil || len(ctx.OrderList) < 3 {
		return false
	}
	type item struct {
		id int
		ts int64
	}
	items := make([]item, 0, len(ctx.OrderList))
	for id, ts := range ctx.OrderList {
		items = append(items, item{id, ts})
	}
	// 按时间戳排序后检查 old-new-old
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].ts > items[j].ts {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	for i := 0; i+2 < len(items); i++ {
		old1 := !IsTXNew(ctx.Mig1Time, items[i].ts)
		newMid := IsTXNew(ctx.Mig1Time, items[i+1].ts)
		old2 := !IsTXNew(ctx.Mig1Time, items[i+2].ts)
		if old1 && newMid && old2 {
			// 暂停尚未提交、时间戳更大的 old 交易
			for k := i + 2; k < len(items); k++ {
				if !IsTXNew(ctx.Mig1Time, items[k].ts) {
					ctx.PausedTxIDs[items[k].id] = true
				}
			}
			ctx.SyncNeeded = true
			ctx.FSM = MigFSMPauseOld
			return true
		}
	}
	return false
}

func (ctx *MigAccountCtx) IsPaused(txID int) bool {
	if ctx == nil || ctx.PausedTxIDs == nil {
		return false
	}
	return ctx.PausedTxIDs[txID]
}

// 目标分片待应用的同步状态（由 TXsync 写入，出块时合并）。
var (
	MigPendingStateLock sync.Mutex
	MigPendingState     map[string]*AccountState
)

func SetMigPendingState(addr string, st *AccountState) {
	MigPendingStateLock.Lock()
	defer MigPendingStateLock.Unlock()
	if MigPendingState == nil {
		MigPendingState = make(map[string]*AccountState)
	}
	MigPendingState[addr] = st
}

func TakeMigPendingState(addr string) (*AccountState, bool) {
	MigPendingStateLock.Lock()
	defer MigPendingStateLock.Unlock()
	st, ok := MigPendingState[addr]
	if ok {
		delete(MigPendingState, addr)
	}
	return st, ok
}
