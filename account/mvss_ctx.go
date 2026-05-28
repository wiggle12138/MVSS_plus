package account

import (
	"math/big"
	"sync"
)

// 迁移账户 FSM 状态（论文 Stage 3 交错同步）。
const (
	MigFSMActive = iota
	MigFSMPauseOld
	MigFSMSyncOut
)

// MigAccountCtx 单账户迁移上下文（仅 MVSS+ 使用）。
type MigAccountCtx struct {
	TargetShard   int
	Mig1Time      int64
	LastCN        uint64
	SyncNeeded    bool
	MigNonce      uint64 // 重定向标签盐值
	NextNonce     uint64 // 下一笔待分配 nonce
	OrderList     map[int]int64 // txId -> ClientTimestamp
	ArrivalList   map[int]int64 // txId -> RequestTime（到达时间，判 old/new）
	PausedTxIDs   map[int]bool
	FSM           int
	LastDeltaHash []byte
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

// RegisterOrder 记录交易的逻辑时间戳与到达时间。
func (ctx *MigAccountCtx) RegisterOrder(txID int, clientTS, arrivalTS int64) {
	if ctx == nil {
		return
	}
	if ctx.OrderList == nil {
		ctx.OrderList = make(map[int]int64)
	}
	if ctx.ArrivalList == nil {
		ctx.ArrivalList = make(map[int]int64)
	}
	if ctx.PausedTxIDs == nil {
		ctx.PausedTxIDs = make(map[int]bool)
	}
	ctx.OrderList[txID] = clientTS
	ctx.ArrivalList[txID] = arrivalTS
}

func (ctx *MigAccountCtx) arrivalTime(txID int) int64 {
	if ctx == nil || ctx.ArrivalList == nil {
		return 0
	}
	return ctx.ArrivalList[txID]
}

// DetectInterleave 检测时间戳交错：按 ClientTimestamp 排序，用到达时间判 old/new。
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
	for i := 0; i < len(items); i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].ts > items[j].ts {
				items[i], items[j] = items[j], items[i]
			}
		}
	}
	for i := 0; i+2 < len(items); i++ {
		old1 := !IsTXNew(ctx.Mig1Time, ctx.arrivalTime(items[i].id))
		newMid := IsTXNew(ctx.Mig1Time, ctx.arrivalTime(items[i+1].id))
		old2 := !IsTXNew(ctx.Mig1Time, ctx.arrivalTime(items[i+2].id))
		if old1 && newMid && old2 {
			for k := i + 2; k < len(items); k++ {
				if !IsTXNew(ctx.Mig1Time, ctx.arrivalTime(items[k].id)) {
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

// MigPendingDelta 是 MVSS-Delta 待应用的增量状态。
type MigPendingDelta struct {
	DeltaBalance *big.Int
	DeltaNonce   int64
	StartN       uint64
	EndN         uint64
	PrevHash     []byte
	DeltaHash    []byte
	RequestTime  int64
}

var (
	MigPendingDeltaLock sync.Mutex
	MigPendingDeltaMap  map[string]*MigPendingDelta
)

var (
	MigAbortLock   sync.Mutex
	MigAbortReason map[string]string
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

func SetMigPendingDelta(addr string, d *MigPendingDelta) {
	if d == nil {
		return
	}
	MigPendingDeltaLock.Lock()
	defer MigPendingDeltaLock.Unlock()
	if MigPendingDeltaMap == nil {
		MigPendingDeltaMap = make(map[string]*MigPendingDelta)
	}
	MigPendingDeltaMap[addr] = d
}

func TakeMigPendingDelta(addr string) (*MigPendingDelta, bool) {
	MigPendingDeltaLock.Lock()
	defer MigPendingDeltaLock.Unlock()
	d, ok := MigPendingDeltaMap[addr]
	if ok {
		delete(MigPendingDeltaMap, addr)
	}
	return d, ok
}

func DeleteMigPendingDelta(addr string) {
	MigPendingDeltaLock.Lock()
	defer MigPendingDeltaLock.Unlock()
	if MigPendingDeltaMap != nil {
		delete(MigPendingDeltaMap, addr)
	}
}

func MarkMigAbort(addr, reason string) {
	MigAbortLock.Lock()
	defer MigAbortLock.Unlock()
	if MigAbortReason == nil {
		MigAbortReason = make(map[string]string)
	}
	MigAbortReason[addr] = reason
}

func IsMigAborted(addr string) (bool, string) {
	MigAbortLock.Lock()
	defer MigAbortLock.Unlock()
	if MigAbortReason == nil {
		return false, ""
	}
	reason, ok := MigAbortReason[addr]
	return ok, reason
}

func ClearMigAbort(addr string) {
	MigAbortLock.Lock()
	defer MigAbortLock.Unlock()
	if MigAbortReason != nil {
		delete(MigAbortReason, addr)
	}
}
