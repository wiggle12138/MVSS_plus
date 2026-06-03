package account

import (
	"blockEmulator/params"
	"math/big"
	"sync"
)

// 迁移账户 FSM 状态（论文 Stage 3 交错同步）。
const (
	MigFSMActive = iota
	MigFSMPauseOld
	MigFSMSyncOut
	MigFSMWaitSyncIni // 目标片：等待源片 TXsync（State_ini）
	MigFSMSyncApplied // 目标片：已 apply State_ini，可执行 new
	MigFSMAckSent     // 目标片：new 上链后已回传 Stateʲ
)

// MigAccountCtx 单账户迁移上下文（仅 MVSS+ 使用）。
type MigAccountCtx struct {
	SourceShard   int // 迁出分片编号（回传 ack 用）
	TargetShard   int
	Mig1Time      int64
	LastCN        uint64
	SyncNeeded    bool
	MigNonce      uint64 // 重定向标签盐值
	NextNonce     uint64 // 下一笔待分配 nonce
	OrderList     map[int]int64 // txId -> ClientTimestamp
	ArrivalList   map[int]int64 // txId -> RequestTime（到达时间，判 old/new）
	CommittedTx   map[int]bool  // 本片已 commit 的 txId
	FirstNewTxID  int           // 交错检测到的中间 new 交易
	PausedTxIDs    map[int]bool
	FSM            int
	PendingSyncAck bool // 目标片 new 已执行，待块后回传 ack
	LastDeltaHash  []byte
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
			ctx.FirstNewTxID = items[i+1].id
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

// ShouldBlockNewOnTarget 目标片在 apply State_ini 前不得打包 new。
func (ctx *MigAccountCtx) ShouldBlockNewOnTarget() bool {
	return ctx != nil && ctx.FSM == MigFSMWaitSyncIni
}

// PrefixOldAllCommitted 逻辑序上 FirstNew 之前的 old 均已在本片 commit。
func (ctx *MigAccountCtx) PrefixOldAllCommitted() bool {
	if ctx == nil || ctx.FirstNewTxID <= 0 {
		return false
	}
	firstTS, ok := ctx.OrderList[ctx.FirstNewTxID]
	if !ok {
		return false
	}
	for id, ts := range ctx.OrderList {
		if IsTXNew(ctx.Mig1Time, ctx.arrivalTime(id)) {
			continue
		}
		if ts < firstTS {
			if ctx.CommittedTx == nil || !ctx.CommittedTx[id] {
				return false
			}
		}
	}
	return true
}

// PrefixOldReady 扩展判定：已标记 commit，或已不在交易池（已入块）。
func (ctx *MigAccountCtx) PrefixOldReady(inPool func(txID int) bool) bool {
	if ctx == nil || ctx.FirstNewTxID <= 0 {
		return false
	}
	firstTS, ok := ctx.OrderList[ctx.FirstNewTxID]
	if !ok {
		return false
	}
	for id, ts := range ctx.OrderList {
		if IsTXNew(ctx.Mig1Time, ctx.arrivalTime(id)) {
			continue
		}
		if ts >= firstTS {
			continue
		}
		if ctx.CommittedTx != nil && ctx.CommittedTx[id] {
			continue
		}
		if inPool != nil && inPool(id) {
			return false
		}
	}
	return true
}

// MarkTxCommitted 记录交易已上链（源/目标片通用）。
func MarkTxCommitted(addr string, txID int) {
	MigCtxLock.Lock()
	defer MigCtxLock.Unlock()
	ctx, ok := MigCtx[addr]
	if !ok || ctx == nil {
		return
	}
	if ctx.CommittedTx == nil {
		ctx.CommittedTx = make(map[int]bool)
	}
	ctx.CommittedTx[txID] = true
}

// ShouldSourcePackOutgoing 源片 Announce 后账户已迁出映射，仍须打包的 suffix-old / Paused 交易。
func (ctx *MigAccountCtx) ShouldSourcePackOutgoing(txID int, clientTS, arrivalTS int64) bool {
	if ctx == nil {
		return false
	}
	selfShard := params.ShardTable[params.Config.ShardID]
	if ctx.TargetShard == selfShard {
		return false
	}
	if ctx.IsPaused(txID) {
		return true
	}
	if ctx.IsSuffixOldTx(txID, clientTS, arrivalTS) {
		return true
	}
	// 探针 tx3（与 core.SyncProbeIDBase 对齐）
	const probeBase = 9_000_000_000
	const probeStride = 10
	if txID >= probeBase && (txID-probeBase)%probeStride == 3 {
		return true
	}
	return false
}

// IsSuffixOldTx 是否为交错模式中 FirstNew 之后的 old（如探针 tx3）。
func (ctx *MigAccountCtx) IsSuffixOldTx(txID int, clientTS, arrivalTS int64) bool {
	if ctx == nil || ctx.FirstNewTxID <= 0 {
		return false
	}
	if IsTXNew(ctx.Mig1Time, arrivalTS) {
		return false
	}
	firstTS, ok := ctx.OrderList[ctx.FirstNewTxID]
	if !ok {
		return false
	}
	return txID != ctx.FirstNewTxID && clientTS >= firstTS
}

// ResumeAfterSyncAck 源片收到 Stateʲ 后恢复打包剩余 old。
func ResumeAfterSyncAck(ctx *MigAccountCtx) {
	if ctx == nil {
		return
	}
	ctx.PausedTxIDs = make(map[int]bool)
	ctx.SyncNeeded = false
	ctx.FSM = MigFSMActive
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
