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
	"strings"
	"sync"
	"time"
)

var (
	syncProbePendingMu  sync.Mutex
	syncProbePending    []syncProbeTarget
	syncProbePhaseBDone bool
)

const (
	syncProbeIDBase      = core.SyncProbeIDBase
	syncProbeIDStride    = core.SyncProbeIDStride // 每账户占 10 个 Id：+1/+2/+3 为 tx1/tx2/tx3
	syncProbeCTS1        = 100
	syncProbeCTS2        = 200
	syncProbeCTS3           = 300
	syncProbeDefaultMaxN    = 3
	syncProbeOldRequestTime = int64(1) // Phase A 固定为“旧交易”到达时间，避免晚于 Mig1Time
)

// syncProbeTarget 单个迁出账户的探针目标。
type syncProbeTarget struct {
	addr     string
	srcShard string // 如 S0
	dstShard string // 目标片，如 S1；PhaseB 应在该分片发出 Announce 后触发
}

func syncProbeEnabled() bool {
	if params.Config == nil || !params.Config.EnableSyncProbe {
		return false
	}
	return params.IsMVSS() || params.IsMVSSDelta()
}

func syncProbeMaxAccounts() int {
	n := params.Config.SyncProbeMaxAccounts
	if n <= 0 {
		n = syncProbeDefaultMaxN
	}
	return n
}

func syncProbeDelay() time.Duration {
	ms := params.Config.SyncProbeDelayMs
	if ms <= 0 {
		ms = params.Config.Block_interval * 2 * 1000
	}
	if ms < 500 {
		ms = 500
	}
	return time.Duration(ms) * time.Millisecond
}

func syncProbeSettle() time.Duration {
	ms := params.Config.SyncProbeSettleMs
	if ms <= 0 {
		ms = 800
	}
	return time.Duration(ms) * time.Millisecond
}

// syncProbePhaseBDelay NewMap 后等待 TXmig1 上链再发 Phase B（真实 new tx）。
func syncProbePhaseBDelay() time.Duration {
	ms := params.Config.SyncProbePhaseBDelayMs
	if ms <= 0 {
		ms = params.Config.Block_interval * 3 * 1000
	}
	if ms < 2000 {
		ms = 2000
	}
	return time.Duration(ms) * time.Millisecond
}

func syncProbeID(accountIdx, slot int) int {
	return syncProbeIDBase + accountIdx*syncProbeIDStride + slot
}

// isSyncProbeTxID 判断是否为探针交易 ID（仅用于诊断日志）。
func isSyncProbeTxID(id int) bool {
	return core.IsSyncProbeTxID(id)
}

// pickSyncProbeTargets 从迁出列表中选取最多 N 个账户（源片 handleNewMap 前注入）。
func pickSyncProbeTargets(newAddrs []string, newMap map[string]int) []syncProbeTarget {
	if !syncProbeEnabled() || len(newAddrs) == 0 {
		return nil
	}
	maxN := syncProbeMaxAccounts()
	forced := strings.TrimSpace(params.Config.SyncProbeAccount)
	out := make([]syncProbeTarget, 0, maxN)

	tryAdd := func(addr string) {
		if len(out) >= maxN {
			return
		}
		oldShard, okOld := account.Account2Shard[addr]
		newShard, okNew := newMap[addr]
		if !okNew || !okOld || oldShard == newShard {
			return
		}
		src := params.ShardTableInt2Str[oldShard]
		if src == "" {
			return
		}
		for _, t := range out {
			if t.addr == addr {
				return
			}
		}
		dst := params.ShardTableInt2Str[newShard]
		out = append(out, syncProbeTarget{addr: addr, srcShard: src, dstShard: dst})
	}

	if forced != "" {
		tryAdd(forced)
		return out
	}
	for _, addr := range newAddrs {
		tryAdd(addr)
		if len(out) >= maxN {
			break
		}
	}
	return out
}

// pickSyncProbeRecipient 在指定分片内选与 sender 不同、且本轮不迁出的收款方（避免 NewMap 后映射变化导致不入块/不写日志）。
func pickSyncProbeRecipient(senderHex string, shardStr string, exclude map[string]bool) string {
	srcSID, ok := params.ShardTable[shardStr]
	if !ok {
		return ""
	}
	try := func(a string) bool {
		if a == senderHex || (exclude != nil && exclude[a]) {
			return false
		}
		account.Account2ShardLock.Lock()
		sid := account.Account2Shard[a]
		account.Account2ShardLock.Unlock()
		return sid == srcSID
	}
	for _, a := range params.Init_addrs {
		if try(a) {
			return a
		}
	}
	account.Account2ShardLock.Lock()
	defer account.Account2ShardLock.Unlock()
	for a, sid := range account.Account2Shard {
		if a == senderHex || sid != srcSID || (exclude != nil && exclude[a]) {
			continue
		}
		return a
	}
	return ""
}

func buildSyncProbeTx(senderHex, recipientHex string, id int, clientTS, requestTS int64) *core.Transaction {
	sender, err := hex.DecodeString(senderHex)
	if err != nil {
		log.Panic(err)
	}
	recipient, err := hex.DecodeString(recipientHex)
	if err != nil {
		log.Panic(err)
	}
	tx := &core.Transaction{
		Sender:             sender,
		Recipient:          recipient,
		Value:              big.NewInt(1),
		Id:                 id,
		RequestTime:        requestTS,
		ClientTimestamp:    clientTS,
		Second_RequestTime: -1,
		TXmig1_Time:        -1,
		TXmig2_Time:        -1,
		CommitTime:         -1,
		LockTime:           -1,
		UnlockTime:         -1,
	}
	tx.TxHash = tx.Hash()
	return tx
}

// injectClientTxsToShard 向指定分片 N0 发送交易，不覆盖时间戳；async 仅用于 PhaseB。
func injectClientTxsToShard(shardStr string, txs []*core.Transaction, async bool) {
	if len(txs) == 0 {
		return
	}
	nodes := params.NodeTable[shardStr]
	if nodes == nil {
		fmt.Printf("[SyncProbe] 分片 %s 无节点表，跳过注入\n", shardStr)
		return
	}
	leader := nodes["N0"]
	if leader == "" {
		return
	}
	for _, tx := range txs {
		if len(tx.TxHash) == 0 {
			tx.TxHash = tx.Hash()
		}
	}
	payload, err := json.Marshal(TxFromClient{Txs: txs})
	if err != nil {
		log.Panic(err)
	}
	msg := jointMessage(cClient, payload)
	if async {
		go utils.TcpDial(msg, leader)
		return
	}
	utils.TcpDial(msg, leader)
}

// syncProbePhaseA 在 SendNewAddr2Shard 之前向源片注入 tx1+tx3（old），模拟到达序 1→3 且均早于 tx2。
// tx3 在 S0 handleNewMap 后由 mvssProbeEarlyPauseSuffixOld 暂停打包，直至 S1 上真实 tx2 完成并 ack。
func syncProbePhaseA(targets []syncProbeTarget) {
	byShard := map[string][]*core.Transaction{}
	oldRT := syncProbeOldRequestTime
	migrating := make(map[string]bool, len(targets))
	for _, t := range targets {
		migrating[t.addr] = true
	}
	for i, t := range targets {
		recv := pickSyncProbeRecipient(t.addr, t.srcShard, migrating)
		if recv == "" {
			fmt.Printf("[SyncProbe] 账户 %s 无可用收款方，跳过\n", t.addr)
			continue
		}
		tx1 := buildSyncProbeTx(t.addr, recv, syncProbeID(i, 1), syncProbeCTS1, oldRT)
		tx3 := buildSyncProbeTx(t.addr, recv, syncProbeID(i, 3), syncProbeCTS3, oldRT)
		byShard[t.srcShard] = append(byShard[t.srcShard], tx1, tx3)
		fmt.Printf("[SyncProbe] PhaseA addr=%s shard=%s ids=%d,%d recv=%s RequestTime=%d\n",
			t.addr, t.srcShard, tx1.Id, tx3.Id, recv, oldRT)
	}
	for shard, batch := range byShard {
		injectClientTxsToShard(shard, batch, false)
	}
	if len(byShard) > 0 {
		time.Sleep(syncProbeSettle())
		fmt.Printf("[SyncProbe] PhaseA 已同步发送，等待入池 %v 后再下发 NewMap\n", syncProbeSettle())
	}
}

// syncProbePhaseB 向迁出片注入 new（tx2）；源片重定向后由目标片入池、打包上链。
func syncProbePhaseB(targets []syncProbeTarget) {
	byShard := map[string][]*core.Transaction{}
	now := time.Now().UnixMilli()
	created := 0
	migrating := make(map[string]bool, len(targets))
	for _, t := range targets {
		migrating[t.addr] = true
	}
	for i, t := range targets {
		// 收款方必须在目标片；若找不到则跳过该账户，避免 tx2 到目标片后因 recipient 非本片被过滤。
		recv := pickSyncProbeRecipient(t.addr, t.dstShard, migrating)
		if recv == "" {
			fmt.Printf("[SyncProbe] PhaseB 目标片 %s 无可用收款方，跳过 addr=%s\n", t.dstShard, t.addr)
			continue
		}
		tx2 := buildSyncProbeTx(t.addr, recv, syncProbeID(i, 2), syncProbeCTS2, now)
		// 不在源片设 IsRelay：目标片须按迁户本地 new 执行 sender/nonce（IsRelay 会跳过扣款）。
		tx2.IsRelay = false
		tx2.Second_RequestTime = tx2.RequestTime
		byShard[t.srcShard] = append(byShard[t.srcShard], tx2)
		created++
		fmt.Printf("[SyncProbe] PhaseB addr=%s src=%s dst=%s id=%d recv=%s RequestTime=%d\n",
			t.addr, t.srcShard, t.dstShard, tx2.Id, recv, now)
	}
	for shard, batch := range byShard {
		fmt.Printf("[SyncProbe] PhaseB 发送到 %s 的 tx2 数量=%d\n", shard, len(batch))
		injectClientTxsToShard(shard, batch, false)
	}
	if created == 0 {
		fmt.Println("[SyncProbe] PhaseB 未生成任何 tx2")
	}
}

// runSyncProbeBeforeMigration 在 SendNewAddr2Shard 前调用；返回实际探针目标供 PhaseB 使用。
func runSyncProbeBeforeMigration(newAddrs []string, newMap map[string]int) []syncProbeTarget {
	targets := pickSyncProbeTargets(newAddrs, newMap)
	if len(targets) == 0 {
		return nil
	}
	fmt.Printf("[SyncProbe] 本轮迁出探针账户数=%d（最多 %d）\n", len(targets), syncProbeMaxAccounts())
	syncProbePhaseA(targets)
	return targets
}

// ArmSyncProbePhaseB 在 NewMap 之后延迟触发 PhaseB（等待 TXmig1 出块、MigCtx 仍有效）。
func ArmSyncProbePhaseB(targets []syncProbeTarget) {
	if len(targets) == 0 {
		return
	}
	syncProbePendingMu.Lock()
	syncProbePending = targets
	syncProbePhaseBDone = false
	syncProbePendingMu.Unlock()
	fmt.Printf("[SyncProbe] PhaseB 将于 NewMap 后 %v 触发（真实 tx2→目标片）\n", syncProbePhaseBDelay())
	go syncProbePhaseBScheduled(targets)
}

func syncProbePhaseBScheduled(targets []syncProbeTarget) {
	time.Sleep(syncProbePhaseBDelay())
	syncProbePendingMu.Lock()
	if syncProbePhaseBDone {
		syncProbePendingMu.Unlock()
		return
	}
	syncProbePhaseBDone = true
	syncProbePendingMu.Unlock()
	fmt.Println("[SyncProbe] 延迟到期，触发 PhaseB")
	syncProbePhaseB(targets)
}

// TrySyncProbePhaseBOnAnnounce 保留空实现，避免 Announce 过早触发 PhaseB（已改为延迟调度）。
func TrySyncProbePhaseBOnAnnounce(announceShardID string) {
	_ = announceShardID
}
