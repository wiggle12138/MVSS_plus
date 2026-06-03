package pbft

import (
	"blockEmulator/params"
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	synclogByShard  = map[string]*csv.Writer{}
	synclogMu       sync.Mutex
	synclogCounter  = map[string]int{}
)

// syncLogOwner 仅由每个分片的 N0 负责写 sync 日志，避免 N0/N1 并发创建同一文件导致写入失效。
func syncLogOwner() bool {
	return params.Config != nil && params.Config.NodeID == "N0"
}

// syncProbeModeForLog 判断是否进入探针日志模式：
// 1) client 显式开启 EnableSyncProbe；或
// 2) 通过环境变量 SYNC_PROBE=1 启动（节点进程常用）。
func syncProbeModeForLog() bool {
	if params.Config != nil && params.Config.EnableSyncProbe {
		return true
	}
	return os.Getenv("SYNC_PROBE") == "1"
}

func initSyncLog(shardID string) {
	if !syncLogOwner() {
		return
	}
	csvFile, err := os.Create("./log/" + shardID + "_sync.csv")
	if err != nil {
		// 不影响主流程，失败时仅跳过 sync 统计。
		fmt.Printf("[SyncLog] init failed shard=%s node=%s err=%v\n", shardID, params.Config.NodeID, err)
		return
	}
	synclogMu.Lock()
	defer synclogMu.Unlock()
	synclogByShard[shardID] = csv.NewWriter(csvFile)
	_ = synclogByShard[shardID].Write([]string{"ts", "event", "mode", "addr", "start_n", "end_n", "ok", "reason", "bytes"})
	synclogByShard[shardID].Flush()
	if syncProbeModeForLog() {
		fmt.Printf("[SyncLog] init ok shard=%s node=%s file=./log/%s_sync.csv\n", shardID, params.Config.NodeID, shardID)
	}
}

// ensureSyncLogWriterLocked 在 writer 缺失时懒初始化（追加模式），避免初始化时机丢失导致整轮无日志。
// 调用方需已持有 synclogMu。
func ensureSyncLogWriterLocked(shardID string) *csv.Writer {
	if w := synclogByShard[shardID]; w != nil {
		return w
	}
	f, err := os.OpenFile("./log/"+shardID+"_sync.csv", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		fmt.Printf("[SyncLog] lazy init failed shard=%s node=%s err=%v\n", shardID, params.Config.NodeID, err)
		return nil
	}
	w := csv.NewWriter(f)
	// 文件为空时补表头，防止首次懒初始化没有列名。
	if st, err := f.Stat(); err == nil && st.Size() == 0 {
		_ = w.Write([]string{"ts", "event", "mode", "addr", "start_n", "end_n", "ok", "reason", "bytes"})
		w.Flush()
	}
	synclogByShard[shardID] = w
	if syncProbeModeForLog() {
		fmt.Printf("[SyncLog] lazy init ok shard=%s node=%s file=./log/%s_sync.csv\n", shardID, params.Config.NodeID, shardID)
	}
	return w
}

// writeSyncLog 写入用于论文分析的最小同步日志（按本分片 ShardID 分文件）。
func writeSyncLog(event, mode, addr string, startN, endN uint64, ok bool, reason string, bytes int) {
	if !syncLogOwner() {
		return
	}
	shardID := ""
	if params.Config != nil {
		shardID = params.Config.ShardID
	}
	synclogMu.Lock()
	w := ensureSyncLogWriterLocked(shardID)
	if w == nil {
		synclogMu.Unlock()
		return
	}
	okVal := "0"
	if ok {
		okVal = "1"
	}
	record := []string{
		strconv.FormatInt(time.Now().UnixMilli(), 10),
		event,
		mode,
		addr,
		strconv.FormatUint(startN, 10),
		strconv.FormatUint(endN, 10),
		okVal,
		reason,
		fmt.Sprintf("%d", bytes),
	}
	synclogCounter[shardID]++
	cnt := synclogCounter[shardID]
	probeMode := syncProbeModeForLog()
	synclogMu.Unlock()
	if err := w.Write(record); err != nil {
		fmt.Printf("[SyncLog] write failed shard=%s node=%s event=%s mode=%s err=%v\n",
			shardID, params.Config.NodeID, event, mode, err)
		return
	}
	// 常规模式降低 flush 频率；探针模式优先保证日志完整落盘，便于核对 sync 链路。
	if probeMode || cnt%32 == 0 || event == "abort" || event == "send" {
		w.Flush()
		if err := w.Error(); err != nil {
			fmt.Printf("[SyncLog] flush failed shard=%s node=%s event=%s mode=%s err=%v\n",
				shardID, params.Config.NodeID, event, mode, err)
			return
		}
	}
	if probeMode {
		fmt.Printf("[SyncLog] write ok shard=%s node=%s event=%s mode=%s count=%d\n",
			shardID, params.Config.NodeID, event, mode, cnt)
	}
}
