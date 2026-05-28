package pbft

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"
)

var (
	synclog        *csv.Writer
	synclogMu      sync.Mutex
	synclogCounter int
)

func initSyncLog(shardID string) {
	csvFile, err := os.Create("./log/" + shardID + "_sync.csv")
	if err != nil {
		// 不影响主流程，失败时仅跳过 sync 统计。
		return
	}
	synclog = csv.NewWriter(csvFile)
	_ = synclog.Write([]string{"ts", "event", "mode", "addr", "start_n", "end_n", "ok", "reason", "bytes"})
	synclog.Flush()
}

// writeSyncLog 写入用于论文分析的最小同步日志。
func writeSyncLog(event, mode, addr string, startN, endN uint64, ok bool, reason string, bytes int) {
	if synclog == nil {
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
	synclogMu.Lock()
	defer synclogMu.Unlock()
	_ = synclog.Write(record)
	synclogCounter++
	// 降低 flush 频率，减少运行时抖动。
	if synclogCounter%32 == 0 || event == "abort" {
		synclog.Flush()
	}
}
