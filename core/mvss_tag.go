package core

import (
	"bytes"
	"crypto/sha256"
	"fmt"
)

const (
	SyncProbeIDBase   = 9_000_000_000
	SyncProbeIDStride = 10
)

// IsSyncProbeTxID 是否为 Sync 探针交易（Id 区间 9000000001 起）。
func IsSyncProbeTxID(id int) bool {
	if id < SyncProbeIDBase {
		return false
	}
	offset := id - SyncProbeIDBase
	return offset >= 0 && offset < 1000
}

// RedirectTag 生成迁移期新交易重定向标签（仿真用确定性哈希）。
func RedirectTag(addr string, mig1Time int64, migNonce uint64) []byte {
	s := fmt.Sprintf("mvss:%s:%d:%d", addr, mig1Time, migNonce)
	h := sha256.Sum256([]byte(s))
	return h[:]
}

// ValidRedirectTag 校验重定向标签是否匹配迁移上下文。
func ValidRedirectTag(tag []byte, addr string, mig1Time int64, migNonce uint64) bool {
	if len(tag) == 0 {
		return false
	}
	return bytes.Equal(tag, RedirectTag(addr, mig1Time, migNonce))
}
