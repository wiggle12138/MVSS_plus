package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
)

// TXsyncDelta 是 MVSS-Delta 的增量同步消息（最小可行实现）。
type TXsyncDelta struct {
	Address      string
	FromShard    string
	DeltaBalance *big.Int
	DeltaNonce   int64
	StartN       uint64
	EndN         uint64
	PrevHash     []byte
	DeltaHash    []byte
	RequestTime  int64
	Ack          bool
}

func (tx *TXsyncDelta) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	if err := enc.Encode(tx); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeTXsyncDelta(b []byte) *TXsyncDelta {
	var tx TXsyncDelta
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&tx); err != nil {
		log.Panic(err)
	}
	return &tx
}

// CalcDeltaHash 计算 delta 链哈希，包含 prevHash 保证链式约束。
func (tx *TXsyncDelta) CalcDeltaHash() []byte {
	bal := "0"
	if tx.DeltaBalance != nil {
		bal = tx.DeltaBalance.String()
	}
	payload := fmt.Sprintf("%s|%s|%s|%d|%d|%d|%d|%t|%x",
		tx.Address, tx.FromShard, bal, tx.DeltaNonce, tx.StartN, tx.EndN, tx.RequestTime, tx.Ack, tx.PrevHash)
	h := sha256.Sum256([]byte(payload))
	return h[:]
}
