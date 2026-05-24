package core

import (
	"blockEmulator/account"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
)

// TXsync 账户版本同步消息（论文 Stage 3）。
type TXsync struct {
	Address     string
	FromShard   string
	StateOld    *account.AccountState
	StateNew    *account.AccountState
	MPOld       *ProofDB
	MPNew       *ProofDB
	StartN      uint64
	EndN        uint64
	RequestTime int64
}

func (tx *TXsync) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	if err := enc.Encode(tx); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeTXsync(b []byte) *TXsync {
	var tx TXsync
	if err := gob.NewDecoder(bytes.NewReader(b)).Decode(&tx); err != nil {
		log.Panic(err)
	}
	return &tx
}

func (tx *TXsync) Hash() []byte {
	h := sha256.Sum256(tx.Encode())
	return h[:]
}
