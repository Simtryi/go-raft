package raft

import (
	crand "crypto/rand"
	"encoding/base64"
	"log"
	"math/big"
	"math/rand"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

func DropAndSet(ch chan struct{}) {
	select {
	case <- ch:
	default:
	}
	ch <- struct{}{}
}

func Clone(date []byte) []byte {
	x := make([]byte, len(date))
	copy(x, date)
	return x
}

func MakeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigInt, _ := crand.Int(crand.Reader, max)
	return bigInt.Int64()
}

func RandString(n int) string {
	b := make([]byte, 2 * n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0 : n]
}

func RandomTime() time.Duration {
	randInt := 300 + rand.Intn(100)
	return time.Duration(randInt) * time.Millisecond
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}