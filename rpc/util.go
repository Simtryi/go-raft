package rpc

import "log"

const Debug = false

func DPrintf(format string, message ...interface{}) {
	if Debug {
		log.Printf(format, message...)
	}
}

func DropAndSet(ch chan struct{}) {
	select {
	case <- ch:
	default:
	}
	ch <- struct{}{}
}