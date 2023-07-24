package kvraft

import (
	"fmt"
	"log"
)

const tempDebug = false
const tempDebug2 = false
const tempDebug3 = false
const followerDebug = false

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug {
		log.Printf(format, a...)
	}
	return
}

func Temp2DPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug2 {
		log.Printf(format, a...)
	}
	return
}

func (kv *KVServer) kvStoreDPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug3 {
		votedFor := int(kv.rf.GetVotedFor())
		prefix := fmt.Sprintf("KVServer: %v, ", kv.me)
		if votedFor == kv.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}
