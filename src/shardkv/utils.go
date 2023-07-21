package shardkv

import (
	"fmt"
	"log"
	"time"
)

const TempDebug = true
const MoveShardDebug = true
const TransmitSenderDebug = true
const TransmitHandlerDebug = true
const SnapshotDebug = true
const FollowerDebug = false
const Temp2Debug = true

const WatchLock = true

func (skv *ShardKV) lockMu(format string, a ...interface{}) {
	skv.mu.Lock()
	if WatchLock {
		skv.lockChan = make(chan int)
		go skv.testLock(format, a...)
	}
}

func (skv *ShardKV) unlockMu() {
	if WatchLock {
		skv.lockChan <- 1
	}
	skv.mu.Unlock()
}

func (skv *ShardKV) testLock(format string, a ...interface{}) {
	quit := false
	for !quit {
		select {
		case <-skv.lockChan:
			quit = true
		case <-time.After(5 * time.Second):
			skv.snapshot2DPrintf("ShardKV testLock(): "+format+"is not unlocked", a...)
		}
	}
}

func (skv *ShardKV) snapshot2DPrintf(format string, a ...interface{}) (n int, err error) {
	// if SnapshotDebug && (leaderId == skv.me || FollowerDebug) {
	if SnapshotDebug {
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) printState(msg string) {
	skv.tempDPrintf(msg+`
	skv.serveShardIDs: %v,\n
	skv.serveShards: %v,\n
	skv.receivingShards: %v,\n
	skv.futureServeConfigNums: %v,\n
	skv.shadowShardGroups: %v,\n
	skv.serveCachedReplies: %v,\n
	skv.receivingCachedReplies: %v,\n
	skv.futureCachedReplies: %v,\n
	skv.finishedTransmit: %v,\n
	skv.config: %v,\n
	skv.transmitNum: %v,\n
	`,
		skv.serveShardIDs,
		skv.serveShards,
		skv.receivingShards,
		skv.futureServeConfigNums,
		skv.shadowShardGroups,
		skv.serveCachedReplies,
		skv.receivingCachedReplies,
		skv.futureCachedReplies,
		skv.finishedTransmit,
		skv.config,
		skv.transmitNum,
	)
}
func (skv *ShardKV) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf("Group: %v, ShardKVServer: %v, try to get votedFor\n", skv.gid, skv.me)
		// _, votedFor, _ := skv.rf.GetLeaderId()
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		// log.Printf("Group: %v, ShardKVServer: %v, got votedFor: %v for msg: %v\n", skv.gid, skv.me, votedFor, prefix)
		// if votedFor == skv.me || FollowerDebug {
		log.Printf(prefix+format, a...)
		// }
	}
	return
}

func (skv *ShardKV) transmitSenderDPrintf(format string, a ...interface{}) (n int, err error) {
	// _, votedFor, _ := skv.rf.GetLeaderId()
	// if TransmitSenderDebug && (votedFor == skv.me || FollowerDebug) {
	if TransmitSenderDebug {
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) transmitHandlerDPrintf(format string, a ...interface{}) (n int, err error) {
	// _, votedFor, _ := skv.rf.GetLeaderId()
	// if TransmitHandlerDebug && (votedFor == skv.me || FollowerDebug) {
	if TransmitHandlerDebug {
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) moveShardDPrintf(format string, a ...interface{}) (n int, err error) {
	// _, votedFor, _ := skv.rf.GetLeaderId()
	// if MoveShardDebug && (votedFor == skv.me || FollowerDebug) {
	if MoveShardDebug {
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) snapshotDPrintf(leaderId int, format string, a ...interface{}) (n int, err error) {
	// if SnapshotDebug && (leaderId == skv.me || FollowerDebug) {
	if SnapshotDebug {
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf(format, a...)
	}
	return
}

func Temp2DPrintf(format string, a ...interface{}) (n int, err error) {
	if Temp2Debug {
		log.Printf(format, a...)
	}
	return
}
