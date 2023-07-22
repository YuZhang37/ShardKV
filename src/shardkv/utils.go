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
const FollowerDebug = true
const Temp2Debug = true
const TestDebug = true

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
	quit := 0
	for quit != 1 {
		select {
		case <-skv.lockChan:
			quit = 1
		case <-time.After(5 * time.Second):
			skv.snapshot2DPrintf("ShardKV testLock(): "+format+"is not unlocked", a...)
		}
	}
}

func (skv *ShardKV) snapshot2DPrintf(format string, a ...interface{}) (n int, err error) {
	if SnapshotDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) printStateForTest(msg string) {
	if TestDebug {
		fmt.Printf(msg+`
		Group: %v, \n
		ShardKVServer: %v, \n
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
			skv.gid,
			skv.me,
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
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) transmitSenderDPrintf(format string, a ...interface{}) (n int, err error) {
	if TransmitSenderDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) transmitHandlerDPrintf(format string, a ...interface{}) (n int, err error) {
	if TransmitHandlerDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) moveShardDPrintf(format string, a ...interface{}) (n int, err error) {
	if MoveShardDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) snapshotDPrintf(leaderId int, format string, a ...interface{}) (n int, err error) {
	if SnapshotDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
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
