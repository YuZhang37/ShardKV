package shardkv

import (
	"fmt"
	"log"
	"time"
)

const TempDebug = false
const MoveShardDebug = false
const TransmitSenderDebug = false
const TransmitHandlerDebug = false
const SnapshotDebug = false
const FollowerDebug = false
const Temp2Debug = false
const TestDebug = false

const WatchLock = 1

func (skv *ShardKV) lockMu(format string, a ...interface{}) {
	skv.mu.Lock()
	if WatchLock == 1 {
		skv.lockChan = make(chan int)
		go skv.watchMuLock(format, a...)
	}
}

func (skv *ShardKV) unlockMu() {
	if WatchLock == 1 {
		skv.lockChan <- 1
	}
	skv.mu.Unlock()
}

func (skv *ShardKV) watchMuLock(format string, a ...interface{}) {
	quit := 0
	for quit != 1 {
		select {
		case <-skv.lockChan:
			quit = 1
		case <-time.After(5 * time.Second):
			votedFor := int(skv.rf.GetVotedFor())
			prefix := fmt.Sprintf("MuLock: Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
			if votedFor == skv.me || FollowerDebug {
				log.Printf(prefix+"ShardKV testLock(): "+format+" is not unlocked\n", a...)
			}
		}
	}
}

func (skv *ShardKV) lockShard(shard int, format string, a ...interface{}) {
	skv.shardLocks[shard].Lock()
	if WatchLock == 1 {
		skv.shardLockChans[shard] = make(chan int)
		go skv.watchShardLock(shard, format, a...)
	}
}

func (skv *ShardKV) unlockShard(shard int) {
	if WatchLock == 1 {
		skv.shardLockChans[shard] <- 1
	}
	skv.shardLocks[shard].Unlock()
}

func (skv *ShardKV) watchShardLock(shard int, format string, a ...interface{}) {
	quit := 0
	for quit != 1 {
		select {
		case <-skv.shardLockChans[shard]:
			quit = 1
		case <-time.After(5 * time.Second):
			votedFor := int(skv.rf.GetVotedFor())
			prefix := fmt.Sprintf("ShardLock: Group: %v, ShardKVServer: %v, Shard: %v ", skv.gid, skv.me, shard)
			if votedFor == skv.me || FollowerDebug {
				log.Printf(prefix+"ShardKV testLock(): "+format+" is not unlocked\n", a...)
			}
		}
	}
}

// can't disable follower
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

// can disable follower
func (skv *ShardKV) printState(msg string) {
	skv.tempDPrintf(msg+`
	skv.serveShardIDs: %v,
	skv.serveShards: %v,
	skv.receivingShards: %v,
	skv.futureServeConfigNums: %v,
	skv.shadowShardGroups: %v,
	skv.serveCachedReplies: %v,
	skv.receivingCachedReplies: %v,
	skv.futureCachedReplies: %v,
	skv.finishedTransmit: %v,
	skv.config: %v,
	skv.transmitNum: %v,
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

func (skv *ShardKV) logFatal(format string, a ...interface{}) {
	prefix := fmt.Sprintf("Fatal: Group: %v, ShardKVServer: %v, ", skv.gid, skv.me)
	log.Fatalf(prefix+format, a...)
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
		prefix := fmt.Sprintf("ShardKVServer: %v, Group: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) transmitHandlerDPrintf(format string, a ...interface{}) (n int, err error) {
	if TransmitHandlerDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("ShardKVServer: %v, Group: %v, ", skv.gid, skv.me)
		if votedFor == skv.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (skv *ShardKV) moveShardDPrintf(format string, a ...interface{}) (n int, err error) {
	if MoveShardDebug {
		votedFor := int(skv.rf.GetVotedFor())
		prefix := fmt.Sprintf("ShardKVServer: %v, Group: %v, ", skv.gid, skv.me)
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
