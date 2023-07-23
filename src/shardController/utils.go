package shardController

import (
	"fmt"
	"log"
	"time"
)

const TempDebug = false
const FollowerDebug = false
const Temp2Debug = false
const SnapshotDebug = false

const WatchLock = true

func (sc *ShardController) lockMu(format string, a ...interface{}) {
	sc.mu.Lock()
	if WatchLock {
		sc.lockChan = make(chan int)
		go sc.testLock(format, a...)
	}
}

func (sc *ShardController) unlockMu() {
	if WatchLock {
		sc.lockChan <- 1
	}
	sc.mu.Unlock()
}

func (sc *ShardController) testLock(format string, a ...interface{}) {
	quit := false
	for !quit {
		select {
		case <-sc.lockChan:
			quit = true
		case <-time.After(5 * time.Second):
			votedFor := int(sc.rf.GetVotedFor())
			prefix := fmt.Sprintf("ShardController: %v, ", sc.me)
			if votedFor == sc.me || FollowerDebug {
				log.Printf(prefix+"MuLock: ShardController testLock(): "+format+"is not unlocked", a...)
			}
		}
	}
}

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardController) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		votedFor := int(sc.rf.GetVotedFor())
		prefix := fmt.Sprintf("ShardController: %v, ", sc.me)
		if votedFor == sc.me || FollowerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (sc *ShardController) processPrintf(start bool, operation string, command ControllerCommand, reply ControllerReply) (n int, err error) {
	votedFor := sc.rf.GetVotedFor()
	if TempDebug && (votedFor == sc.me || FollowerDebug) {
		// if TempDebug {
		if start {
			format := fmt.Sprintf("ShardController: %v, is processing operation: %v, got command %v\n", sc.me, operation, command)
			log.Printf(format)
		} else {
			format := fmt.Sprintf("ShardController: %v, finishes processing operation: %v for %v, got reply %v\n", sc.me, operation, command, reply)
			log.Printf(format)
		}
		log.Printf("ShardController: %v, current state: \n", sc.me)
		for index, config := range sc.configs {
			log.Printf("\nShardController: %v, index: %v, config.Num: %v, \nconfig.operation: %v, \nconfig.Shards (size %v): %v, \nconfig.Groups (size %v): %v, \nconfig.ServerNames (size %v): %v, \nconfig.GroupInfos (size %v): %v\n config.UninitializedShards (size %v): %v", sc.me, index, config.Num, config.Operation, len(config.Shards), config.Shards, len(config.Groups), config.Groups, len(config.ServerNames), config.ServerNames, len(config.GroupInfos), config.GroupInfos, len(config.UninitializedShards), config.UninitializedShards)
		}

	}
	return
}

func Temp2DPrintf(format string, a ...interface{}) (n int, err error) {
	if Temp2Debug {
		log.Printf(format, a...)
	}
	return
}
