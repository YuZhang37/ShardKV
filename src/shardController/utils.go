package shardController

import (
	"fmt"
	"log"
	"time"
)

const tempDebug = false
const followerDebug = false

const watchLock = true

func (sc *ShardController) lockMu(format string, a ...interface{}) {
	sc.mu.Lock()
	if watchLock {
		sc.lockChan = make(chan int)
		go sc.testLock(format, a...)
	}
}

func (sc *ShardController) unlockMu() {
	if watchLock {
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
			if votedFor == sc.me || followerDebug {
				log.Printf(prefix+"MuLock: ShardController testLock(): "+format+"is not unlocked", a...)
			}
		}
	}
}

func tempDPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardController) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	if tempDebug {
		votedFor := int(sc.rf.GetVotedFor())
		prefix := fmt.Sprintf("ShardController: %v, ", sc.me)
		if votedFor == sc.me || followerDebug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func (sc *ShardController) processPrintf(start bool, operation string, command ControllerCommand, reply ControllerReply) (n int, err error) {
	votedFor := sc.rf.GetVotedFor()
	if tempDebug && (votedFor == sc.me || followerDebug) {
		// if tempDebug {
		if start {
			format := fmt.Sprintf("ShardController: %v, is processing operation: %v, got command %v", sc.me, operation, command)
			log.Println(format)
		} else {
			format := fmt.Sprintf("ShardController: %v, finishes processing operation: %v for %v, got reply %v\n", sc.me, operation, command, reply)
			log.Println(format)
		}
		log.Printf("ShardController: %v, current state: \n", sc.me)
		for index, config := range sc.configs {
			log.Printf("\nShardController: %v, index: %v, config.Num: %v, \nconfig.operation: %v, \nconfig.Shards (size %v): %v, \nconfig.Groups (size %v): %v, \nconfig.ServerNames (size %v): %v, \nconfig.GroupInfos (size %v): %v\n config.UninitializedShards (size %v): %v", sc.me, index, config.Num, config.Operation, len(config.Shards), config.Shards, len(config.Groups), config.Groups, len(config.ServerNames), config.ServerNames, len(config.GroupInfos), config.GroupInfos, len(config.UninitializedShards), config.UninitializedShards)
		}

	}
	return
}
