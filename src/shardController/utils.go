package shardController

import (
	"fmt"
	"log"
)

const TempDebug = false
const FollowerDebug = false
const Temp2Debug = false

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardController) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	_, votedFor, _ := sc.rf.GetLeaderId()
	if TempDebug && (votedFor == sc.me || FollowerDebug) {
		// if TempDebug {
		prefix := fmt.Sprintf("ShardController: %v ", sc.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (sc *ShardController) processPrintf(start bool, operation string, command ControllerCommand, reply ControllerReply) (n int, err error) {
	_, votedFor, _ := sc.rf.GetLeaderId()
	if TempDebug && (votedFor == sc.me || FollowerDebug) {
		// if TempDebug {
		if start {
			format := fmt.Sprintf("ShardServer: %v, is processing operation: %v, got command %v\n", sc.me, operation, command)
			log.Printf(format)
		} else {
			format := fmt.Sprintf("ShardServer: %v, finishes processing operation: %v for %v, got reply %v\n", sc.me, operation, command, reply)
			log.Printf(format)
		}
		log.Printf("ShardServer: %v, current state: \n", sc.me)
		for index, config := range sc.configs {
			log.Printf("\nShardServer: %v, index: %v, config.Num: %v, \nconfig.operation: %v, \nconfig.Shards (size %v): %v, \nconfig.Groups (size %v): %v, \nconfig.ServerNames (size %v): %v, \nconfig.GroupInfos (size %v): %v\n config.UninitializedShards (size %v): %v", sc.me, index, config.Num, config.Operation, len(config.Shards), config.Shards, len(config.Groups), config.Groups, len(config.ServerNames), config.ServerNames, len(config.GroupInfos), config.GroupInfos, len(config.UninitializedShards), config.UninitializedShards)
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
