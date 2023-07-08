package shardController

import (
	"fmt"
	"log"
)

const TempDebug = true
const Temp2Debug = false

func TempDPrintf(format string, a ...interface{}) (n int, err error) {
	if TempDebug {
		log.Printf(format, a...)
	}
	return
}

func (sc *ShardController) tempDPrintf(format string, a ...interface{}) (n int, err error) {
	// _, votedFor, _ := sc.rf.GetLeaderId()
	if TempDebug {
		prefix := fmt.Sprintf("ShardController: %v ", sc.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (sc *ShardController) processPrintf(start bool, operation string, commandOrReply interface{}) (n int, err error) {
	// _, votedFor, _ := sc.rf.GetLeaderId()
	if TempDebug {
		if start {
			format := fmt.Sprintf("ShardServer: %v, is processing operation: %v, got command %v\n", sc.me, operation, commandOrReply)
			log.Printf(format)
		} else {
			format := fmt.Sprintf("ShardServer: %v, finishes processing operation: %v, reply %v\n", sc.me, operation, commandOrReply)
			log.Printf(format)
		}
		log.Printf("ShardServer: %v, current state: \n", sc.me)
		for index, config := range sc.configs {
			log.Printf("ShardServer: %v, index: %v, config.Num: %v, config.Shards: %v, config.Groups(size %v): %v, config.ServerNames: %v, config.GroupShards: %v\n", sc.me, index, config.Num, config.Shards, len(config.Groups), config.Groups, config.ServerNames, config.GroupShards)
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
