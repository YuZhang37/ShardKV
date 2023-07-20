package shardkv

import (
	"fmt"
	"log"
)

const TempDebug = true
const MoveShardDebug = true
const TransmitSenderDebug = true
const TransmitHandlerDebug = true
const SnapshotDebug = true
const FollowerDebug = false
const Temp2Debug = true

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
	_, votedFor, _ := skv.rf.GetLeaderId()
	if TempDebug && (votedFor == skv.me || FollowerDebug) {
		prefix := fmt.Sprintf("Group: %v: ShardKVServer: %v ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) transmitSenderDPrintf(format string, a ...interface{}) (n int, err error) {
	_, votedFor, _ := skv.rf.GetLeaderId()
	if TransmitSenderDebug && (votedFor == skv.me || FollowerDebug) {
		prefix := fmt.Sprintf("Group: %v: ShardKVServer: %v ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) transmitHandlerDPrintf(format string, a ...interface{}) (n int, err error) {
	_, votedFor, _ := skv.rf.GetLeaderId()
	if TransmitHandlerDebug && (votedFor == skv.me || FollowerDebug) {
		prefix := fmt.Sprintf("Group: %v: ShardKVServer: %v ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) moveShardDPrintf(format string, a ...interface{}) (n int, err error) {
	_, votedFor, _ := skv.rf.GetLeaderId()
	if MoveShardDebug && (votedFor == skv.me || FollowerDebug) {
		prefix := fmt.Sprintf("Group: %v: ShardKVServer: %v ", skv.gid, skv.me)
		log.Printf(prefix+format, a...)
	}
	return
}

func (skv *ShardKV) snapshotDPrintf(leaderId int, format string, a ...interface{}) (n int, err error) {
	// if SnapshotDebug && (leaderId == skv.me || FollowerDebug) {
	if SnapshotDebug {
		prefix := fmt.Sprintf("Group: %v: ShardKVServer: %v ", skv.gid, skv.me)
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

// func (sc *ShardController) tempDPrintf(format string, a ...interface{}) (n int, err error) {
// 	_, votedFor, _ := sc.rf.GetLeaderId()
// 	if TempDebug && (votedFor == sc.me || FollowerDebug) {
// 		// if TempDebug {
// 		prefix := fmt.Sprintf("ShardController: %v ", sc.me)
// 		log.Printf(prefix+format, a...)
// 	}
// 	return
// }

// func (sc *ShardController) processPrintf(start bool, operation string, command ControllerCommand, reply ControllerReply) (n int, err error) {
// 	_, votedFor, _ := sc.rf.GetLeaderId()
// 	if TempDebug && (votedFor == sc.me || FollowerDebug) {
// 		// if TempDebug {
// 		if start {
// 			format := fmt.Sprintf("ShardServer: %v, is processing operation: %v, got command %v\n", sc.me, operation, command)
// 			log.Printf(format)
// 		} else {
// 			format := fmt.Sprintf("ShardServer: %v, finishes processing operation: %v for %v, got reply %v\n", sc.me, operation, command, reply)
// 			log.Printf(format)
// 		}
// 		log.Printf("ShardServer: %v, current state: \n", sc.me)
// 		for index, config := range sc.configs {
// 			log.Printf("\nShardServer: %v, index: %v, config.Num: %v, \nconfig.operation: %v, \nconfig.Shards (size %v): %v, \nconfig.Groups (size %v): %v, \nconfig.ServerNames (size %v): %v, \nconfig.GroupInfos (size %v): %v\n", sc.me, index, config.Num, config.Operation, len(config.Shards), config.Shards, len(config.Groups), config.Groups, len(config.ServerNames), config.ServerNames, len(config.GroupInfos), config.GroupInfos)
// 		}

// 	}
// 	return
// }
