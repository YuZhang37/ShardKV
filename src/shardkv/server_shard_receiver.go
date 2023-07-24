package shardkv

import (
	"time"
	"unsafe"
)

func (skv *ShardKV) TransmitShardHandler(args *TransmitShardArgs, reply *TransmitShardReply) {
	skv.transmitHandlerDPrintf("TransmitShardHandler() receives args: %v\n", args)
	skv.fillReply(args, reply)
	skv.transmitHandlerDPrintf("TransmitShardHandler() fillReply: %v\n", reply)
	if !skv.checkLeaderForTransmit(args, reply) {
		return
	}
	if skv.checkDupTransmit(args, reply) {
		return
	}
	if skv.checkOngoingTransmit(args, reply) {
		if skv.waitCommitted(args) {
			reply.Succeeded = true
		}
		return
	}
	command := skv.getTransmitShardCommand(args, reply)
	skv.transmitHandlerDPrintf("TransmitShardHandler() getTransmitShardCommand: %v\n", command)
	if command == nil {
		return
	}
	_, _, appended := skv.startCommit(*command)
	skv.transmitHandlerDPrintf("TransmitShardHandler() appended: %v\n", appended)
	if !appended {
		return
	}
	skv.lockMu("TransmitShardHandler() with args: %v \n", args)
	// if the command is executed, the command has the latest TransmitNum and ChunkNum
	skv.onGoingTransmit[command.Shard] = TransmitInfo{
		FromGID:     command.FromGID,
		TransmitNum: command.TransmitNum,
		ChunkNum:    command.ChunkNum,
	}

	skv.unlockMu()
	skv.transmitHandlerDPrintf("TransmitShardHandler() waitCommitted\n")
	if skv.waitCommitted(args) {
		reply.Succeeded = true
	}
	skv.transmitHandlerDPrintf("TransmitShardHandler() finishes with %v\n", reply)
}

func (skv *ShardKV) fillReply(args *TransmitShardArgs, reply *TransmitShardReply) {
	reply.Operation = args.Operation
	reply.TransmitNum = args.TransmitNum
	reply.ChunkNum = args.ChunkNum
	reply.IsKVData = args.IsKVData
	reply.FromGID = args.FromGID
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
}

func (skv *ShardKV) checkLeaderForTransmit(args *TransmitShardArgs, reply *TransmitShardReply) bool {
	votedFor := skv.rf.GetVotedFor()
	if votedFor != skv.me {
		skv.tempDPrintf("checkLeaderForTransmit(): Not the leader: skv.me: %v, votedFor: %v, args: %v\n", skv.me, votedFor, args)
		return false
	}
	return true
}

func (skv *ShardKV) checkDupTransmit(args *TransmitShardArgs, reply *TransmitShardReply) bool {
	skv.lockMu("checkDupTransmit() with args: %v\n", args)
	defer skv.unlockMu()
	skv.transmitHandlerDPrintf("TransmitShardHandler() checkDupTransmit: %v\n", skv.finishedTransmit[args.FromGID])
	transmit, exists := skv.finishedTransmit[args.FromGID]
	if !exists {
		return false
	}
	if transmit.TransmitNum > args.TransmitNum {
		reply.Succeeded = true
		return true
	}
	if transmit.TransmitNum < args.TransmitNum {
		return false
	}
	// transmit.TransmitNum == args.TransmitNum
	if transmit.ChunkNum >= args.ChunkNum {
		reply.Succeeded = true
		return true
	}
	// transmit.ChunkNum < args.ChunkNum
	return false

}

func (skv *ShardKV) checkOngoingTransmit(args *TransmitShardArgs, reply *TransmitShardReply) bool {
	skv.lockMu("checkOngoingTransmit() with args: %v\n", args)
	defer skv.unlockMu()
	skv.transmitHandlerDPrintf("TransmitShardHandler() checkOngoingTransmit: %v\n", skv.onGoingTransmit[args.FromGID])
	transmit, exists := skv.onGoingTransmit[args.FromGID]
	if !exists {
		return false
	}
	if transmit.TransmitNum > args.TransmitNum {
		reply.Succeeded = true
		return true
	}
	if transmit.TransmitNum < args.TransmitNum {
		return false
	}
	// transmit.TransmitNum == args.TransmitNum
	if transmit.ChunkNum >= args.ChunkNum {
		reply.Succeeded = true
		return true
	}
	// transmit.ChunkNum < args.ChunkNum
	return false
}

func (skv *ShardKV) getTransmitShardCommand(args *TransmitShardArgs, reply *TransmitShardReply) *TransmitShardCommand {
	command := &TransmitShardCommand{
		Operation:   args.Operation,
		TransmitNum: args.TransmitNum,
		ChunkNum:    args.ChunkNum,
		IsKVData:    args.IsKVData,
		FromGID:     args.FromGID,
		ConfigNum:   args.ConfigNum,
		Shard:       args.Shard,
		IsLastChunk: args.IsLastChunk,
	}
	if command.IsKVData {
		size := uintptr(0)
		for key, value := range args.ShardChunk {
			size += unsafe.Sizeof(key) + unsafe.Sizeof(value)
		}
		command.ShardKVStoreChunk = ChunkKVStore{
			// the size is a 8-byte pointer, needs to update
			Size:    size,
			KVStore: args.ShardChunk,
		}
	} else {
		size := uintptr(0)
		for key, value := range args.ReplyChunk {
			size += unsafe.Sizeof(key) + unsafe.Sizeof(value)
		}
		command.ShardCachedReplyChunk = ChunkedCachedReply{
			// the size is a 8-byte pointer, needs to update
			Size:          size,
			CachedReplies: args.ReplyChunk,
		}
	}
	if unsafe.Sizeof(command) >= MAXTRANSMITSIZE {
		reply.SizeExceeded = true
		return nil
	}
	return command
}

func (skv *ShardKV) waitCommitted(args *TransmitShardArgs) bool {
	skv.transmitHandlerDPrintf("waitCommitted() receives args: %v\n", args)
	for {
		time.Sleep(time.Duration(CHECKCOMMITTEDTIMEOUT) * time.Millisecond)
		isValidLeader := skv.rf.IsValidLeader()
		if skv.killed() || !isValidLeader {
			break
		}
		skv.lockMu("waitCommitted() with args: %v\n", args)
		transmit, exists := skv.finishedTransmit[args.FromGID]
		skv.transmitHandlerDPrintf("waitCommitted() gets transmit for FromGID %v, TransmitNum %v: transmit: %v, exists: %v, TransmitShardArgs: %v\n", args.FromGID, args.TransmitNum, transmit, exists, args)
		if exists {
			if transmit.TransmitNum > args.TransmitNum {
				skv.unlockMu()
				return true
			}
			if transmit.TransmitNum == args.TransmitNum && transmit.ChunkNum >= args.ChunkNum {
				skv.unlockMu()
				return true
			}
		}
		skv.unlockMu()
	}
	return false

}
