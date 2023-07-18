package shardkv

import (
	"time"
	"unsafe"
)

func (skv *ShardKV) TransmitShardHandler(args *TransmitShardArgs, reply *TransmitShardReply) {
	skv.fillReply(args, reply)
	TempDPrintf("ShardKV: %v,TransmitShardHandler() is called with %v\n", skv.me, args)
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
	if command == nil {
		return
	}
	_, _, succeeded := skv.startCommit(*command)
	if !succeeded {
		return
	}
	if skv.waitCommitted(args) {
		reply.Succeeded = true
	}
	TempDPrintf("RequestHandler() finishes with %v\n", reply)
}

func (skv *ShardKV) fillReply(args *TransmitShardArgs, reply *TransmitShardReply) {
	reply.Operation = args.Operation
	reply.TransmitNum = args.TransmitNum
	reply.ChunkNum = args.ChunkNum
	reply.IsKVData = args.IsKVData
	reply.GID = args.GID
	reply.ConfigNum = args.ConfigNum
	reply.Shard = args.Shard
}

func (skv *ShardKV) checkLeaderForTransmit(args *TransmitShardArgs, reply *TransmitShardReply) bool {
	leaderId, votedFor, term := skv.rf.GetLeaderId()
	if votedFor != skv.me {
		TempDPrintf("ShardKV: %v is not the leader. LeaderId: %v, votedFor: %v, term: %v\n", skv.me, leaderId, votedFor, term)
		return false
	}
	return true
}

func (skv *ShardKV) checkDupTransmit(args *TransmitShardArgs, reply *TransmitShardReply) bool {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	transmit, exists := skv.finishedTransmit[args.GID]
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
	skv.mu.Lock()
	defer skv.mu.Unlock()
	transmit, exists := skv.onGoingTransmit[args.GID]
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
		GID:         args.GID,
		ConfigNum:   args.ConfigNum,
		Shard:       args.Shard,
	}
	if command.IsKVData {
		command.ShardKVStoreChunk = ChunkKVStore{
			Size:    unsafe.Sizeof(args.ShardChunk),
			KVStore: args.ShardChunk,
		}
	} else {
		command.ShardCachedReplyChunk = ChunkedCachedReply{
			Size:          unsafe.Sizeof(args.ReplyChunk),
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
	for {
		time.Sleep(time.Duration(CHECKCOMMITTEDTIMEOUT) * time.Millisecond)
		isValidLeader := skv.rf.IsValidLeader()
		if skv.killed() || !isValidLeader {
			break
		}
		skv.mu.Lock()
		if transmit, exists := skv.finishedTransmit[args.GID]; exists {
			if transmit.TransmitNum > args.TransmitNum {
				skv.mu.Unlock()
				return true
			}
			if transmit.TransmitNum == args.TransmitNum && transmit.ChunkNum >= args.ChunkNum {
				skv.mu.Unlock()
				return true
			}
		}
		skv.mu.Unlock()
	}
	return false

}
