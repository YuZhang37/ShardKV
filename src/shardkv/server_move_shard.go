package shardkv

import (
	"log"
	"time"
	"unsafe"

	"6.5840/labrpc"
)

/*
transmit request sender and handler
*/

/*
need a separate client and handler
for ShardOpArgs and ShardOpReply
client: retry on failure until the leader of the group commits the request
handler: create a command and commit the command, no need to apply for replying

when updating config, need to move shards which are no longer managed to shallowShards
and send transmitShard requests

this process may retry, needs to keep info on which shard sends to which group
to avoid the command being executed twice, the receive end needs to cached shardTransmit requests

when processing transmitShard, need to fetch shards
send fetchShard requests

when processing ACK,
send ACK requests
*/

func (skv *ShardKV) transmitToGroup(group ShadowShardGroup) {
	// no currency on read/write a group
	var servers []*labrpc.ClientEnd
	for si := 0; si < len(group.Servernames); si++ {
		srv := skv.make_end(group.Servernames[si])
		servers = append(servers, srv)
	}
	chunkNum := 0
	for len(group.ShardIDs) > 0 && !skv.killed() {
		shard := group.ShardIDs[0]
		configNum := group.ConfigNums[0]
		shardChunks := group.ShadowShards[0]
		for i := 0; i < len(shardChunks); i++ {
			chunk := shardChunks[i]
			chunkNum++
			args := &TransmitShardArgs{
				Operation:   TRANSMITSHARD,
				TransmitNum: group.TransmitNum,
				ChunkNum:    chunkNum,
				IsKVData:    true,
				GID:         group.TargetGID,
				ConfigNum:   configNum,
				Shard:       shard,
				ShardChunk:  chunk.KVStore,
				IsLastChunk: i == len(shardChunks)-1,
			}
			skv.sendRequestToServers(args, servers)
		}
		replyChunks := group.ShadowCachedReplies[0]
		for i := 0; i < len(replyChunks); i++ {
			chunk := replyChunks[i]
			chunkNum++
			args := &TransmitShardArgs{
				Operation:   TRANSMITSHARD,
				TransmitNum: group.TransmitNum,
				ChunkNum:    chunkNum,
				IsKVData:    false,
				GID:         group.TargetGID,
				ConfigNum:   configNum,
				Shard:       shard,
				ReplyChunk:  chunk.CachedReplies,
				IsLastChunk: i == len(shardChunks)-1,
			}
			skv.sendRequestToServers(args, servers)
		}
		skv.removeShardFromShadow(group.TargetGID, shard)
	}
}

func (skv *ShardKV) sendRequestToServers(args *TransmitShardArgs, servers []*labrpc.ClientEnd) {
	var reply *TransmitShardReply = nil
	for reply == nil && !skv.killed() {
		for si := 0; si < len(servers); si++ {
			tempReply := &TransmitShardReply{}
			ok := servers[si].Call("ShardKV.TransmitShardHandler", args, tempReply)
			if !ok {
				// failed server or network disconnection
				continue
			}
			if tempReply.Succeeded {
				// the raft server commits
				reply = tempReply
				break
			}
			if tempReply.SizeExceeded {
				log.Fatalf("Transmit chunk is too large, max allowed chunk size is %v\n", MAXSHARDCHUNKSIZE)
			}
			// not the leader
		}
		time.Sleep(time.Duration(RETRYTRANSMITTIMEOUT) * time.Millisecond)
	}
	TempDPrintf("sendRequest() finishes with %v\n", reply)
}

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

	if transmit.ChunkNum < args.ChunkNum {
		return false
	}

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

	if transmit.ChunkNum < args.ChunkNum {
		return false
	}

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

func (skv *ShardKV) forwardReceivingChunks(configNum int, shard int) {
	if configNum == skv.config.Num {
		skv.serveShardIDs[shard] = true
		skv.serveShards[shard] = skv.receivingShards[shard]
		skv.serveCachedReplies[shard] = skv.receivingCachedReplies[shard]
	} else if configNum > skv.config.Num {
		skv.futureServeShards[shard] = skv.receivingShards[shard]
		skv.futureCachedReplies[shard] = skv.receivingCachedReplies[shard]
		skv.futureServeConfigNums[shard] = configNum
	} else {
		// configNum < skv.config.Num
		if skv.config.Shards[shard] == skv.gid {
			skv.serveShardIDs[shard] = true
			skv.serveShards[shard] = skv.receivingShards[shard]
			skv.serveCachedReplies[shard] = skv.receivingCachedReplies[shard]
		} else {
			// move shard to shadow
			skv.moveShardToShadow(shard, skv.receivingShards, skv.receivingCachedReplies)
		}
	}

}

// skv.shardLocks[shard] needs to be held
func (skv *ShardKV) moveShardToShadow(shard int, sourceShards map[int][]ChunkKVStore, sourceCachedReplies map[int][]ChunkedCachedReply) {
	targetGID := skv.config.Shards[shard]
	var group *ShadowShardGroup = nil
	for _, tempGroup := range skv.shadowShardGroups {
		if tempGroup.TargetGID == targetGID {
			group = &tempGroup
			break
		}
	}
	if group == nil {
		servernames := make([]string, 0)
		servernames = append(servernames, skv.config.Groups[targetGID]...)
		group = &ShadowShardGroup{
			TargetGID:           targetGID,
			Servernames:         servernames,
			ShardIDs:            make([]int, 0),
			ConfigNums:          make([]int, 0),
			ShadowShards:        make([][]ChunkKVStore, 0),
			ShadowCachedReplies: make([][]ChunkedCachedReply, 0),
		}
	}
	group.ShardIDs = append(group.ShardIDs, shard)
	group.ConfigNums = append(group.ConfigNums, skv.config.Num)
	group.ShadowShards = append(group.ShadowShards, sourceShards[shard])
	group.ShadowCachedReplies = append(group.ShadowCachedReplies, sourceCachedReplies[shard])
	delete(sourceShards, shard)
	delete(sourceCachedReplies, shard)
}

func (skv *ShardKV) removeShardFromShadow(targetGID int, shard int) {
	var index int
	var group ShadowShardGroup
	var found bool = false
	for index, group = range skv.shadowShardGroups {
		if group.TargetGID == targetGID {
			found = true
			if len(group.ShardIDs) == 0 {
				log.Fatalf("remove shard %v from group %v error: len(group.Shards) == 0\n", shard, targetGID)
			}
			if group.ShardIDs[0] != shard {
				log.Fatalf("remove shard %v from group %v error: group.Shards[0]: %v != command.Shard\n", shard, targetGID, group.ShardIDs[0])
			}
			group.ShardIDs = group.ShardIDs[1:]
			group.ShadowShards = group.ShadowShards[1:]
			group.ShadowCachedReplies = group.ShadowCachedReplies[1:]
		}
	}
	if !found {
		log.Fatalf("remove shard %v from group %v error: not found group\n", shard, targetGID)
	}
	if len(group.ShardIDs) == 0 {
		newGroups := skv.shadowShardGroups[:index]
		newGroups = append(newGroups, skv.shadowShardGroups[index:]...)
		skv.shadowShardGroups = newGroups
	}
}
