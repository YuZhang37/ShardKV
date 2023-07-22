package shardkv

import (
	"fmt"
	"log"
	"time"

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

func (skv *ShardKV) transmitToGroup(index int) {
	skv.lockMu("transmitToGroup() with index: %v\n", index)
	if index >= len(skv.shadowShardGroups) {
		skv.unlockMu()
		return
	}
	if skv.shadowShardGroups[index].Processing {
		skv.unlockMu()
		return
	}
	skv.shadowShardGroups[index].Processing = true
	group := skv.shadowShardGroups[index]
	skv.unlockMu()
	skv.transmitSenderDPrintf("transmitToGroup() receives group: %v\n", group)
	// no currency on read/write a group
	var servers []*labrpc.ClientEnd
	for si := 0; si < len(group.Servernames); si++ {
		srv := skv.make_end(group.Servernames[si])
		servers = append(servers, srv)
	}
	for len(group.ShardIDs) > 0 && !skv.killed() {
		shard := group.ShardIDs[0]
		transmitNum := group.TransmitNums[0]
		configNum := group.ConfigNums[0]
		shardChunks := group.ShadowShards[0]
		skv.transmitSenderDPrintf("transmitToGroup() sends shardKV data: shard: %v, transmitNum: %v, configNum: %v, shardChunks: %v\n group: %v\n", shard, transmitNum, configNum, shardChunks, group)
		for chunkNum := 0; chunkNum < len(shardChunks); chunkNum++ {
			chunk := shardChunks[chunkNum]
			args := &TransmitShardArgs{
				Operation:   TRANSMITSHARD,
				TransmitNum: transmitNum,
				ChunkNum:    chunkNum,
				IsKVData:    true,
				FromGID:     skv.gid,
				ConfigNum:   configNum,
				Shard:       shard,
				ShardChunk:  skv.copyShardKVStoreChunk(&chunk).KVStore,
				IsLastChunk: false,
			}
			skv.sendRequestToServers(args, servers)
		}

		replyChunks := group.ShadowCachedReplies[0]
		skv.transmitSenderDPrintf("transmitToGroup() sends shardKV replies: shard: %v, transmitNum: %v, configNum: %v, replyChunks: %v\n", shard, transmitNum, configNum, replyChunks)
		for i := 0; i < len(replyChunks); i++ {
			chunk := replyChunks[i]
			args := &TransmitShardArgs{
				Operation:   TRANSMITSHARD,
				TransmitNum: transmitNum,
				ChunkNum:    i + len(shardChunks),
				IsKVData:    false,
				FromGID:     skv.gid,
				ConfigNum:   configNum,
				Shard:       shard,
				ReplyChunk:  skv.copyShardCachedReplyChunk(&chunk).CachedReplies,
				IsLastChunk: i == len(replyChunks)-1,
			}
			skv.sendRequestToServers(args, servers)
		}
		skv.transmitSenderDPrintf("transmitToGroup() finishes shard: %v in group of GID: %v\n", shard, group.TargetGID)
		group = skv.removeShardFromShadow(group.TargetGID, shard)
		skv.transmitSenderDPrintf("transmitToGroup() finishes shardKV: shard: %v, transmitNum: %v, configNum: %v, shardChunks: %v len(group.ShardIDs): %v,\n group: %v\n", shard, transmitNum, configNum, shardChunks, len(group.ShardIDs), group)
		skv.mu.Lock()
		skv.printState(fmt.Sprintf("after transmitToGroup() finishes shard: %v", shard))
		skv.mu.Unlock()
	}
	skv.transmitSenderDPrintf("transmitToGroup() quit with group: %v\n skv.killed(): %v", group, skv.killed())
	skv.mu.Lock()
	skv.printState(fmt.Sprintf("after transmitToGroup() finishes group: %v", group))
	skv.mu.Unlock()

}

func (skv *ShardKV) sendRequestToServers(args *TransmitShardArgs, servers []*labrpc.ClientEnd) {
	skv.transmitSenderDPrintf("sendRequestToServers() receives args: %v\n", args)
	var reply *TransmitShardReply = nil
	for reply == nil && !skv.killed() {
		for si := 0; si < len(servers); si++ {
			tempReply := &TransmitShardReply{}
			ok := servers[si].Call("ShardKV.TransmitShardHandler", args, tempReply)
			if !ok {
				// failed server or network disconnection
				skv.transmitSenderDPrintf("sendRequestToServers() with failed server or network disconnection")
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
			skv.transmitSenderDPrintf("sendRequestToServers() not the leader")
		}
		time.Sleep(time.Duration(RETRYTRANSMITTIMEOUT) * time.Millisecond)
	}
	skv.transmitSenderDPrintf("sendRequestToServers() finishes with reply: %v\n", reply)
}

func (skv *ShardKV) removeShardFromShadow(targetGID int, shard int) ShadowShardGroup {
	skv.transmitSenderDPrintf("removeShardFromShadow() receives targetGID: %v, shard: %v\n", targetGID, shard)
	skv.lockMu("removeShardFromShadow() with targetGID: %v, shard: %v\n", targetGID, shard)
	defer skv.unlockMu()
	var index int
	var group *ShadowShardGroup
	var found bool = false
	for index = range skv.shadowShardGroups {
		group = &skv.shadowShardGroups[index]
		if group.TargetGID == targetGID {
			found = true
			break
		}
	}
	if !found {
		log.Fatalf("Fatal: remove shard %v from group %v error: not found group\n", shard, targetGID)
	}

	skv.transmitSenderDPrintf("removeShardFromShadow() before removing group: %v\n", group)
	if len(group.ShardIDs) == 0 {
		log.Fatalf("Fatal: remove shard %v from group %v error: len(group.Shards) == 0\n", shard, targetGID)
	}
	if group.ShardIDs[0] != shard {
		log.Fatalf("Fatal: remove shard %v from group %v error: group.Shards[0]: %v != command.Shard\n", shard, targetGID, group.ShardIDs[0])
	}
	group.ShardIDs = group.ShardIDs[1:]
	group.TransmitNums = group.TransmitNums[1:]
	group.ConfigNums = group.ConfigNums[1:]
	group.ShadowShards = group.ShadowShards[1:]
	group.ShadowCachedReplies = group.ShadowCachedReplies[1:]
	returnedGroup := *group
	skv.transmitSenderDPrintf("removeShardFromShadow() after removing group: %v\n, returnedGroup: %v\n", group, returnedGroup)
	if len(group.ShardIDs) == 0 {
		newGroups := skv.shadowShardGroups[:index]
		newGroups = append(newGroups, skv.shadowShardGroups[index+1:]...)
		// looks like the pointer of an array element is calculated based on base address + offset.
		skv.shadowShardGroups = newGroups
	}
	skv.transmitSenderDPrintf("removeShardFromShadow() finishes targetGID: %v, shard: %v\n group: %v\n returnedGroup: %v\n skv.shardowShardGroups: %v", targetGID, shard, group, returnedGroup, skv.shadowShardGroups)
	return returnedGroup
}
