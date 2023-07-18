package shardkv

import (
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
