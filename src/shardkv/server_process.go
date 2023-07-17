package shardkv

import (
	"bytes"
	"log"
	"unsafe"

	"6.5840/labgob"
)

/*
skv.mu is held for all function calls in this file
*/

func (skv *ShardKV) processConfigUpdateStatic(command ConfigUpdateCommand) {
	skv.tempDPrintf("ShardKV %v receives ConfigUpdateCommand: %v\n", skv.me, command)
	if command.Config.Num < 2 {
		skv.tempDPrintf("Config with configNum: %v is not static, return\n", command.Config.Num)
		return
	}
	if command.Config.Num <= skv.config.Num {
		skv.tempDPrintf("Config with configNum: %v <= current config.Num: %v, return\n", command.Config.Num, skv.config.Num)
		return
	}
	skv.config = command.Config
	for shard, GID := range skv.config.Shards {
		if GID == skv.gid {
			chunks := make([]ChunkKVStore, 0)
			chunks = append(chunks, ChunkKVStore{
				Size:    0,
				KVStore: make(map[string]string),
			})
			skv.serveShards[shard] = chunks
			skv.serveShardIDs[shard] = true
		}
	}
	for shard, GID := range skv.config.Shards {
		if GID == skv.gid {
			chunks := make([]ChunkedCachedReply, 0)
			chunks = append(chunks, ChunkedCachedReply{
				Size:          0,
				CachedReplies: make(map[int64]RequestReply),
			})
			// must lock, since the request handler can check cached replies
			skv.serveCachedReplies[shard] = chunks
		}
	}
	skv.tempDPrintf("skv.config: %v, skv.serveShards: %v, skv.serveShardIDs: %v, skv.serveCachedReplies: %v\n", skv.config, skv.serveShards, skv.serveShardIDs, skv.serveCachedReplies)
	skv.tempDPrintf("ShardKV %v finishes ConfigUpdateCommand: %v\n", skv.me, command)
}

/*
when updating config,
shards in serveShards which are no longer served will be moved to shadow
shards in futureShards for which the current config.Num >= its config.Num, will be inspected to move to serve or shadow
*/

/*
send from group 0 to the current group?
controller needs to tell the server request not the client request
that this is the very first request it sends and the the server needs to pick shards from group 0
*/

// func (skv *ShardKV) processConfigUpdate(command ConfigUpdateCommand) {
// 	if command.Config.Num <= skv.config.Num {
// 		return
// 	}
// 	skv.config = command.Config
// 	for shard := range skv.serveShardIDs {
// 		if skv.config.Shards[shard] != skv.gid {
// 			delete(skv.serveShardIDs, shard)
// 			skv.moveShardToShadow(shard, skv.serveShards, skv.serveCachedReplies)
// 		}
// 	}
// 	for shard, configNum := range skv.futureServeConfigNums {
// 		if configNum <= skv.config.Num {
// 			skv.shardLocks[shard].Lock()
// 			// the server can process futureShards
// 			if skv.config.Shards[shard] == skv.gid {
// 				// move shard from future to serve
// 				skv.serveShardIDs[shard] = true
// 				skv.serveShards[shard] = skv.futureServeShards[shard]
// 				skv.serveCachedReplies[shard] = skv.futureCachedReplies[shard]
// 				delete(skv.futureServeConfigNums, shard)
// 				delete(skv.futureServeShards, shard)
// 				delete(skv.futureCachedReplies, shard)
// 			} else {
// 				// move shard from future to shadow
// 				delete(skv.futureServeConfigNums, shard)
// 				skv.moveShardToShadow(shard, skv.futureServeShards, skv.futureCachedReplies)
// 			}
// 			skv.shardLocks[shard].Unlock()
// 		}
// 	}
// }

/*
when processing the command for client requests, serveShards must have a map for the shard
*/

func (skv *ShardKV) processGet(command ShardKVCommand) *RequestReply {
	value := ""
	exists := false
	for _, chunk := range skv.serveShards[command.Shard] {
		if value, exists = chunk.KVStore[command.Key]; exists {
			break
		}
	}

	reply := &RequestReply{
		ClerkId: command.ClerkId,
		SeqNum:  command.SeqNum,

		ConfigNum: command.ConfigNum,
		Shard:     command.Shard,

		Succeeded: true,
		Value:     value,
		Exists:    exists,
	}
	return reply
}

/*
find the first chunk that has enough space for the put, then add
*/
func (skv *ShardKV) processPut(command ShardKVCommand) *RequestReply {
	skv.putAppendOp(&command)
	reply := &RequestReply{
		ClerkId: command.ClerkId,
		SeqNum:  command.SeqNum,

		ConfigNum: command.ConfigNum,
		Shard:     command.Shard,

		Succeeded: true,
	}
	return reply
}

func (skv *ShardKV) processAppend(command ShardKVCommand) *RequestReply {
	prevValue, prevExists := skv.putAppendOp(&command)
	reply := &RequestReply{
		ClerkId: command.ClerkId,
		SeqNum:  command.SeqNum,

		ConfigNum: command.ConfigNum,
		Shard:     command.Shard,

		Succeeded: true,
		Value:     prevValue,
		Exists:    prevExists,
	}
	return reply
}

func (skv *ShardKV) putAppendOp(command *ShardKVCommand) (string, bool) {

	var targetChunk *ChunkKVStore = nil
	var prevValue string = ""
	var prevExists bool = false
	for _, chunk := range skv.serveShards[command.Shard] {
		if tempValue, tempExists := chunk.KVStore[command.Key]; tempExists {
			delete(chunk.KVStore, command.Key)
			chunk.Size -= unsafe.Sizeof(command.Key + tempValue)
			prevValue = tempValue
			prevExists = true
			break
		}
	}
	var newValue string = command.Value
	if command.Operation == APPEND && prevExists {
		newValue += prevValue
	}
	commandSize := unsafe.Sizeof(command.Key + newValue)
	for _, chunk := range skv.serveShards[command.Shard] {
		if chunk.Size+commandSize <= MAXSHARDCHUNKSIZE {
			targetChunk = &chunk
			break
		}
	}
	if targetChunk == nil {
		targetChunk = &ChunkKVStore{
			Size:    0,
			KVStore: make(map[string]string),
		}
		skv.serveShards[command.Shard] = append(skv.serveShards[command.Shard], *targetChunk)
	}
	targetChunk.Size += commandSize

	targetChunk.KVStore[command.Key] = newValue
	return prevValue, prevExists
}

/*
processSnapshot will only called in follower
or at the start of a new leader
for both cases, skv.clerkChans will be empty
*/
func (skv *ShardKV) processSnapshot(snapshotIndex int, snapshotTerm int, snapshot []byte) {
	skv.mu.Lock()
	defer skv.mu.Unlock()
	skv.decodeSnapshot(snapshot)
	skv.latestAppliedIndex = snapshotIndex
	skv.latestAppliedTerm = snapshotTerm
	/*
		start threads to send requests in shadowShardGroups
	*/
}

/*
must be called with skv.mu.Lock
*/
func (skv *ShardKV) decodeSnapshot(snapshot []byte) {
	reader := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(reader)

	// shard -> key: value store
	var (
		serveShardIDs         map[int]bool
		serveShards           map[int][]ChunkKVStore
		receivingShards       map[int][]ChunkKVStore
		futureServeConfigNums map[int]int
		futureServeShards     map[int][]ChunkKVStore
		shadowShardGroups     []ShadowShardGroup

		// shard -> clerk id: latest cached reply
		serveCachedReplies     map[int][]ChunkedCachedReply
		receivingCachedReplies map[int][]ChunkedCachedReply
		futureCachedReplies    map[int][]ChunkedCachedReply
		finishedTransmit       map[int]TransmitInfo
	)

	if d.Decode(&serveShardIDs) != nil ||
		d.Decode(&serveShards) != nil ||
		d.Decode(&receivingShards) != nil ||
		d.Decode(&futureServeConfigNums) != nil ||
		d.Decode(&futureServeShards) != nil ||
		d.Decode(&shadowShardGroups) != nil ||
		d.Decode(&serveCachedReplies) != nil ||
		d.Decode(&receivingCachedReplies) != nil ||
		d.Decode(&futureCachedReplies) != nil ||
		d.Decode(&finishedTransmit) != nil {
		log.Fatalf("Fatal: decoding error!\n")
	} else {
		skv.serveShardIDs = serveShardIDs
		skv.serveShards = serveShards
		skv.receivingShards = receivingShards
		skv.futureServeConfigNums = futureServeConfigNums
		skv.futureServeShards = futureServeShards
		skv.shadowShardGroups = shadowShardGroups
		skv.serveCachedReplies = serveCachedReplies
		skv.receivingCachedReplies = receivingCachedReplies
		skv.futureCachedReplies = futureCachedReplies
		skv.finishedTransmit = finishedTransmit
	}
}

/*
must be called with skv.mu.Lock
*/
func (skv *ShardKV) encodeSnapshot() []byte {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	if e.Encode(skv.serveShardIDs) != nil ||
		e.Encode(skv.serveShards) != nil ||
		e.Encode(skv.receivingShards) != nil ||
		e.Encode(skv.futureServeConfigNums) != nil ||
		e.Encode(skv.futureServeShards) != nil ||
		e.Encode(skv.shadowShardGroups) != nil ||
		e.Encode(skv.serveCachedReplies) != nil ||
		e.Encode(skv.receivingCachedReplies) != nil ||
		e.Encode(skv.futureCachedReplies) != nil ||
		e.Encode(skv.finishedTransmit) != nil {
		log.Fatalf("encoding error!\n")
	}
	data := writer.Bytes()
	return data
}
