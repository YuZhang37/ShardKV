package shardkv

import (
	"bytes"
	"log"
	"sort"
	"unsafe"

	"6.5840/labgob"
	"6.5840/shardController"
)

/*
skv.mu is held for all function calls in this file
*/

/*
when updating config,
shards in serveShards which are no longer served will be moved to shadow
shards in futureShards for which the current config.Num >= its config.Num, will be inspected to move to serve or shadow
*/

func (skv *ShardKV) processConfigUpdate(command ConfigUpdateCommand) {
	skv.tempDPrintf("ShardKV %v receives ConfigUpdateCommand: %v\n", skv.me, command)
	if command.Config.Num <= skv.config.Num {
		skv.tempDPrintf("Config with configNum: %v <= current config.Num: %v, return\n", command.Config.Num, skv.config.Num)
		return
	}
	skv.tempDPrintf("ShardKV %v receives valid ConfigUpdateCommand: %v\n", skv.me, command)
	skv.controllerSeqNum = skv.controllerSeqNum + 1
	skv.config = command.Config
	if len(skv.config.AssignedShards) > 0 {
		skv.tempDPrintf("ShardKV %v initializing for ConfigUpdateCommand: %v\n", skv.me, command)
		skv.initializeShardsFromConfig()
		skv.tempDPrintf("ShardKV %v finishes initializing for ConfigUpdateCommand, skv.serveShardIDs: %v,\n skv.serveShards: %v,\n skv.serveCachedReplies: %v\n", skv.me, skv.serveShardIDs, skv.serveShards, skv.serveCachedReplies)
	}
	sortedShards := skv.sortedKeys(skv.serveShardIDs)
	skv.tempDPrintf("processConfigUpdate(): sortedShards: %v, skv.serveShardIDs: %v\n", sortedShards, skv.serveShardIDs)
	for _, shard := range sortedShards {
		if skv.config.Shards[shard] != skv.gid {
			skv.shardLocks[shard].Lock()
			delete(skv.serveShardIDs, shard)
			skv.moveShardToShadow(shard, skv.serveShards, skv.serveCachedReplies)
			skv.moveShardDPrintf("after moveShardToShadow() updating group for shard: %v, shadowGroups: %v\n", shard, skv.shadowShardGroups)
			skv.shardLocks[shard].Unlock()
		}
	}
	for shard, configNum := range skv.futureServeConfigNums {
		if configNum <= skv.config.Num {
			skv.shardLocks[shard].Lock()
			// the server can process futureShards
			if skv.config.Shards[shard] == skv.gid {
				// move shard from future to serve
				skv.serveShardIDs[shard] = true
				skv.serveShards[shard] = skv.futureServeShards[shard]
				skv.serveCachedReplies[shard] = skv.futureCachedReplies[shard]
				delete(skv.futureServeConfigNums, shard)
				delete(skv.futureServeShards, shard)
				delete(skv.futureCachedReplies, shard)
			} else {
				// move shard from future to shadow
				delete(skv.futureServeConfigNums, shard)
				skv.moveShardToShadow(shard, skv.futureServeShards, skv.futureCachedReplies)
			}
			skv.shardLocks[shard].Unlock()
		}
	}
}

func (skv *ShardKV) sortedKeys(groups map[int]bool) []int {
	keys := make([]int, 0)
	for key := range groups {
		keys = append(keys, key)
	}
	sort.Ints(keys)
	return keys
}

func (skv *ShardKV) initializeShardsFromConfig() {
	for _, shard := range skv.config.AssignedShards {
		chunks := make([]ChunkKVStore, 0)
		chunks = append(chunks, ChunkKVStore{
			Size:    0,
			KVStore: make(map[string]string),
		})
		skv.serveShards[shard] = chunks
		skv.serveShardIDs[shard] = true
	}
	for _, shard := range skv.config.AssignedShards {
		chunks := make([]ChunkedCachedReply, 0)
		chunks = append(chunks, ChunkedCachedReply{
			Size:          0,
			CachedReplies: make(map[int64]RequestReply),
		})
		// must lock, since the request handler can check cached replies
		skv.serveCachedReplies[shard] = chunks
	}
	skv.tempDPrintf("skv.config: %v, skv.serveShards: %v, skv.serveShardIDs: %v, skv.serveCachedReplies: %v\n", skv.config, skv.serveShards, skv.serveShardIDs, skv.serveCachedReplies)
}

/*
KVData is saved in receivingShards
Replies is saved in receivingCachedReplies
when the last chunk of both is received, the sender will delete
both, the receiver will need to forward the received
*/

func (skv *ShardKV) processTransmitShard(command TransmitShardCommand) {
	skv.moveShardDPrintf("processTransmitShard() receives TransmitShardCommand: %v\n", command)
	skv.shardLocks[command.Shard].Lock()
	defer skv.shardLocks[command.Shard].Unlock()
	if skv.checkDupTransmitForProcessing(&command) {
		skv.moveShardDPrintf("processTransmitShard() detects dup for command: %v\n", command)
		return
	}
	skv.moveShardDPrintf("processTransmitShard() receives valid TransmitShardCommand: %v\n", command)
	if command.IsKVData {
		var shardKVStore []ChunkKVStore
		var exists bool
		if shardKVStore, exists = skv.receivingShards[command.Shard]; !exists {
			shardKVStore = make([]ChunkKVStore, 0)
			skv.receivingShards[command.Shard] = shardKVStore
		}
		skv.receivingShards[command.Shard] = append(shardKVStore, skv.copyShardKVStoreChunk(&command.ShardKVStoreChunk))

	} else {
		var shardCachedReply []ChunkedCachedReply
		var exists bool
		if shardCachedReply, exists = skv.receivingCachedReplies[command.Shard]; !exists {
			shardCachedReply = make([]ChunkedCachedReply, 0)
			skv.receivingCachedReplies[command.Shard] = shardCachedReply
		}
		skv.receivingCachedReplies[command.Shard] = append(shardCachedReply, skv.copyShardCachedReplyChunk(&command.ShardCachedReplyChunk))
	}

	// if the command is executed, the command has the latest TransmitNum and ChunkNum
	skv.finishedTransmit[command.FromGID] = TransmitInfo{
		FromGID:     command.FromGID,
		TransmitNum: command.TransmitNum,
		ChunkNum:    command.ChunkNum,
	}
	skv.moveShardDPrintf("processTransmitShard() updates: \n skv.receivingShards(size %v): %v,\n skv.receivingCachedReplies (size %v): %v,\n skv.finishedTransmit (size %v): %v\n",
		len(skv.receivingShards), skv.receivingShards, len(skv.receivingCachedReplies), skv.receivingCachedReplies, len(skv.finishedTransmit), skv.finishedTransmit)
	// the transmit request handler can poll finishedTransmit to find out if the command has been applied or not

	if command.IsLastChunk {
		skv.printState("Before forwarding: \n")
		skv.forwardReceivingChunks(command.ConfigNum, command.Shard)
		skv.printState("After forwarding: \n")
	}
	skv.moveShardDPrintf("processTransmitShard() finishes TransmitShardCommand: %v\n", command)
}

func (skv *ShardKV) checkDupTransmitForProcessing(command *TransmitShardCommand) bool {
	transmit, exists := skv.finishedTransmit[command.FromGID]
	skv.moveShardDPrintf("checkDupTransmitForProcessing for command: %v, cached transmit: %v, exists: %v\n", command, transmit, exists)
	if !exists {
		return false
	}
	if transmit.TransmitNum > command.TransmitNum {
		return true
	}
	if transmit.TransmitNum < command.TransmitNum {
		return false
	}
	// transmit.TransmitNum == args.TransmitNum
	return transmit.ChunkNum >= command.ChunkNum
}

func (skv *ShardKV) copyShardKVStoreChunk(chunk *ChunkKVStore) ChunkKVStore {
	newChunk := ChunkKVStore{
		Size:    chunk.Size,
		KVStore: make(map[string]string),
	}
	for key, value := range chunk.KVStore {
		newChunk.KVStore[key] = value
	}
	return newChunk
}

func (skv *ShardKV) copyShardCachedReplyChunk(chunk *ChunkedCachedReply) ChunkedCachedReply {
	newChunk := ChunkedCachedReply{
		Size:          chunk.Size,
		CachedReplies: make(map[int64]RequestReply),
	}
	for key, value := range chunk.CachedReplies {
		newChunk.CachedReplies[key] = value
	}
	return newChunk
}

// skv.mu and shardLocks[shard] are held
func (skv *ShardKV) forwardReceivingChunks(configNum int, shard int) {
	skv.moveShardDPrintf("forwardReceivingChunks() receives configNum: %v, shard: %v\n", configNum, shard)
	if configNum == skv.config.Num {
		skv.moveShardDPrintf("In forwardReceivingChunks() configNum: %v == skv.config.Num: %v, move from receiving to serve\n", configNum, skv.config.Num)
		skv.serveShardIDs[shard] = true
		skv.serveShards[shard] = skv.receivingShards[shard]
		skv.serveCachedReplies[shard] = skv.receivingCachedReplies[shard]
	} else if configNum > skv.config.Num {
		skv.moveShardDPrintf("In forwardReceivingChunks() configNum: %v > skv.config.Num: %v, move from receiving to future\n", configNum, skv.config.Num)
		skv.futureServeShards[shard] = skv.receivingShards[shard]
		skv.futureCachedReplies[shard] = skv.receivingCachedReplies[shard]
		skv.futureServeConfigNums[shard] = configNum
	} else {
		// configNum < skv.config.Num
		if skv.config.Shards[shard] == skv.gid {
			skv.moveShardDPrintf("In forwardReceivingChunks() configNum: %v < skv.config.Num: %v, but currently serve shard: %v, move from receiving to serve\n", configNum, skv.config.Num, shard)
			skv.serveShardIDs[shard] = true
			skv.serveShards[shard] = skv.receivingShards[shard]
			skv.serveCachedReplies[shard] = skv.receivingCachedReplies[shard]
		} else {
			skv.moveShardDPrintf("In forwardReceivingChunks() configNum: %v < skv.config.Num: %v, doesn't serve shard: %v, move from receiving to shadow\n", configNum, skv.config.Num, shard)
			// move shard to shadow
			skv.moveShardToShadow(shard, skv.receivingShards, skv.receivingCachedReplies)
		}
	}
	delete(skv.receivingShards, shard)
	delete(skv.receivingCachedReplies, shard)
	skv.moveShardDPrintf("forwardReceivingChunks() finishes configNum: %v, shard: %v\n", configNum, shard)
}

// skv.mu and shardLocks[shard] are held
// skv.shardLocks[shard] needs to be held
func (skv *ShardKV) moveShardToShadow(shard int, sourceShards map[int][]ChunkKVStore, sourceCachedReplies map[int][]ChunkedCachedReply) {
	skv.moveShardDPrintf("moveShardToShadow() receives shard: %v,\n sourceShards: %v,\n sourceCachedReplies: %v\n", shard, sourceShards, sourceCachedReplies)

	skv.transmitNum++
	targetGID := skv.config.Shards[shard]
	var group *ShadowShardGroup = nil
	skv.moveShardDPrintf("moveShardToShadow() before updating group for shard: %v: %v, shadowGroups: %v\n", shard, group, skv.shadowShardGroups)
	for index, tempGroup := range skv.shadowShardGroups {
		if tempGroup.TargetGID == targetGID {
			group = &(skv.shadowShardGroups[index])
			break
		}
	}
	skv.moveShardDPrintf("moveShardToShadow() gets nil group for shard? %v: %v: %v\n", group == nil, shard, group)
	if group == nil {
		servernames := make([]string, 0)
		servernames = append(servernames, skv.config.Groups[targetGID]...)
		newGroup := ShadowShardGroup{
			TargetGID:           targetGID,
			Servernames:         servernames,
			ShardIDs:            make([]int, 0),
			TransmitNums:        make([]int, 0),
			ConfigNums:          make([]int, 0),
			ShadowShards:        make([][]ChunkKVStore, 0),
			ShadowCachedReplies: make([][]ChunkedCachedReply, 0),
		}
		skv.shadowShardGroups = append(skv.shadowShardGroups, newGroup)
		group = &(skv.shadowShardGroups[len(skv.shadowShardGroups)-1])
	}
	skv.moveShardDPrintf("moveShardToShadow() gets group for shard: %v: %v\n", shard, group)
	group.ShardIDs = append(group.ShardIDs, shard)
	group.TransmitNums = append(group.TransmitNums, skv.transmitNum)
	group.ConfigNums = append(group.ConfigNums, skv.config.Num)
	group.ShadowShards = append(group.ShadowShards, sourceShards[shard])
	group.ShadowCachedReplies = append(group.ShadowCachedReplies, sourceCachedReplies[shard])
	skv.moveShardDPrintf("moveShardToShadow() updates group for shard: %v: %v, shadowGroups: %v\n", shard, group, skv.shadowShardGroups)
	delete(sourceShards, shard)
	delete(sourceCachedReplies, shard)
	skv.moveShardDPrintf("moveShardToShadow() finishes shard: %v,\n sourceShards: %v,\n sourceCachedReplies: %v\n", shard, sourceShards, sourceCachedReplies)
}

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
		newValue = prevValue + newValue
	}
	commandSize := unsafe.Sizeof(command.Key + newValue)
	for index, chunk := range skv.serveShards[command.Shard] {
		if chunk.Size+commandSize <= MAXSHARDCHUNKSIZE {
			targetChunk = &(skv.serveShards[command.Shard][index])
			break
		}
	}
	if targetChunk == nil {
		tempChunk := ChunkKVStore{
			Size:    0,
			KVStore: make(map[string]string),
		}
		skv.serveShards[command.Shard] = append(skv.serveShards[command.Shard], tempChunk)
		targetChunk = &(skv.serveShards[command.Shard][len(skv.serveShards[command.Shard])-1])
	}
	targetChunk.Size = targetChunk.Size + commandSize

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
}

/*
must be called with skv.mu.Lock
if skv.mu.Lock is held, no threads will make changes to these fields, no race condition
read is allowed
*/
func (skv *ShardKV) decodeSnapshot(snapshot []byte) {
	reader := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(reader)

	// shard -> key: value store
	var (
		config                shardController.Config
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

		controllerSeqNum int64
		transmitNum      int
	)

	if d.Decode(&config) != nil ||
		d.Decode(&serveShardIDs) != nil ||
		d.Decode(&serveShards) != nil ||
		d.Decode(&receivingShards) != nil ||
		d.Decode(&futureServeConfigNums) != nil ||
		d.Decode(&futureServeShards) != nil ||
		d.Decode(&shadowShardGroups) != nil ||
		d.Decode(&serveCachedReplies) != nil ||
		d.Decode(&receivingCachedReplies) != nil ||
		d.Decode(&futureCachedReplies) != nil ||
		d.Decode(&finishedTransmit) != nil ||
		d.Decode(&controllerSeqNum) != nil ||
		d.Decode(&transmitNum) != nil {
		log.Fatalf("Fatal: decoding error!\n")
	} else {
		skv.config = config
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
		skv.controllerSeqNum = controllerSeqNum
		skv.transmitNum = transmitNum

		skv.snapshotDPrintf(skv.leaderId, `
		decodeSnapshot(): \n
		skv.serveShardIDs: %v,\n
		skv.serveShards: %v,\n
		skv.receivingShards: %v,\n
		skv.futureServeConfigNums: %v,\n
		skv.shadowShardGroups: %v,\n
		skv.serveCachedReplies: %v,\n
		skv.receivingCachedReplies: %v,\n
		skv.futureCachedReplies: %v,\n
		skv.finishedTransmit: %v,\n
		skv.controllerSeqNum: %v,\n
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
			skv.controllerSeqNum,
			skv.transmitNum,
		)
	}
}

/*
must be called with skv.mu.Lock
if skv.mu.Lock is held, no threads will make changes to these fields, no race condition
read is allowed
*/
func (skv *ShardKV) encodeSnapshot() []byte {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	if e.Encode(skv.config) != nil ||
		e.Encode(skv.serveShardIDs) != nil ||
		e.Encode(skv.serveShards) != nil ||
		e.Encode(skv.receivingShards) != nil ||
		e.Encode(skv.futureServeConfigNums) != nil ||
		e.Encode(skv.futureServeShards) != nil ||
		e.Encode(skv.shadowShardGroups) != nil ||
		e.Encode(skv.serveCachedReplies) != nil ||
		e.Encode(skv.receivingCachedReplies) != nil ||
		e.Encode(skv.futureCachedReplies) != nil ||
		e.Encode(skv.finishedTransmit) != nil ||
		e.Encode(skv.controllerSeqNum) != nil ||
		e.Encode(skv.transmitNum) != nil {
		log.Fatalf("encoding error!\n")
	}
	skv.snapshotDPrintf(skv.leaderId, `
	encodeSnapshot(): \n
	skv.serveShardIDs: %v,\n
	skv.serveShards: %v,\n
	skv.receivingShards: %v,\n
	skv.futureServeConfigNums: %v,\n
	skv.shadowShardGroups: %v,\n
	skv.serveCachedReplies: %v,\n
	skv.receivingCachedReplies: %v,\n
	skv.futureCachedReplies: %v,\n
	skv.finishedTransmit: %v,\n
	skv.controllerSeqNum: %v,\n
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
		skv.controllerSeqNum,
		skv.transmitNum,
	)
	data := writer.Bytes()
	return data
}
