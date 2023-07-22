package shardkv

import (
	"fmt"
	"log"
	"sync/atomic"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardController"
)

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	TempDPrintf("Start ShardKV Server: me: %v, maxRaftState: %v, gid: %v\n", me, maxRaftState, gid)
	labgob.Register(ShardKVCommand{})
	labgob.Register(ConfigUpdateCommand{})
	labgob.Register(TransmitShardCommand{})

	skv := new(ShardKV)
	skv.me = me
	skv.leaderId = -1
	if maxRaftState != -1 {
		maxRaftState = 8 * maxRaftState
	}
	skv.maxRaftState = maxRaftState
	skv.make_end = make_end
	skv.gid = gid

	skv.controllerClerk = shardController.MakeQueryClerk(ctrlers, skv.gid, int64(skv.gid))
	skv.controllerSeqNum = 1

	skv.applyCh = make(chan raft.ApplyMsg)
	skv.rf = raft.Make(servers, me, persister, skv.applyCh, skv.maxRaftState, skv.gid)
	skv.serveShardIDs = make(map[int]bool)
	skv.serveShards = make(map[int][]ChunkKVStore)
	skv.receivingShards = make(map[int][]ChunkKVStore)
	skv.futureServeConfigNums = make(map[int]int)
	skv.futureServeShards = make(map[int][]ChunkKVStore)
	skv.shadowShardGroups = make([]ShadowShardGroup, 0)

	skv.serveCachedReplies = make(map[int][]ChunkedCachedReply)
	skv.receivingCachedReplies = make(map[int][]ChunkedCachedReply)
	skv.futureCachedReplies = make(map[int][]ChunkedCachedReply)

	skv.latestAppliedIndex = 0
	skv.latestAppliedTerm = 0

	skv.finishedTransmit = make(map[int]TransmitInfo)
	skv.onGoingTransmit = make(map[int]TransmitInfo)

	for i := 0; i < len(skv.clerkChans); i++ {
		skv.clerkChans[i] = make(map[int64]chan RequestReply)
	}

	go skv.commandExecutor()
	go skv.snapshotController()
	go skv.configChecker()
	go skv.shadowShardInspector()
	return skv
}

func (skv *ShardKV) RequestHandler(args *RequestArgs, reply *RequestReply) {
	skv.tempDPrintf("GID: %v, ShardKV: %v, RequestHandler() is called with %v\n", skv.gid, skv.me, args)
	if !skv.checkLeader(args, reply) {
		return
	}
	if skv.checkCachedReply(args, reply) {
		return
	}
	command := skv.getShardKVCommand(args, reply)
	if command == nil {
		return
	}
	skv.shardLocks[args.Shard].Lock()
	clerkChan := make(chan RequestReply)
	skv.clerkChans[args.Shard][args.ClerkId] = clerkChan
	skv.shardLocks[args.Shard].Unlock()
	skv.tempDPrintf("Got command: ClerkId=%v, SeqNum=%v, Key=%v, Value=%v, Operation=%v\n", command.ClerkId, command.SeqNum, command.Key, command.Value, command.Operation)
	_, _, succeeded := skv.startCommit(*command)
	if !succeeded {
		return
	}
	skv.waitReply(clerkChan, args, reply)

	skv.shardLocks[args.Shard].Lock()
	delete(skv.clerkChans[args.Shard], args.ClerkId)
	skv.shardLocks[args.Shard].Unlock()

	skv.tempDPrintf("RequestHandler() finishes with %v\n", reply)
}

func (skv *ShardKV) checkLeader(args *RequestArgs, reply *RequestReply) bool {
	votedFor := skv.rf.GetVotedFor()
	if votedFor != skv.me {
		skv.tempDPrintf("ShardKV: %v is not the leader. votedFor: %v\n", skv.me, votedFor)
		return false
	}
	return true
}

func (skv *ShardKV) checkCachedReply(args *RequestArgs, reply *RequestReply) bool {
	skv.lockMu("checkCachedReply() with args: %v\n", args)
	defer skv.unlockMu()
	// skv.shardLocks[args.Shard].Lock()
	// defer skv.shardLocks[args.Shard].Unlock()
	chunkedCachedReplies, exists := skv.serveCachedReplies[args.Shard]
	if !exists {
		return false
	}
	for _, chunk := range chunkedCachedReplies {
		cachedReply, exists := chunk.CachedReplies[args.ClerkId]
		if exists {
			if args.SeqNum == cachedReply.SeqNum {
				// the previous reply is lost
				skv.copyReply(&cachedReply, reply)
				skv.tempDPrintf("ShardKV: %v caches reply. Reply: %v\n", skv.me, cachedReply)
				return true
			}
			// args.SeqNum > cachedReply.SeqNum
			// not possible to be args.SeqNum < cachedReply.SeqNum
			return false
		}
	}
	return false
}

func (skv *ShardKV) getShardKVCommand(args *RequestArgs, reply *RequestReply) *ShardKVCommand {
	command := &ShardKVCommand{
		ClerkId:   args.ClerkId,
		SeqNum:    args.SeqNum,
		ConfigNum: args.ConfigNum,

		Shard:     args.Shard,
		Operation: args.Operation,
		Key:       args.Key,
		Value:     args.Value,
	}
	if unsafe.Sizeof(command) >= MAXKVCOMMANDSIZE {
		reply.SizeExceeded = true
		return nil
	}
	return command
}

func (skv *ShardKV) checkCommand(command interface{}) bool {
	_, isShardKVCommand := command.(ShardKVCommand)
	_, isMetaUpdateCommand := command.(ConfigUpdateCommand)
	_, isTransmitShardCommand := command.(TransmitShardCommand)
	if !isShardKVCommand && !isMetaUpdateCommand && !isTransmitShardCommand {
		return false
	}
	return true
}

func (skv *ShardKV) startCommit(command interface{}) (int, int, bool) {
	skv.tempDPrintf("startCommit receives command: %v\n", command)
	quit := false
	if !skv.checkCommand(command) {
		log.Fatalf("expecting command to be type of ShardKVCommand, ConfigUpdateCommand, TransmitShardCommand, RemoveShardCommand!")
	}
	var appendIndex, appendTerm int
	for !quit {
		index, term, isLeader := skv.rf.Start(command)
		skv.tempDPrintf("startCommit index: %v, term: %v, isLeader: %v, for Start command: %v\n", index, term, isLeader, command)
		if !isLeader {
			return -1, -1, false
		}
		if index > 0 {
			skv.tempDPrintf("Appended command: %v\n", command)
			appendIndex = index
			appendTerm = term
			quit = true
		} else {
			// the server is the leader, but log exceeds maxLogSize
			// retry later
			/*
				if the leader is isolated from the majority of the group, the client will only contact this server, and got stuck with this server.
				When this server re-joins the group, the client will give up this server and re-try other servers.
				The model is that:  the client always isolated with the server it can contact.
			*/
			time.Sleep(time.Duration(CHECKLOGTIMEOUT) * time.Millisecond)
			if skv.killed() {
				return -1, -1, false
			}
			skv.tempDPrintf("ShardKV: %v the leader can't add new command: %v, LeaderId: %v\n", skv.me, command, skv.me)
			skv.tempDPrintf("skv.me: %v, retry index: %v, on command: %v", skv.me, index, command)
		}
	}
	return appendIndex, appendTerm, true
}

func (skv *ShardKV) waitReply(clerkChan chan RequestReply, args *RequestArgs, reply *RequestReply) {
	quit := false
	for !quit {
		select {
		case tempReply := <-clerkChan:
			// drop reply for args.SeqNum > tempReply.SeqNum
			// it's possible when previous request is committed and server crashes before reply
			// the new leader just applies this command
			if tempReply.SeqNum == args.SeqNum {
				skv.copyReply(&tempReply, reply)
				quit = true
				skv.tempDPrintf("RequestHandler() succeeds with %v\n", reply)
			}
		case <-time.After(time.Duration(CHECKLEADERTIMEOUT) * time.Millisecond):
			isValidLeader := skv.rf.IsValidLeader()
			if skv.killed() || !isValidLeader {
				quit = true
			}
		}
	}
}

func (skv *ShardKV) copyReply(from *RequestReply, to *RequestReply) {
	to.ClerkId = from.ClerkId
	to.SeqNum = from.SeqNum

	to.ConfigNum = from.ConfigNum
	to.Shard = from.Shard

	to.Succeeded = from.Succeeded
	to.WrongGroup = from.WrongGroup
	to.WaitForUpdate = from.WaitForUpdate
	to.SizeExceeded = from.SizeExceeded
	to.Value = from.Value
	to.Exists = from.Exists
	to.ErrorMsg = from.ErrorMsg
}

// long-running thread for leader
func (skv *ShardKV) commandExecutor() {
	quit := false
	for !quit {
		select {
		case msg := <-skv.applyCh:
			if msg.SnapshotValid {
				skv.processSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				skv.processCommand(msg.CommandIndex, msg.CommandTerm, msg.Command)
			}
		case <-time.After(time.Duration(CHECKLEADERTIMEOUT) * time.Millisecond):
			if skv.killed() {
				quit = true
			}
		}
	}
}

func (skv *ShardKV) processCommand(commandIndex int, commandTerm int, commandFromRaft interface{}) {
	skv.tempDPrintf("ShardKV: %v, processCommand() is called with commandIndex: %v, commandTerm: %v, commandFromRaft: %v\n", skv.me, commandIndex, commandTerm, commandFromRaft)
	skv.lockMu("processCommand() with commandIndex: %v, commandTerm: %v, commandFromRaft: %v\n", commandIndex, commandTerm, commandFromRaft)
	defer skv.unlockMu()

	// check command index and term
	if commandIndex != skv.latestAppliedIndex+1 {
		log.Fatalf("Fatal: ShardKV: %v, expecting log index: %v, got %v", skv.me, skv.latestAppliedIndex+1, commandIndex)
	} else {
		skv.latestAppliedIndex++
	}
	if commandTerm < skv.latestAppliedTerm {
		log.Fatalf("Fatal: ShardKV: %v, expecting log term: >=%v, got %v", skv.me, skv.latestAppliedTerm, commandTerm)
	} else {
		skv.latestAppliedTerm = commandTerm
	}

	// check command is Noop
	noop, isNoop := commandFromRaft.(raft.Noop)
	if isNoop {
		skv.tempDPrintf("ShardKV %v receives noop: %v\n", skv.me, noop)
		return
	}

	// check command is ConfigUpdateCommand
	configUpdateCommand, isConfigUpdateCommand := commandFromRaft.(ConfigUpdateCommand)
	if isConfigUpdateCommand {
		skv.processConfigUpdate(configUpdateCommand)
		// no need to copy configUpdateCommand, it's read only
		return
	}

	// check command is TransmitShardCommand
	transmitShardCommand, isTransmitShardCommand := commandFromRaft.(TransmitShardCommand)
	if isTransmitShardCommand {
		// need to copy the maps inside
		skv.processTransmitShard(transmitShardCommand)
		return
	}

	// the command is from client
	clientCommand, isShardKVCommand := commandFromRaft.(ShardKVCommand)
	if !isShardKVCommand {
		log.Fatalf("ShardKV %v, expecting a ShardKVCommand: %v\n", skv.me, clientCommand)
	}
	skv.processClientRequest(clientCommand)
	skv.tempDPrintf("ShardKV: %v, processCommand() finishes\n", skv.me)
}

/*
skv.shardLocks[command.Shard].Lock() is held
and the skv serves command.Shard
*/
func (skv *ShardKV) processClientRequest(command ShardKVCommand) {
	skv.tempDPrintf("ShardKV: %v, processClientRequest() is called with command: %v\n", skv.me, command)
	// check if the current group serves command· or not
	if !skv.checkServing(&command) {
		return
	}

	if cachedReply := skv.checkCachedClientReplyForProcessing(&command); cachedReply != nil {
		go skv.sendReply(cachedReply)
		return
	}

	skv.moveShardDPrintf("processClientRequest() receives valid ShardKVCommand: %v\n", command)

	skv.shardLocks[command.Shard].Lock()
	defer skv.shardLocks[command.Shard].Unlock()

	var reply *RequestReply
	switch command.Operation {
	case GET:
		reply = skv.processGet(command)
	case PUT:
		reply = skv.processPut(command)
	case APPEND:
		reply = skv.processAppend(command)
	default:
		log.Fatalf("ShardKV: %v, Got unsupported operation: %v", skv.me, command.Operation)
	}

	// caching the latest reply for each client
	// skv.cachedReplies[reply.ClerkId].SeqNum < reply.SeqNum
	skv.cacheClientRequestReply(&command, reply)
	skv.tempDPrintf("ShardKV: %v, processClientRequest() finishes with reply: %v\n", skv.me, reply)
	go skv.sendReply(reply)
}

func (skv *ShardKV) checkCachedClientReplyForProcessing(command *ShardKVCommand) *RequestReply {
	chunkedCachedReplies, exists := skv.serveCachedReplies[command.Shard]
	if !exists {
		return nil
	}
	for _, chunk := range chunkedCachedReplies {
		cachedReply, exists := chunk.CachedReplies[command.ClerkId]
		if exists {
			if cachedReply.SeqNum >= command.SeqNum {
				skv.tempDPrintf(" %v the command has cachedReply: %v\n", command, cachedReply)
				return &cachedReply
			}
			return nil
		}
	}
	return nil
}

func (skv *ShardKV) cacheClientRequestReply(command *ShardKVCommand, reply *RequestReply) {
	for _, chunk := range skv.serveCachedReplies[command.Shard] {
		if _, exists := chunk.CachedReplies[command.ClerkId]; exists {
			chunk.Size -= unsafe.Sizeof(chunk.CachedReplies[command.ClerkId]) - unsafe.Sizeof(command.ClerkId)
			delete(chunk.CachedReplies, command.ClerkId)
			break
		}
	}

	var targetChunk *ChunkedCachedReply = nil
	requestSize := unsafe.Sizeof(command.ClerkId) + unsafe.Sizeof(reply)
	for _, chunk := range skv.serveCachedReplies[command.Shard] {
		if chunk.Size+requestSize <= MAXSHARDCHUNKSIZE {
			targetChunk = &chunk
			break
		}
	}
	if targetChunk == nil {
		targetChunk = &ChunkedCachedReply{
			Size:          0,
			CachedReplies: make(map[int64]RequestReply),
		}
		skv.serveCachedReplies[command.Shard] = append(skv.serveCachedReplies[command.Shard], *targetChunk)
	}
	targetChunk.Size += requestSize

	targetChunk.CachedReplies[command.ClerkId] = *reply
}

func (skv *ShardKV) checkServing(command *ShardKVCommand) bool {
	var reply *RequestReply
	// check if current config serves command.Shard
	if skv.config.Shards[command.Shard] != skv.gid {
		// current config doesn't serve command.Shard
		// reply.Succeeded = false: the command is committed but not applied successfully
		if skv.config.Num < command.ConfigNum {
			reply = skv.getFakeReply(command)
			reply.WaitForUpdate = true
			reply.ErrorMsg = fmt.Sprintf("skv.config.Num: %v < command.ConfigNum: %v", skv.config.Num, command.ConfigNum)
		} else {
			// skv.config.Num > command.Shard
			reply = skv.getFakeReply(command)
			reply.WrongGroup = true
		}
		skv.tempDPrintf(" case 1 doesn't serve command: %v,\n skv.config: %v, \n fakeReply: %v\n", command, skv.config, reply)
		go skv.sendReply(reply)
		return false
	}

	// current config serves command.Shard
	// but serveMap may have not received the shard yet
	if _, exists := skv.serveShardIDs[command.Shard]; !exists {
		reply = skv.getFakeReply(command)
		reply.WaitForUpdate = true
		reply.ErrorMsg = "current config serves command.Shard, but serveMap may have not received the shard yet"
		skv.tempDPrintf("WaitForUpdate: skv.config: %v, skv.serveShardIDs: %v, ", skv.config, skv.serveShardIDs)
		go skv.sendReply(reply)
		skv.tempDPrintf(" case 2 doesn't serve command: %v,\n skv.config: %v, \n fakeReply: %v\n", command, skv.config, reply)
		go skv.sendReply(reply)
		return false
	}
	skv.tempDPrintf(" serves command: %v,\n skv.config: %v\n", command, skv.config)
	return true
}

func (skv *ShardKV) getFakeReply(command *ShardKVCommand) *RequestReply {
	reply := RequestReply{
		ClerkId:   command.ClerkId,
		SeqNum:    command.SeqNum,
		ConfigNum: command.ConfigNum,
		Shard:     command.Shard,
	}
	return &reply
}

func (skv *ShardKV) sendReply(reply *RequestReply) {
	skv.tempDPrintf("ShardKV: %v, sends to: %v reply %v \n", skv.me, reply, reply.ClerkId)

	// check if the server is leader or not
	isValidLeader := skv.rf.IsValidLeader()
	if skv.killed() || !isValidLeader {
		return
	}

	skv.shardLocks[reply.Shard].Lock()
	clerkChan, exists := skv.clerkChans[reply.Shard][reply.ClerkId]
	if !exists {
		// the clerk is not waiting for the reply on this server
		skv.shardLocks[reply.Shard].Unlock()
		return
	}
	skv.shardLocks[reply.Shard].Unlock()
	quit := false
	for !quit {
		select {
		case clerkChan <- *reply:
		case <-time.After(time.Duration(CHECKLEADERTIMEOUT) * time.Millisecond):
			isValidLeader := skv.rf.IsValidLeader()
			if skv.killed() || !isValidLeader {
				quit = true
			}
		}
	}
}

func (skv *ShardKV) snapshotController() {
	quit := false
	for !quit {
		select {
		case <-skv.rf.SignalSnapshot:
			skv.lockMu("snapshotController()\n")
			data := skv.encodeSnapshot()
			snapshot := raft.SnapshotInfo{
				Data:              data,
				LastIncludedIndex: skv.latestAppliedIndex,
			}
			skv.unlockMu()
			quit1 := false
			for !quit1 {
				select {
				case skv.rf.SnapshotChan <- snapshot:
					quit1 = true
				case <-time.After(time.Duration(CHECKLEADERTIMEOUT) * time.Millisecond):
					if skv.killed() {
						quit1 = true
						quit = true
					}
				}
			}
		case <-time.After(time.Duration(CHECKLEADERTIMEOUT) * time.Millisecond):
			if skv.killed() {
				quit = true
			}
		}
	}
}

/*
every CHECKCONFIGTIMEOUT, this thread issues a Query(-1)
if the resulting Config has larger configNum than skv.config
then issues a MetaUpdateCommand
*/
func (skv *ShardKV) configChecker() {
	skv.tempDPrintf("configChecker is running...")
	for !skv.killed() {
		time.Sleep(time.Duration(CHECKCONFIGTIMEOUT) * time.Millisecond)
		// skv.tempDPrintf("configChecker sends query...\n")
		isValidLeader := skv.rf.IsValidLeader()
		if !isValidLeader {
			continue
		}
		// no need to lock,
		// seqNum is incremented when applying, the controller should not cache the query
		skv.lockMu("1 configChecker()\n")
		skv.leaderId = skv.me
		controllerSeqNum := skv.controllerSeqNum
		skv.unlockMu()
		newConfig := skv.controllerClerk.QueryWithSeqNum(-1, controllerSeqNum)
		skv.lockMu("2 configChecker()\n")
		// skv.tempDPrintf("configChecker queries newConfig: %v, old config: %v\n", newConfig, skv.config)
		if newConfig.Num > skv.config.Num {
			// skv.tempDPrintf("configChecker gets newConfig: %v, old config: %v\n", newConfig, skv.config)
			// issue a command
			command := ConfigUpdateCommand{
				Operation: UPDATECONFIG,
				Config:    newConfig,
			}
			skv.unlockMu()
			if unsafe.Sizeof(command) >= MAXKVCOMMANDSIZE {
				log.Fatalf("Fatal: command is too large, max allowed command size is %v\n", MAXKVCOMMANDSIZE)
			}
			skv.tempDPrintf("configChecker for newConfig: %v and issues ConfigUpdateCommand: %v\n", newConfig, command)
			skv.startCommit(command)
		} else {
			skv.unlockMu()
		}
	}
}

/*
the shards moved to the same group needs to be in order
*/
func (skv *ShardKV) shadowShardInspector() {
	// inspect all shadowed groups and initiate moveShard command
	skv.moveShardDPrintf("shadowShardInspector is running...")
	quit := false
	for !quit {
		time.Sleep(time.Duration(INSPECTSHADOWTIMEOUT) * time.Millisecond)
		skv.lockMu("shadowShardInspector()\n")
		for index := range skv.shadowShardGroups {
			if !skv.shadowShardGroups[index].Processing {
				skv.moveShardDPrintf("shadowShardInspector gets a shardGroup with no thread processing: %v\n", skv.shadowShardGroups[index])
				go skv.transmitToGroup(index)
			}
		}

		if skv.killed() {
			quit = true
		}
		skv.unlockMu()
	}
	skv.moveShardDPrintf("shadowShardInspector quit!")
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (skv *ShardKV) Kill() {
	atomic.StoreInt32(&skv.dead, 1)
	skv.rf.Kill()
}

func (skv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&skv.dead)
	return z == 1
}
