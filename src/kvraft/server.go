package kvraft

import (
	"bytes"
	"log"
	"sync/atomic"
	"time"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func (kv *KVServer) RequestHandler(args *RequestArgs, reply *RequestReply) {
	TempDPrintf("KVServer: %v, RequestHandler() is called with %v\n", kv.me, args)
	if !kv.checkLeader(args, reply) {
		return
	}
	if kv.checkCachedReply(args, reply) {
		return
	}
	command := kv.getKVCommand(args, reply)
	if command == nil {
		return
	}
	kv.mu.Lock()
	clerkChan := make(chan RequestReply)
	kv.clerkChans[args.ClerkId] = clerkChan
	kv.mu.Unlock()
	kv.kvStoreDPrintf("Got command: ClerkId=%v, SeqNum=%v, Key=%v, Value=%v, Operation=%v\n", command.ClerkId, command.SeqNum, command.Key, command.Value, command.Operation)
	if !kv.startCommit(command, reply) {
		return
	}
	kv.waitReply(clerkChan, args, reply)
	kv.mu.Lock()
	delete(kv.clerkChans, args.ClerkId)
	kv.mu.Unlock()
	TempDPrintf("RequestHandler() finishes with %v\n", reply)
}

func (kv *KVServer) checkLeader(args *RequestArgs, reply *RequestReply) bool {
	votedFor := kv.rf.GetVotedFor()
	if votedFor != kv.me {
		TempDPrintf("KVServer: %v is not the leader. votedFor: %v\n", kv.me, votedFor)
		reply.LeaderId = votedFor
		return false
	}
	return true
}

func (kv *KVServer) checkCachedReply(args *RequestArgs, reply *RequestReply) bool {
	kv.mu.Lock()
	cachedReply := kv.cachedReplies[args.ClerkId]
	if args.SeqNum == cachedReply.SeqNum {
		// the previous reply is lost
		kv.copyReply(&cachedReply, reply)
		kv.mu.Unlock()
		TempDPrintf("KVServer: %v caches reply. Reply: %v\n", kv.me, cachedReply)
		return true
	}
	kv.mu.Unlock()
	return false
}

func (kv *KVServer) getKVCommand(args *RequestArgs, reply *RequestReply) *KVCommand {
	command := &KVCommand{
		ClerkId:   args.ClerkId,
		SeqNum:    args.SeqNum,
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Operation,
	}
	if unsafe.Sizeof(command) >= MAXKVCOMMANDSIZE {
		reply.SizeExceeded = true
		reply.LeaderId = -1
		return nil
	}
	return command
}

func (kv *KVServer) startCommit(command *KVCommand, reply *RequestReply) bool {
	quit := false
	for !quit {
		index, _, isLeader := kv.rf.Start(*command)
		if !isLeader {
			reply.LeaderId = -1
			return false
		}
		if index > 0 {
			kv.kvStoreDPrintf("Appended command: ClerkId=%v, SeqNum=%v, Key=%v, Value=%v, Operation=%v\n", command.ClerkId, command.SeqNum, command.Key, command.Value, command.Operation)
			quit = true
		} else {
			// the server is the leader, but log exceeds maxLogSize
			// retry later
			time.Sleep(time.Duration(CHECKTIMEOUT) * time.Millisecond)
			if kv.killed() {
				reply.LeaderId = -1
				return false
			}
			TempDPrintf("KVServer: %v the leader can't add new command: %v, LeaderId: %v\n", kv.me, command, kv.me)
			kv.kvStoreDPrintf("kv.me: %v, retry index: %v, on command: %v", kv.me, index, command)
		}
	}
	return true
}

func (kv *KVServer) waitReply(clerkChan chan RequestReply, args *RequestArgs, reply *RequestReply) {
	quit := false
	for !quit {
		select {
		case tempReply := <-clerkChan:
			// drop reply for args.SeqNum > tempReply.SeqNum
			// it's possible when previous request is committed and server crashes before reply
			// the new leader just applies this command
			if tempReply.SeqNum == args.SeqNum {
				kv.copyReply(&tempReply, reply)
				quit = true
				TempDPrintf("RequestHandler() succeeds with %v\n", reply)
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			isValidLeader := kv.rf.IsValidLeader()
			if kv.killed() || !isValidLeader {
				quit = true
				reply.LeaderId = -1
			}
		}
	}
}

func (kv *KVServer) copyReply(from *RequestReply, to *RequestReply) {
	to.ClerkId = from.ClerkId
	to.SeqNum = from.SeqNum
	to.LeaderId = from.LeaderId

	to.Succeeded = from.Succeeded
	to.Value = from.Value
	to.Exists = from.Exists
	to.SizeExceeded = from.SizeExceeded
}

// long-running thread for leader
func (kv *KVServer) commandExecutor() {
	quit := false
	for !quit {
		select {
		case msg := <-kv.applyCh:
			if msg.SnapshotValid {
				kv.processSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				kv.processCommand(msg.CommandIndex, msg.CommandTerm, msg.Command)
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			if kv.killed() {
				quit = true
			}
		}
	}
}

/*
processSnapshot will only called in follower
or at the start of a new leader
for both cases, kv.clerkChans will be empty
*/
func (kv *KVServer) processSnapshot(snapshotIndex int, snapshotTerm int, snapshot []byte) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.decodeSnapshot(snapshot)
	kv.latestAppliedIndex = snapshotIndex
	kv.latestAppliedTerm = snapshotTerm
}

func (kv *KVServer) processCommand(commandIndex int, commandTerm int, commandFromRaft interface{}) {
	TempDPrintf("KVServer: %v, processCommand() is called with commandIndex: %v, commandTerm: %v, commandFromRaft: %v\n", kv.me, commandIndex, commandTerm, commandFromRaft)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if commandIndex != kv.latestAppliedIndex+1 {
		log.Fatalf("KVServer: %v, expecting log index: %v, got %v", kv.me, kv.latestAppliedIndex+1, commandIndex)
	} else {
		kv.latestAppliedIndex++
	}
	if commandTerm < kv.latestAppliedTerm {
		log.Fatalf("KVServer: %v, expecting log term: >=%v, got %v", kv.me, kv.latestAppliedTerm, commandTerm)
	} else {
		kv.latestAppliedTerm = commandTerm
	}

	var reply *RequestReply
	noop, isNoop := commandFromRaft.(raft.Noop)
	if isNoop {
		TempDPrintf("KVServer %v receives noop: %v\n", kv.me, noop)
		return
	}
	command, isKVCommand := commandFromRaft.(KVCommand)
	if !isKVCommand {
		log.Fatalf("KVServer %v, expecting a KVCommand: %v\n", kv.me, command)
	}
	if kv.cachedReplies[command.ClerkId].SeqNum >= command.SeqNum {
		// drop the duplicated commands
		return
	}
	switch command.Operation {
	case GET:
		reply = kv.processGet(command)
	case PUT:
		reply = kv.processPut(command)
	case APPEND:
		reply = kv.processAppend(command)
	default:
		log.Fatalf("KVServer: %v, Got unsupported operation: %v", kv.me, command.Operation)
	}
	// caching the latest reply for each client
	// kv.cachedReplies[reply.ClerkId].SeqNum < reply.SeqNum
	kv.cachedReplies[reply.ClerkId] = *reply
	TempDPrintf("KVServer: %v, processCommand() finishes with reply: %v\n", kv.me, reply)
	go kv.sendReply(reply)
}

func (kv *KVServer) sendReply(reply *RequestReply) {
	TempDPrintf("KVServer: %v, sends to: %v reply %v \n", kv.me, reply, reply.ClerkId)
	kv.mu.Lock()
	clerkChan := kv.clerkChans[reply.ClerkId]
	kv.mu.Unlock()
	quit := false
	for !quit {
		select {
		case clerkChan <- *reply:
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			isValidLeader := kv.rf.IsValidLeader()
			if kv.killed() || !isValidLeader {
				quit = true
			}
		}
	}
}

func (kv *KVServer) processGet(command KVCommand) *RequestReply {
	value, exists := kv.kvStore[command.Key]
	reply := &RequestReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: kv.me,

		Succeeded: true,
		Value:     value,
		Exists:    exists,
	}
	return reply
}

func (kv *KVServer) processPut(command KVCommand) *RequestReply {
	kv.kvStore[command.Key] = command.Value
	reply := &RequestReply{
		ClerkId:   command.ClerkId,
		SeqNum:    command.SeqNum,
		LeaderId:  kv.me,
		Succeeded: true,
	}
	return reply
}

func (kv *KVServer) processAppend(command KVCommand) *RequestReply {
	value, exists := kv.kvStore[command.Key]
	kv.kvStore[command.Key] = value + command.Value
	reply := &RequestReply{
		ClerkId:  command.ClerkId,
		SeqNum:   command.SeqNum,
		LeaderId: kv.me,

		Succeeded: true,
		Value:     value,
		Exists:    exists,
	}
	return reply
}

// func (kv *KVServer) fakeReply() *RequestReply {
// 	return &RequestReply{}
// }

/*
servers[] contains the ports of the set of
servers that will cooperate via Raft to
form the fault-tolerant key/value service.
me is the index of the current server in servers[].
the k/v server should store snapshots through the underlying Raft
implementation, which should call persister.SaveStateAndSnapshot() to atomically save the Raft state along with the snapshot.
the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes, in order to allow Raft to garbage-collect its log. if maxraftstate is -1, you don't need to snapshot.
StartKVServer() must return quickly, so it should start goroutines for any long-running work.
*/
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(KVCommand{})

	kv := new(KVServer)
	kv.me = me

	// command size checking will not be disabled
	if maxRaftState != -1 {
		maxRaftState = 8 * maxRaftState
	}
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh, maxRaftState)
	kv.maxRaftState = maxRaftState

	kv.latestAppliedIndex = 0
	kv.latestAppliedTerm = 0
	kv.kvStore = make(map[string]string)
	kv.cachedReplies = make(map[int64]RequestReply)
	kv.clerkChans = make(map[int64]chan RequestReply)

	go kv.commandExecutor()
	go kv.snapshotController()

	return kv
}

func (kv *KVServer) snapshotController() {
	quit := false
	for !quit {
		select {
		case <-kv.rf.SignalSnapshot:
			kv.mu.Lock()
			data := kv.encodeSnapshot()
			snapshot := raft.SnapshotInfo{
				Data:              data,
				LastIncludedIndex: kv.latestAppliedIndex,
			}
			kv.mu.Unlock()
			quit1 := false
			for !quit1 {
				select {
				case kv.rf.SnapshotChan <- snapshot:
					quit1 = true
				case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
					if kv.killed() {
						quit1 = true
						quit = true
					}
				}
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			if kv.killed() {
				quit = true
			}
		}
	}
}

/*
must be called with kv.mu.Lock
*/
func (kv *KVServer) decodeSnapshot(snapshot []byte) {
	reader := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(reader)

	var maxRaftState int
	var kvStore map[string]string
	var cachedReplies map[int64]RequestReply

	if d.Decode(&maxRaftState) != nil ||
		d.Decode(&kvStore) != nil ||
		d.Decode(&cachedReplies) != nil {
		log.Fatalf("decoding error!\n")
	} else {
		kv.maxRaftState = maxRaftState
		kv.kvStore = kvStore
		kv.cachedReplies = cachedReplies
	}
}

/*
must be called with kv.mu.Lock
*/
func (kv *KVServer) encodeSnapshot() []byte {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	if e.Encode(kv.maxRaftState) != nil ||
		// e.Encode(kv.latestAppliedIndex) != nil ||
		// e.Encode(kv.latestAppliedTerm) != nil ||
		e.Encode(kv.kvStore) != nil ||
		e.Encode(kv.cachedReplies) != nil {
		log.Fatalf("Fatal: encodeSnapshot() in kvraft encoding error!\n")
	}
	data := writer.Bytes()
	return data
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
