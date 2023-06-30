package kvraft

import (
	"log"
	"sync"
	"sync/atomic"

	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
)

func (kv *KVServer) RequestHandler(args *RequestArgs, reply *RequestReply) {
	TempDPrintf("RequestHandler() is called with %v\n", args)
	leaderId := kv.rf.GetLeaderId()
	if leaderId != kv.me {
		reply.LeaderId = leaderId
		return
	}
	kv.mu.Lock()
	cachedReply := kv.cachedReplies[args.ClerkId]
	if args.SeqNum == cachedReply.SeqNum {
		// the previous reply is lost
		kv.copyReply(&cachedReply, reply)
		kv.mu.Unlock()
		return
	}

	clerkChan := make(chan RequestReply)
	kv.clerkChans[args.ClerkId] = clerkChan
	kv.mu.Unlock()

	defer kv.mu.Unlock()
	defer delete(kv.clerkChans, args.ClerkId)
	defer kv.mu.Lock()

	_, _, isLeader := kv.commitRequest(args)
	if !isLeader {
		reply.LeaderId = -1
		return
	}

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
			}
		case <-kv.rf.SignalKilled:
			quit = true
			reply.LeaderId = -1
		case <-kv.rf.SignalDemotion:
			quit = true
			reply.LeaderId = -1
		case <-kv.SignalKilled:
			quit = true
			reply.LeaderId = -1
		}
	}
	TempDPrintf("RequestHandler() finishes with %v\n", reply)
}

func (kv *KVServer) copyReply(from *RequestReply, to *RequestReply) {
	to.ClerkId = from.ClerkId
	to.SeqNum = from.SeqNum
	to.LeaderId = from.LeaderId

	to.Succeeded = from.Succeeded
	to.Value = from.Value
	to.Exists = from.Exists
}

func (kv *KVServer) commitRequest(args *RequestArgs) (int, int, bool) {
	command := KVCommand{
		ClerkId:   args.ClerkId,
		SeqNum:    args.SeqNum,
		Key:       args.Key,
		Value:     args.Value,
		Operation: args.Operation,
	}
	index, term, isLeader := kv.rf.Start(command)
	return index, term, isLeader
}

// long-running thread for leader
func (kv *KVServer) commandExecutor() {
	for msg := range kv.applyCh {
		if msg.SnapshotValid {
			kv.processSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
		} else {
			kv.processCommand(msg.CommandIndex, msg.CommandTerm, msg.Command)
		}
	}
}

func (kv *KVServer) processSnapshot(snapshotIndex int, snapshotTerm int, snapshot []byte) {
	log.Fatalf("No implementation for processSnapshot()!")
}

func (kv *KVServer) processCommand(commandIndex int, commandTerm int, commandFromRaft interface{}) {
	TempDPrintf("processCommand() is called with commandIndex: %v, commandTerm: %v, commandFromRaft: %v\n", commandIndex, commandTerm, commandFromRaft)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if commandIndex != kv.latestAppliedIndex+1 {
		log.Fatalf("expecting log index: %v, got %v", kv.latestAppliedIndex+1, commandIndex)
	} else {
		kv.latestAppliedIndex++
	}
	if commandTerm < kv.latestAppliedTerm {
		log.Fatalf("expecting log term: >=%v, got %v", kv.latestAppliedTerm, commandTerm)
	} else {
		kv.latestAppliedTerm = commandTerm
	}

	var reply *RequestReply
	command := KVCommand(commandFromRaft.(KVCommand))
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
		log.Fatalf("Got unsupported operation: %v", command.Operation)
	}
	// caching the latest reply for each client
	// kv.cachedReplies[reply.ClerkId].SeqNum < reply.SeqNum
	kv.cachedReplies[reply.ClerkId] = *reply
	TempDPrintf("processCommand() finishes with reply: %v\n", reply)
	go func(reply *RequestReply) {
		TempDPrintf("reply %v sends to: %v\n", reply, reply.ClerkId)
		kv.mu.Lock()
		clerkChan := kv.clerkChans[reply.ClerkId]
		kv.mu.Unlock()
		select {
		case clerkChan <- *reply:
		case <-kv.rf.SignalKilled:
		case <-kv.rf.SignalDemotion:
		case <-kv.SignalKilled:
			TempDPrintf("reply %v sended to: %v\n", reply, reply.ClerkId)
		}
	}(reply)

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(KVCommand{})

	kv := new(KVServer)
	kv.cond = sync.NewCond(&kv.mu)
	kv.me = me
	kv.SignalKilled = make(chan int)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.maxraftstate = maxraftstate

	kv.latestAppliedIndex = 0
	kv.latestAppliedTerm = 0
	kv.kvStore = make(map[string]string)
	kv.cachedReplies = make(map[int64]RequestReply)
	kv.clerkChans = make(map[int64]chan RequestReply)

	go kv.commandExecutor()

	return kv
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
	close(kv.SignalKilled)
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
