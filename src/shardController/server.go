package shardController

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

func (sc *ShardController) RequestHandler(args *ControllerRequestArgs, reply *ControllerReply) {
	TempDPrintf("ShardController: %v, RequestHandler() is called with %v\n", sc.me, args)
	if !sc.checkLeader(args, reply) {
		return
	}
	if sc.checkCachedReply(args, reply) {
		return
	}
	command := sc.getControllerCommand(args, reply)
	if command == nil {
		return
	}
	sc.mu.Lock()
	clerkChan := make(chan ControllerReply)
	sc.clerkChans[args.ClerkId] = clerkChan
	sc.mu.Unlock()
	TempDPrintf("ShardController: %v, Got command: ClerkId=%v, SeqNum=%v Operation=%v, Command: %v\n", sc.me, command.ClerkId, command.SeqNum, command.Operation, command)
	if !sc.startCommit(command, reply) {
		return
	}
	sc.waitReply(clerkChan, args, reply)
	sc.mu.Lock()
	delete(sc.clerkChans, args.ClerkId)
	sc.mu.Unlock()
	TempDPrintf("ShardController: %v, RequestHandler() finishes with %v\n", sc.me, reply)
}

func (sc *ShardController) checkLeader(args *ControllerRequestArgs, reply *ControllerReply) bool {
	leaderId, votedFor, term := sc.rf.GetLeaderId()
	if votedFor != sc.me {
		TempDPrintf("ShardController: %v is not the leader. LeaderId: %v, votedFor: %v, term: %v\n", sc.me, leaderId, votedFor, term)
		reply.LeaderId = votedFor
		return false
	}
	return true
}

func (sc *ShardController) checkCachedReply(args *ControllerRequestArgs, reply *ControllerReply) bool {
	sc.mu.Lock()
	cachedReply := sc.cachedReplies[args.ClerkId]
	if args.SeqNum == cachedReply.SeqNum {
		// the previous reply is lost
		sc.copyReply(&cachedReply, reply)
		sc.mu.Unlock()
		TempDPrintf("ShardController: %v caches reply. Reply: %v\n", sc.me, cachedReply)
		return true
	}
	sc.mu.Unlock()
	return false
}

func (sc *ShardController) getControllerCommand(args *ControllerRequestArgs, reply *ControllerReply) *ControllerCommand {
	command := &ControllerCommand{
		ClerkId:      args.ClerkId,
		SeqNum:       args.SeqNum,
		Operation:    args.Operation,
		JoinedGroups: args.JoinedGroups,
		LeaveGIDs:    args.LeaveGIDs,
		MovedShard:   args.MovedShard,
		MovedGID:     args.MovedGID,
		QueryNum:     args.QueryNum,
	}
	if unsafe.Sizeof(command) >= MAXCONTROLLERCOMMANDSIZE {
		reply.SizeExceeded = true
		reply.LeaderId = -1
		return nil
	}
	return command
}

func (sc *ShardController) startCommit(command *ControllerCommand, reply *ControllerReply) bool {
	quit := false
	for !quit {
		index, _, isLeader := sc.rf.Start(*command)
		if !isLeader {
			reply.LeaderId = -1
			return false
		}
		if index > 0 {
			TempDPrintf("ShardController: %v, Appended command: ClerkId=%v, SeqNum=%v Operation=%v, Command: %v\n", sc.me, command.ClerkId, command.SeqNum, command.Operation, command)
			quit = true
		} else {
			// the server is the leader, but log exceeds maxLogSize
			// retry later
			time.Sleep(time.Duration(CHECKTIMEOUT) * time.Millisecond)
			if sc.killed() {
				reply.LeaderId = -1
				return false
			}
			TempDPrintf("ShardController: %v the leader can't add new command: %v, LeaderId: %v\n", sc.me, command, sc.me)
			TempDPrintf("sc.me: %v, retry index: %v, on command: %v", sc.me, index, command)
		}
	}
	return true
}

func (sc *ShardController) waitReply(clerkChan chan ControllerReply, args *ControllerRequestArgs, reply *ControllerReply) {
	quit := false
	for !quit {
		select {
		case tempReply := <-clerkChan:
			// drop reply for args.SeqNum > tempReply.SeqNum
			// it's possible when previous request is committed and server crashes before reply
			// the new leader just applies this command
			if tempReply.SeqNum == args.SeqNum {
				sc.copyReply(&tempReply, reply)
				quit = true
				TempDPrintf("ShardController: %v, RequestHandler() succeeds with %v\n", sc.me, reply)
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			isValidLeader := sc.rf.IsValidLeader()
			if sc.killed() || !isValidLeader {
				quit = true
				reply.LeaderId = -1
			}
		}
	}
}

func (sc *ShardController) copyReply(from *ControllerReply, to *ControllerReply) {
	to.ClerkId = from.ClerkId
	to.SeqNum = from.SeqNum
	to.LeaderId = from.LeaderId

	to.Succeeded = from.Succeeded
	to.SizeExceeded = from.SizeExceeded
	to.Config = from.Config
}

// long-running thread for leader
func (sc *ShardController) commandExecutor() {
	TempDPrintf("ShardController: %v commandExecutor() is running...\n", sc.me)
	quit := false
	for !quit {
		select {
		case msg := <-sc.applyCh:
			TempDPrintf("ShardController: %v commandExecutor() got a msg: %v\n", sc.me, msg)
			if msg.SnapshotValid {
				sc.processSnapshot(msg.SnapshotIndex, msg.SnapshotTerm, msg.Snapshot)
			} else {
				sc.processCommand(msg.CommandIndex, msg.CommandTerm, msg.Command)
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			if sc.killed() {
				quit = true
			}
		}
	}
}

/*
processSnapshot will only called in follower
or at the start of a new leader
for both cases, sc.clerkChans will be empty
*/
func (sc *ShardController) processSnapshot(snapshotIndex int, snapshotTerm int, snapshot []byte) {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	sc.decodeSnapshot(snapshot)
	sc.latestAppliedIndex = snapshotIndex
	sc.latestAppliedTerm = snapshotTerm
}

func (sc *ShardController) processCommand(commandIndex int, commandTerm int, commandFromRaft interface{}) {
	TempDPrintf("ShardController %v, processCommand() is called with commandIndex: %v, commandTerm: %v, commandFromRaft: %v\n", sc.me, commandIndex, commandTerm, commandFromRaft)
	sc.mu.Lock()
	defer sc.mu.Unlock()
	if commandIndex != sc.latestAppliedIndex+1 {
		log.Fatalf("ShardController %v, expecting log index: %v, got %v", sc.me, sc.latestAppliedIndex+1, commandIndex)
	} else {
		sc.latestAppliedIndex++
	}
	if commandTerm < sc.latestAppliedTerm {
		log.Fatalf("ShardController %v, expecting log term: >=%v, got %v", sc.me, sc.latestAppliedTerm, commandTerm)
	} else {
		sc.latestAppliedTerm = commandTerm
	}

	var reply *ControllerReply
	noop, isNoop := commandFromRaft.(raft.Noop)
	if isNoop {
		TempDPrintf("ShardController %v receives noop: %v\n", sc.me, noop)
		return
	}
	command, isControllerCommand := commandFromRaft.(ControllerCommand)
	if !isControllerCommand {
		log.Fatalf("ShardController %v, expecting a ControllerCommand: %v\n", sc.me, command)
	}
	if sc.cachedReplies[command.ClerkId].SeqNum >= command.SeqNum {
		// drop the duplicated commands
		return
	}
	switch command.Operation {
	case JOIN:
		reply = sc.processJoin(command)
	case LEAVE:
		reply = sc.processLeave(command)
	case MOVE:
		reply = sc.processMove(command)
	case QUERY:
		reply = sc.processQuery(command)
	default:
		log.Fatalf("ShardController %v, Got unsupported operation: %v", sc.me, command.Operation)
	}
	// caching the latest reply for each client
	// sc.cachedReplies[reply.ClerkId].SeqNum < reply.SeqNum
	sc.cachedReplies[reply.ClerkId] = *reply
	TempDPrintf("ShardController %v, processCommand() finishes with reply: %v\n", sc.me, reply)
	go sc.sendReply(reply)
}

func (sc *ShardController) sendReply(reply *ControllerReply) {
	TempDPrintf("ShardController %v, sends to: %v reply %v \n", sc.me, reply, reply.ClerkId)
	sc.mu.Lock()
	clerkChan := sc.clerkChans[reply.ClerkId]
	sc.mu.Unlock()
	quit := false
	for !quit {
		select {
		case clerkChan <- *reply:
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			isValidLeader := sc.rf.IsValidLeader()
			if sc.killed() || !isValidLeader {
				quit = true
			}
		}
	}
}

func (sc *ShardController) snapshotController() {
	quit := false
	for !quit {
		select {
		case <-sc.rf.SignalSnapshot:
			sc.mu.Lock()
			data := sc.encodeSnapshot()
			snapshot := raft.SnapshotInfo{
				Data:              data,
				LastIncludedIndex: sc.latestAppliedIndex,
			}
			sc.mu.Unlock()
			quit1 := false
			for !quit1 {
				select {
				case sc.rf.SnapshotChan <- snapshot:
					quit1 = true
				case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
					if sc.killed() {
						quit1 = true
						quit = true
					}
				}
			}
		case <-time.After(time.Duration(CHECKTIMEOUT) * time.Millisecond):
			if sc.killed() {
				quit = true
			}
		}
	}
}

/*
must be called with sc.mu.Lock
*/
func (sc *ShardController) decodeSnapshot(snapshot []byte) {
	reader := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(reader)

	var maxRaftState int
	var configs []Config
	var cachedReplies map[int64]ControllerReply

	if d.Decode(&maxRaftState) != nil ||
		d.Decode(&configs) != nil ||
		d.Decode(&cachedReplies) != nil {
		log.Fatalf("decoding error!\n")
	} else {
		sc.maxRaftState = maxRaftState
		sc.configs = configs
		sc.cachedReplies = cachedReplies
	}
}

/*
must be called with sc.mu.Lock
*/
func (sc *ShardController) encodeSnapshot() []byte {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	if e.Encode(sc.maxRaftState) != nil ||
		// e.Encode(sc.latestAppliedIndex) != nil ||
		// e.Encode(sc.latestAppliedTerm) != nil ||
		e.Encode(sc.configs) != nil ||
		e.Encode(sc.cachedReplies) != nil {
		log.Fatalf("encoding error!\n")
	}
	data := writer.Bytes()
	return data
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardController) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardController) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardsc tester
func (sc *ShardController) Raft() *raft.Raft {
	return sc.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardController service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardController {
	labgob.Register(ControllerCommand{})
	sc := new(ShardController)
	sc.me = me
	maxRaftState := -1
	// command size checking will not be disabled
	if maxRaftState != -1 {
		maxRaftState = 8 * maxRaftState
	}
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh, maxRaftState)
	sc.maxRaftState = maxRaftState

	sc.cachedReplies = make(map[int64]ControllerReply)
	sc.clerkChans = make(map[int64]chan ControllerReply)

	sc.latestAppliedIndex = 0
	sc.latestAppliedTerm = 0

	sc.configs = make([]Config, 0)
	sc.initConfig(0)

	go sc.commandExecutor()
	go sc.snapshotController()

	return sc
}

// all Shards are managed by group 0, which has no servers
func (sc *ShardController) initConfig(num int) {
	config := Config{}
	config.Num = num
	config.Groups = make(map[int][]string)
	config.ServerNames = make(map[string]int)
	config.GroupShards = make(map[int][]int)
	sc.configs = append(sc.configs, config)
}
