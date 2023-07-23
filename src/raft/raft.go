package raft

import (
	"fmt"
	"log"
	"unsafe"

	"6.5840/labgob"
	"6.5840/labrpc"
)

/*
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
*/
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg, opts ...interface{}) *Raft {
	labgob.Register(Noop{})
	// no lock is need at initialization
	rf := &Raft{}
	rf.lockChan = nil
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.gid = -1
	rf.dead = 0
	rf.applyCh = applyCh

	rf.SignalSnapshot = make(chan int)
	rf.SnapshotChan = make(chan SnapshotInfo)
	if len(opts) > 0 {
		rf.maxRaftState = opts[0].(int)
	} else {
		rf.maxRaftState = -1
	}
	if len(opts) > 1 {
		rf.gid = opts[1].(int)
	}

	if rf.maxRaftState >= 0 {
		rf.maxLogSize = rf.maxRaftState - RESERVESPACE
	} else {
		rf.maxLogSize = rf.maxRaftState
	}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.appliedLockChan = nil

	// persistent states
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.role = FOLLOWER
	rf.msgReceived = false
	rf.currentLeader = -1
	rf.currentAppended = 0

	rf.nextIndices = make([]int, len(rf.peers))
	rf.matchIndices = make([]int, len(rf.peers))
	rf.latestIssuedEntryIndices = make([]int, len(rf.peers))
	rf.trailingReplyChan = make(chan AppendEntriesReply)
	rf.quitTrailingReplyChan = make(chan int)
	go func() {
		rf.quitTrailingReplyChan <- 0
	}()

	rf.hbTimeOut = HBTIMEOUT
	rf.eleTimeOut = ELETIMEOUT
	rf.randomRange = RANDOMRANGE

	rf.snapshotLastIndex = 0
	rf.snapshotLastTerm = 0

	rf.orderedDeliveryChan = make(chan ApplyMsg)
	rf.pendingMsg = make(map[int]ApplyMsg)
	go rf.OrderedCommandDelivery()

	// initialize from state persisted before a crash
	recover := rf.readPersist()
	if !recover {
		PersistenceDPrintf("Not previous state to recover from, persist initialization\n")
		rf.persistState("server %v initialization", rf.me)
	} else {
		PersistenceDPrintf("Recover form previous state\n")
	}

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

/*
if the command sends to the valid leader, entry will be appended to the leader's local log and returns index, term, true,
an AppendCommand goroutine is issued to commit the command
otherwise returns -1, -1, false
if log exceeds maxLogSize and can't reduce size by snapshot()
return -1, -1, true. In this case, start won't return immediately
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	rf.lockMu("Start() with command: %v\n", command)
	defer rf.unlockMu()
	index := -1
	term := -1
	isLeader := false
	KVStoreDPrintf("Server: %v, Start() is called with %v\n", rf.me, command)
	defer KVStoreDPrintf("Server: %v, Start() finished %v with index: %v, term: %v, isLeader: %v\n", rf.me, command, index, term, isLeader)
	if rf.killed() || rf.role != LEADER {
		AppendEntriesDPrintf("Command %v sends to %v, which is not a leader, the leader is %v\n", command, rf.me, rf.currentLeader)
		return index, term, isLeader
	}

	AppendEntriesDPrintf("Command %v sends to %v, which is a leader for term: %v\n", command, rf.me, rf.currentTerm)
	AppendEntriesDPrintf("Start processing...\n")

	term = rf.currentTerm
	isLeader = true
	//append to the leader's local log
	if len(rf.log) == 0 {
		index = rf.snapshotLastIndex + 1
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	}
	if unsafe.Sizeof(entry) >= uintptr(MAXLOGENTRYSIZE) {
		log.Fatalf("***************** entry: %v has size of %v which exceeds the max log entry size: %v ***********", entry, unsafe.Sizeof(entry), MAXLOGENTRYSIZE)
	}
	newLog := append(rf.log, entry)
	size := rf.getLogSize(newLog)
	if rf.maxLogSize >= 0 && size >= rf.maxLogSize {
		// snapshot enabled
		rf.logRaftState("from leader: before signalSnapshot")
		rf.logRaftState2(size)
		if rf.commitIndex > rf.snapshotLastIndex {
			// there are log entries to compact
			rf.snapshotDPrintf("Start() calls insideApplyCommand...")
			rf.insideApplyCommand(rf.commitIndex, true)
			rf.snapshotDPrintf("Start() finishes insideApplyCommand...")
			rf.snapshotDPrintf("Start() calls signalSnapshot...")
			rf.signalSnapshot()
			rf.snapshotDPrintf("Start() finishes signalSnapshot...")
			rf.persistState("server %v Start() snapshots for entry %v", rf.me, entry)
			rf.logRaftState("from leader: after signalSnapshot")
			newLog = append(rf.log, entry)
			size = rf.getLogSize(newLog)
		}
		if size >= rf.maxLogSize {
			index = -1
			term = -1
			return index, term, isLeader
		}
	}
	if rf.maxLogSize == -2 {
		// must apply the command first and take a snapshot if there is log entry
		rf.logRaftState("from leader: before signalSnapshot")
		rf.logRaftState2(size)
		if len(rf.log) > 0 {
			if rf.commitIndex > rf.snapshotLastIndex {
				// there are log entries to compact
				rf.snapshotDPrintf("Start() calls insideApplyCommand...")
				rf.insideApplyCommand(rf.commitIndex, true)
				rf.snapshotDPrintf("Start() finishes insideApplyCommand...")
				rf.snapshotDPrintf("Start() calls signalSnapshot...")
				rf.signalSnapshot()
				rf.snapshotDPrintf("Start() finishes signalSnapshot...")
				rf.persistState("server %v Start() snapshots for entry %v", rf.me, entry)
				rf.logRaftState("from leader: after signalSnapshot")
				newLog = append(rf.log, entry)
			}
			if len(rf.log) > 0 {
				index = -1
				term = -1
				return index, term, isLeader
			}
		}
	}
	rf.log = newLog
	rf.logRaftStateForInstallSnapshot(fmt.Sprintf("leader appends entry: %v ", entry))
	rf.persistState("server %v Start() appends entry %v", rf.me, entry)
	AppendEntriesDPrintf("Command %v is appended on %v at index of %v\n", command, rf.me, len(rf.log))

	go rf.reachConsensus(entry.Index)
	return index, term, isLeader
}

// rf.mu is held
func (rf *Raft) commitNoop() {
	KVStoreDPrintf("Server: %v commitNoop() gets running..\n", rf.me)
	noop := Noop{
		Operation: NOOP,
	}
	//append to the leader's local log
	var index int
	if len(rf.log) == 0 {
		if rf.snapshotLastIndex > rf.commitIndex {
			rf.commitIndex = rf.snapshotLastIndex
		}
		// the leader will update its commitIndex at least to lastsnapshot index
		return
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: noop,
	}
	rf.log = append(rf.log, entry)
	rf.persistState("server %v commitNoop() appends entry %v", rf.me, entry)
	go rf.reachConsensus(entry.Index)
	KVStoreDPrintf("Server: %v commitNoop() at index: %v, entry: %v, log: %v, commitIndex: %v\n", rf.me, index, entry, rf.log, rf.commitIndex)
}

/*
the service using Raft (e.g. a k/v server) wants to start
agreement on the next command to be appended to Raft's log. if this
server isn't the leader, returns false. otherwise start the
agreement and return immediately. there is no guarantee that this
command will ever be committed to the Raft log, since the leader
may fail or lose an election. even if the Raft instance has been killed,
this function should return gracefully.

the first return value is the index that the command will appear at
if it's ever committed. the second return value is the current
term. the third return value is true if this server believes it is
the leader.

if the command sends to the valid leader, entry will be appended to the leader's local log and returns index, term, true,
an AppendCommand goroutine is issued to commit the command
otherwise returns -1, -1, false
if log exceeds maxLogSize and can't reduce size by snapshot()
return -1, -1, true. In this case, start won't return immediately
func (rf *Raft) Start(command interface{}) (int, int, bool)
*/
