package raft

/*
where is rf.currentTerm changed?
all rpc calls, both the receive and transmit ends
1. heartbeats: both the leader and follower
2. election: both the candidate and follower
3. appendEntries: both the leader and follower

4. followers can increment terms when starting election
5. initialization

where is rf.votedFor changed?
1. during election,
	reset to -1 when contacting with higher term,
	update to candidateId when receiving election from a valid candidate
2. initialization

where is rf.log changed?
1. leader can change the log at Start()
2. followers change the log at AppendEntries()

Do we need to persist for every point where these states are changed?
a naive approach would do that,
but we can group them together into one persist() call on all the changes within a function call

try to persist once per func call

both persist() and readPersist() assume the calling functions
handle the locking and unlocking
*/

import (
	"bytes"
	"fmt"
	"log"

	"6.5840/labgob"
)

/*
save Raft's persistent state to stable storage,
where it can later be retrieved after a crash and restart.
see paper's Figure 2 for a description of what should be persistent.
before you've implemented snapshots, you should pass nil as the
second argument to persister.Save().
after you've implemented snapshots, pass the current snapshot
(or nil if there's not yet a snapshot).

the function needs to be called with rf.mu held
*/
func (rf *Raft) persistState(format string, a ...interface{}) {

	rf.persistStateWithSnapshot("persist()")

}

func (rf *Raft) persistStateWithSnapshot(format string, a ...interface{}) {
	KVStoreDPrintf("******* Server %v persist states  for %v *******\n", rf.me, fmt.Sprintf(format, a...))
	SnapshotDPrintf("******* Server %v persist states  for %v *******\n", rf.me, fmt.Sprintf(format, a...))
	SnapshotDPrintf("Server %v is leader: %v\n", rf.me, rf.currentLeader == rf.me)
	SnapshotDPrintf("Server %v currentTerm: %v\n", rf.me, rf.currentTerm)
	SnapshotDPrintf("Server %v votedFor: %v\n", rf.me, rf.votedFor)
	SnapshotDPrintf("Server %v commitIndex: %v\n", rf.me, rf.commitIndex)
	// SnapshotDPrintf("Server %v lastApplied: %v\n", rf.me, rf.lastApplied)
	SnapshotDPrintf("Server %v log: %v at %v\n", rf.me, rf.log, rf.me)
	SnapshotDPrintf("Server %v snapshotLastIndex: %v\n", rf.me, rf.snapshotLastIndex)
	SnapshotDPrintf("Server %v snapshotLastTerm: %v\n", rf.me, rf.snapshotLastTerm)
	// rf.appliedLock.Lock()
	// appliedIndex := rf.lastApplied
	// rf.appliedLock.Unlock()
	KVStoreDPrintf("Server: %v persists rf.commitIndex: %v, rf.currentTerm: %v, rf.votedFor: %v, rf.snapshotLastIndex: %v, rf.snapshotLastTerm: %v rf.log: %v\n", rf.me, rf.commitIndex, rf.currentTerm, rf.votedFor, rf.snapshotLastIndex, rf.snapshotLastTerm, rf.log)
	data := rf.getRaftStateData()
	if len(data) >= rf.maxRaftState {
		log.Fatalf("size of raft state: %v exceeds maxRaftState: %v\n", len(data), rf.maxRaftState)
	}
	rf.persister.Save(data, rf.snapshot)
}

func (rf *Raft) getRaftStateData() []byte {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	// interface type when passing value, no need to register the type
	// can we encode empty slice ? needs to test
	if e.Encode(rf.currentTerm) != nil ||
		e.Encode(rf.votedFor) != nil ||
		e.Encode(rf.snapshotLastIndex) != nil ||
		e.Encode(rf.snapshotLastTerm) != nil ||
		e.Encode(rf.log) != nil {
		log.Fatalf("encoding error!\n")
	}
	data := writer.Bytes()
	return data
}

func (rf *Raft) getLogSize(logEntries []LogEntry) int {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	if e.Encode(logEntries) != nil {
		log.Fatalf("encoding error!\n")
	}
	data := writer.Bytes()
	return len(data)
}

/*
restore previously persisted state.
*/
func (rf *Raft) readPersist() bool {
	PersistenceDPrintf("******* Server %v read states *******\n", rf.me)
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		PersistenceDPrintf("No states to read!\n")
		return false
	}
	reader := bytes.NewBuffer(data)
	d := labgob.NewDecoder(reader)

	var currentTerm, votedFor, snapshotLastIndex, snapshotLastTerm int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatalf("decoding error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.snapshotLastIndex = snapshotLastIndex
		rf.snapshotLastTerm = snapshotLastTerm
		rf.log = logEntries
		if rf.commitIndex < snapshotLastIndex {
			rf.commitIndex = snapshotLastIndex
		}
	}
	PersistenceDPrintf("currentTerm: %v\n", rf.currentTerm)
	PersistenceDPrintf("votedFor: %v\n", rf.votedFor)
	PersistenceDPrintf("snapshotLastIndex: %v\n", rf.snapshotLastIndex)
	PersistenceDPrintf("snapshotLastTerm: %v\n", rf.snapshotLastTerm)
	PersistenceDPrintf("log: %v\n", rf.log)

	KVStoreDPrintf("Server %v read states, currentTerm: %v, votedFor: %v, snapshotLastIndex: %v, snapshotLastTerm: %v, log: %v\n", rf.me, rf.currentTerm, rf.votedFor, rf.snapshotLastIndex, rf.snapshotLastTerm, rf.log)
	snapshot := rf.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		PersistenceDPrintf("No snapshot to read!\n")
		return true
	}
	rf.snapshot = snapshot
	KVStoreDPrintf("server: %v in readPersist() calls rf.ApplySnapshot()\n", rf.me)
	go rf.ApplySnapshot()
	return true
}
