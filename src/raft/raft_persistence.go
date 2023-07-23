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

	rf.persistStateWithSnapshot(format, a...)

}

func (rf *Raft) persistStateWithSnapshot(format string, a ...interface{}) {
	rf.tempDPrintf(`
	persistStateWithSnapshot(): 
	Server %v persist states  for %v
	is leader: %v,
	currentTerm: %v,
	votedFor: %v,
	commitIndex: %v,
	log: %v,
	snapshotLastIndex: %v,
	snapshotLastTerm: %v,
	`,
		rf.me, fmt.Sprintf(format, a...),
		rf.currentLeader == rf.me,
		rf.currentTerm,
		rf.votedFor,
		rf.commitIndex,
		rf.log,
		rf.snapshotLastIndex,
		rf.snapshotLastTerm,
	)
	data := rf.getRaftStateData()
	rf.persister.Save(data, rf.snapshot)
	rf.tempDPrintf("persistStateWithSnapshot(): finished persist states  for %v\n", rf.me, fmt.Sprintf(format, a...))
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
		rf.logFatal("Fatal: getRaftStateData(): encoding error!\n")
	}
	data := writer.Bytes()
	return data
}

func (rf *Raft) getLogSize(logEntries []LogEntry) int {
	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	for index, logEntry := range logEntries {
		if e.Encode(logEntry) != nil {
			rf.logFatal("getLogSize(): encoding error! index: %v, logEntry: %v\n", index, logEntry)
		}
	}

	data := writer.Bytes()
	return len(data)
}

/*
restore previously persisted state.
*/
func (rf *Raft) readPersist() bool {
	rf.persistenceDPrintf("readPersist(): \n")
	data := rf.persister.ReadRaftState()
	if data == nil || len(data) < 1 { // bootstrap without any state?
		rf.persistenceDPrintf("readPersist(): No states to read!\n")
		return false
	}
	reader := bytes.NewBuffer(data)
	d := labgob.NewDecoder(reader)

	var currentTerm, snapshotLastIndex, snapshotLastTerm int
	var votedFor int32
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&snapshotLastIndex) != nil ||
		d.Decode(&snapshotLastTerm) != nil ||
		d.Decode(&logEntries) != nil {
		rf.logFatal("decoding error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.setVotedFor(int(votedFor))
		rf.snapshotLastIndex = snapshotLastIndex
		rf.snapshotLastTerm = snapshotLastTerm
		rf.log = logEntries
		if rf.commitIndex < snapshotLastIndex {
			rf.commitIndex = snapshotLastIndex
		}
	}

	rf.tempDPrintf("readPersist(): currentTerm: %v, votedFor: %v, snapshotLastIndex: %v, snapshotLastTerm: %v, log: %v\n", rf.currentTerm, rf.votedFor, rf.snapshotLastIndex, rf.snapshotLastTerm, rf.log)
	snapshot := rf.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		rf.persistenceDPrintf("readPersist(): No snapshot to read!\n")
		return true
	}
	rf.snapshot = snapshot
	rf.kvStoreDPrintf("readPersist(): calls rf.ApplySnapshot()\n")
	go rf.ApplySnapshot()
	return true
}
