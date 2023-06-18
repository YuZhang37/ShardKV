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
	"log"

	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {

	PersistenceDPrintf("******* persist states *******\n")

	PersistenceDPrintf("currentTerm: %v\n", rf.currentTerm)
	PersistenceDPrintf("votedFor: %v\n", rf.votedFor)
	PersistenceDPrintf("log: %v\n", rf.log)

	writer := new(bytes.Buffer)
	e := labgob.NewEncoder(writer)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	// interface type when passing value, no need to register the type
	e.Encode(rf.log)
	data := writer.Bytes()
	rf.persister.SaveRaftState(data)

}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) bool {
	PersistenceDPrintf("******* read states *******\n")
	if data == nil || len(data) < 1 { // bootstrap without any state?
		PersistenceDPrintf("No states to read!\n")
		return false
	}
	reader := bytes.NewBuffer(data)
	d := labgob.NewDecoder(reader)

	var currentTerm, votedFor int
	var logEntries []LogEntry

	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logEntries) != nil {
		log.Fatalf("decoding error!\n")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logEntries
	}
	PersistenceDPrintf("currentTerm: %v\n", rf.currentTerm)
	PersistenceDPrintf("votedFor: %v\n", rf.votedFor)
	PersistenceDPrintf("log: %v\n", rf.log)

	return true
}
