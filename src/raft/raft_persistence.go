package raft

/*
go test -race -run
TestPersist12C				: pass (without wait)
TestPersist22C				: pass (one fail, entries on different servers don't match)
TestPersist32C				: pass (one fail, with memory error)
TestFigure82C				: pass
TestUnreliableAgree2C		: pass
TestFigure8Unreliable2C		: pass (failed to reach agreement)
								result_1000_5servers-changed-60min.txt 388s
TestReliableChurn2C			: failed to reach agreement(pass)
							result_1000_5servers-TestReliableChurn2C-60min.txt 49.863s
TestUnreliableChurn2C		: pass (failed to reach agreement)

run 1:
go test -race -run 2C
Test (2C): basic persistence ...
  ... Passed --  10.2  3   98   30603    6
Test (2C): more persistence ...
  ... Passed --  28.8  5  830  226034   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   5.0  3   43   14041    4
Test (2C): Figure 8 ...
  ... Passed --  34.5  5  324   85352    8
Test (2C): unreliable agreement ...
  ... Passed --  32.4  5 1600  649249  246
Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (49.47s)
    config.go:567: one(3872) failed to reach agreement
Test (2C): churn ...
--- FAIL: TestReliableChurn2C (27.10s)
    config.go:567: one(8241388452226419523) failed to reach agreement
Test (2C): unreliable churn ...
--- FAIL: TestUnreliableChurn2C (27.20s)
    config.go:567: one(8556106806782075100) failed to reach agreement
FAIL
exit status 1
FAIL    6.824/raft      214.863s

run 2:
go test -race -run 2C
Test (2C): basic persistence ...
  ... Passed --  10.2  3   98   30603    6
Test (2C): more persistence ...
  ... Passed --  26.8  5  790  210220   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   5.0  3   43   14041    4
Test (2C): Figure 8 ...
  ... Passed --  35.3  5  349   93628   12
Test (2C): unreliable agreement ...
  ... Passed --  34.6  5 1648  667062  247
Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (48.25s)
    config.go:567: one(1438) failed to reach agreement
Test (2C): churn ...
  ... Passed --  16.8  5 6424 13582281 1470
Test (2C): unreliable churn ...
--- FAIL: TestUnreliableChurn2C (27.02s)
    config.go:567: one(8241388452226419523) failed to reach agreement
FAIL
exit status 1
FAIL    6.824/raft      204.246s

run 3:

go test -race -run 2C
Test (2C): basic persistence ...
  ... Passed --  10.2  3   98   30603    6
Test (2C): more persistence ...
  ... Passed --  28.9  5  830  226034   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   5.0  3   43   14041    4
Test (2C): Figure 8 ...
  ... Passed --  33.5  5  345   90610   10
Test (2C): unreliable agreement ...
  ... Passed --  32.4  5 1600  649714  246
Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (47.90s)
    config.go:567: one(201) failed to reach agreement
Test (2C): churn ...
--- FAIL: TestReliableChurn2C (26.93s)
    config.go:567: one(3390393562759376202) failed to reach agreement
Test (2C): unreliable churn ...
  ... Passed --  16.9  5 5264 2902558 1235
FAIL
exit status 1
FAIL    6.824/raft      201.985s


run 4:

go test -race -run 2C
Test (2C): basic persistence ...
  ... Passed --  10.2  3   98   30603    6
Test (2C): more persistence ...
  ... Passed --  28.8  5  830  226034   16
Test (2C): partitioned leader and one follower crash, leader restarts ...
  ... Passed --   5.0  3   43   14041    4
Test (2C): Figure 8 ...
  ... Passed --  30.7  5  314   85974   11
Test (2C): unreliable agreement ...
  ... Passed --  32.5  5 1600  651320  246
Test (2C): Figure 8 (unreliable) ...
--- FAIL: TestFigure8Unreliable2C (48.89s)
    config.go:567: one(4338) failed to reach agreement
Test (2C): churn ...
--- FAIL: TestReliableChurn2C (27.01s)
    config.go:567: one(1097372909064907392) failed to reach agreement
Test (2C): unreliable churn ...
--- FAIL: TestUnreliableChurn2C (27.19s)
    config.go:567: one(1774932891286980153) failed to reach agreement
FAIL
exit status 1
FAIL    6.824/raft      210.692s
*/

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

	"6.824/labgob"
)

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist(format string, a ...interface{}) {

	PersistenceDPrintf("******* Server %v persist states  for %v *******\n", rf.me, fmt.Sprintf(format, a...))
	PersistenceDPrintf("Server %v is leader: %v\n", rf.me, rf.currentLeader == rf.me)
	PersistenceDPrintf("Server %v currentTerm: %v\n", rf.me, rf.currentTerm)
	PersistenceDPrintf("Server %v votedFor: %v\n", rf.me, rf.votedFor)
	PersistenceDPrintf("Server %v log: %v at %v\n", rf.me, rf.log, rf.me)
	PersistenceDPrintf("Server %v commitIndex: %v\n", rf.me, rf.commitIndex)

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
	PersistenceDPrintf("******* Server %v read states *******\n", rf.me)
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
