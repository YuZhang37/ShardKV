package raft

import (
	"sync"

	"6.5840/labrpc"
)

/*
	liveThreads  []bool
	can't simply de-duplicate like this:

	scenario 1:
	thread 1: start(comm1) failed on some machines, no majority
	thread 2: start(comm2) failed on some machines, no majority
	if only retries on thread 1,
	thread 2 will never get the majority and proceed,
	although thread 1 can commit comm2,

	solution:
	constantly checking if comm2 is committed by the leader
	if so, replies to the client
	use a different channel from a different thread
	checking every 10ms

	thread 1 may commit the entry for thread 2
	vice versa

	another design:
	for failed server, don't try infinitely if the majority has
	responded successfully

	if index1 is not committed yet, don't issue appendEntries for later index2 > index1, just append this entry in local leader log:
	is this solution gonna be slower?
	yes, if RPC takes long time, it affects performance

	if not synchronize index1 and index2 and the majority
	of the servers have failed:
	rpc flooding to these servers

	to avoid rpc flooding:
	record largest new entry index
	only retries with the largest new entry index
	all other retries just return false replies
	in the case of false replies, constantly checking
	matchedIndices to see if the current entry is commited
*/

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int // index of the log for de-duplication on sending to the state machine
	CommandTerm  int // for debugging

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

const (
	FOLLOWER  int32 = 1
	CANDIDATE int32 = 2
	LEADER    int32 = 3
)

const (
	HBTIMEOUT           int = 200
	ELETIMEOUT          int = 1000
	RANDOMRANGE         int = 1000
	CHECKCOMMITTIMEOUT  int = 25
	REAPPENDTIMEOUT     int = 50
	CHECKAPPLIEDTIMEOUT int = 200
)

const (
	/*
		this size reserved for persistent fields
		currentTerm and votedFor
		snapshotLastIndex and snapshotLastTerm
		8 * 4
		noop:
		LogEntry{
			Term:
			Index:
			Command:
				Noop{
					Operation: "no-op",
				}
		}
		8 * 3
	*/
	RESERVESPACE    int = 8 * 7 * 10
	MAXLOGENTRYSIZE int = 750
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	NOOP string = "no-op"
)

type Noop struct {
	Operation string
}

type SnapshotInfo struct {
	Data              []byte
	LastIncludedIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex // Lock to protect shared access to this peer's state
	lockChan  chan int
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	gid       int
	dead      int32 // set by Kill()
	applyCh   chan ApplyMsg

	SignalSnapshot chan int
	SnapshotChan   chan SnapshotInfo

	// maxLogSize + reserved space, reserved space is for other metadata
	maxRaftState int
	// -1 for no snapshot, -2 for no log entries before appending,
	// 8*non-negative number is the maxLogSize
	maxLogSize int

	/*
		states for all servers (log index)
		log index always starts with 1, not the same as array index
		commitIndex and lastApplied don't need to be reset on leader change
	*/
	commitIndex int
	lastApplied int32

	// need to be persistent
	currentTerm int
	/*
		-1 for nil, initialized to -1
		every time currentTerm increments, votedFor needs to reset to -1
		votedFor is maintained during the whole term, even when the leader is elected, which can avoid the election issued by a server on the same term.
	*/
	votedFor int32
	log      []LogEntry

	/*
		records what the current leader is, used to redirect requests, but this information may be outdated
		initialized to be -1
		reset to -1 when turns into candidate/increment term
		update to itself when turns into leader
		update when a valid leader sends appendEntries or heartbeat to it
		requestVote with higher term will reset to -1
		=>
		whenever the term changes regardless of the change on leader,
		reset this to be -1. This term may or may not elect a valid leader
		if there is a successful leader election, set this variable
	*/
	currentLeader int

	/*
		initialized to be 0,
		whenever the term changes and a new valid leader is elected, reset to be 0, it happens when a valid heartbeat or appendEntries is received. When the server turns into candidate, the variable contains previous value
		 =>
		whenever the term changes regardless of the change on leader,
		reset this to be 0. This term may or may not elect a valid leader
		it turns into candidate or receive heartbeat or appendEntries from leader with higher term
	*/
	currentAppended int

	role int32

	/*
		indicate if leader and valid candidate sends an rpc for
		depressing leader election
		set true when requestVote grants vote for other servers
		or heartbeat or appendEntries is received from a valid leader
		set false when timer goes off
		leader will always have it on false
	*/
	msgReceived bool

	// states on leader

	// reset to be [len(log)+1...len(log)+1] on successful leader election
	nextIndices []int
	// reset to be [0...0] on successful leader election
	matchIndices []int
	/*
		keep track of the latest index of log entry which starts appendEntries in all servers.
		reset to be [0...0] on successful leader election
	*/
	latestIssuedEntryIndices []int

	// close on successful leader election, to terminate the previous trailingReplay thread
	trailingReplyChan chan AppendEntriesReply
	// when we close chan, for reply := range rf.trailingReplyChan may not get executed immediately, need to coordinate
	quitTrailingReplyChan chan int

	// timeout fields
	hbTimeOut   int
	eleTimeOut  int
	randomRange int

	/*
		for snapshot, needs to be persistent
			initialized to 0,
			changed when snapshot is updated:
			Snapshot(), installSnapshot()
	*/
	snapshotLastIndex int
	snapshotLastTerm  int
	snapshot          []byte

	// ordered command delivery
	// lock across channel, may held for long, don't try to use this lock inside rf.mu
	appliedLock         sync.Mutex
	orderedDeliveryChan chan ApplyMsg
	pendingMsg          map[int]ApplyMsg
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int // starting from 1, 0 if no log
	LastLogTerm  int // 0 if no last log
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	Server      int
}

type AppendEntriesArgs struct {
	Term            int
	LeaderId        int
	IssueEntryIndex int

	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {

	// info in request
	Term   int
	Server int
	// NextIndex info from args
	NextIndex int
	/*
		index of the entry starting appendEntries
		for heartbeat, it's -1
	*/
	IssueEntryIndex int

	Success bool

	// when Success = false
	// check these two to indicate why the request failed
	// if both are false, it indicates a failed RPC
	HigherTerm bool
	MisMatched bool
	// index of the last entry sent out by the leader and appended by the server, used by the leader to update matchIndices and nextIndices, 0 on false reply
	LastAppendedIndex int

	// args for optimization on mismatching logs, only used when MisMatched is true

	// if the server has entry at prevIndex, then equals to the term of that entry, otherwise, -1
	ConflictTerm int
	// if the server has entry at prevIndex, then equals to the start index of the term of that entry, otherwise, -1
	ConflictStartIndex int
	// the length of the server's log, only used when ConflictTerm == -1, which means, LogLength < prevIndex
	LogLength int
}

type SendSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte
}

type SendSnapshotReply struct {
	// the server's Term
	Term int
	// the receiving server
	Server int
	// info on the latest snapshot the server stores
	LastIncludedIndex int
	LastIncludedTerm  int
	// false when contacting servers with higher term or snapshot is not more up-to-date than the server's
	Installed bool
}

/*
if a condition needs to check within a lock,
and the condition may change when the code leaves the lock and acquires the lock again
we need to keep checking the condition for correctness every time we leave the locked area and entering it again.

critical thing:
if a leader becomes a follower, it must not send appendEntries requests,
otherwise, the servers will be in inconsistent states

the key thing is the currentTerm changed when the leader becomes the follower

for the conflicting entry at some index of both logs,
can the term of the follower be larger than the leader?

machine 0 		:		1,1,2
machine 1 leader: 		1,1,1,3,3,9
machine 2 follower: 	1,1,7,8,8,8

could this happen?
machine 1 is elected to be leader for term 2, appends 2, 2
but doesn't commit them, an elected to be leader for term 9

The leader initially guesses the nextIndex for a server to be len(log) + 1, after get the reply from appendEntries to that server, the leader can make another guess.

if the follower's reply has a higher term conflicting term,
backing off one such term in the next appendEntries
if the follower's reply has a lower term...
backing off at least one term in the leader's log in the next appendEntries.
*/
