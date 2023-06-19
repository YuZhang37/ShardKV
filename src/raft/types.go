package raft

import (
	"sync"

	"6.824/labrpc"
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

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// type Role int32

// const (
//
//	FOLLOWER  Role = 1
//	CANDIDATE Role = 2
//	LEADER    Role = 3
//
// )

const (
	FOLLOWER  int32 = 1
	CANDIDATE int32 = 2
	LEADER    int32 = 3
)

const (
	HBTIMEOUT          int = 200
	ELETIMEOUT         int = 1000
	RANDOMRANGE        int = 1000
	CHECKCOMMITTIMEOUT int = 25
	REAPPENDTIMEOUT    int = 50
)

type LogEntry struct {
	Term    int
	Index   int
	Command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	applyCh   chan ApplyMsg

	/*
		states for all servers (log index)
		log index always starts with 1, not the same as array index
		commitIndex and lastApplied don't need to be reset on leader change
	*/
	commitIndex int
	lastApplied int

	// need to be persistent
	currentTerm int
	/*
		-1 for nil, initialized to -1
		every time currentTerm increments, votedFor needs to reset to -1
	*/
	votedFor int
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
}
