package raft

import (
	"6.824/labrpc"
	"sync"
)

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
	HBTIMEOUT    int = 200
	ELETIMEOUT   int = 1000
	RANDOMRANGE  int = 1000
	CCTIMEOUT    int = 5
	REAPPTIMEOUT int = 10
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

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// states for all servers (log index)
	// log index always starts with 1, not the same as array index
	commitIndex int
	lastApplied int

	// need to be persistent
	currentTerm int
	votedFor    int // -1 for nil, new term will reset to -1, initialized to -1
	log         []LogEntry

	// records what the current leader is, used to redirect requests, but this information may be outdated
	// update when valid appendEntries request comes
	// reset when turns into candidate
	currentLeader int

	role int32

	// indicate if leader and valid candidate sends an rpc for depressing leader election
	// set true when requestVote grants vote for other servers
	// or appendEntries is received from a valid leader
	// set false when timer goes off
	msgReceived bool

	// count the number of granted votes in election
	// voteGranted int

	// states on leader
	nextIndices  []int
	matchIndices []int
	liveThreads  []bool
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
	*/

	// timeout fields
	hbTimeOut   int
	eleTimeOut  int
	randomRange int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int // starting from 1, 0 if no log
	LastLogTerm  int // 0 if no last log
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
	Server      int
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	// not used
	PrevLogIndex      int
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int
}

type AppendEntriesReply struct {
	Server     int
	Term       int
	Success    bool
	HigherTerm bool
	MisMatched bool
}
