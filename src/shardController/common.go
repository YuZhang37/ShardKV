package shardController

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	CHECKTIMEOUT      = 200
	MAXKVCOMMANDSIZE  = 500
	LEASTMAXRAFTSTATE = 1000
)

const (
	JOIN  = "Join"
	LEAVE = "Leave"
	MOVE  = "Move"
	QUERY = "Query"
)

/************** definition for controller client ****************/
type Clerk struct {
	servers  []*labrpc.ClientEnd
	clerkId  int64
	seqNum   int64
	leaderId int
}

/*********** end of definition for controller client *************/

/************** definition for controller server *************/

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}
type ShardController struct {
	mu      sync.Mutex
	dead    int32 // set by Kill()
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// snapshot if log grows this big
	maxRaftState int // persist

	// replies for all operations
	cachedReplies map[int64]ControllerReply // persist
	clerkChans    map[int64]chan ControllerReply

	latestAppliedIndex int
	latestAppliedTerm  int

	// 0 is the initial config
	// indexed by config num
	configs []Config // persist
}

type ControllerCommand struct {
	ClerkId       int64
	SeqNum        int64
	Operation     string
	JoinedServers map[int][]string
	LeaveGIDs     []int
	MovedShard    int
	MovedGID      int
	QueryNum      int
}

/*********** end of definition for controller server *************/

/*************** clerk-controller RPC definition ***************/
type ControllerRequestArgs struct {
	ClerkId int64
	SeqNum  int64

	Operation string
	// Join
	JoinedServers map[int][]string
	// Leave
	LeaveGIDs []int
	// Move
	MovedShard int
	MovedGID   int
	// Query
	QueryNum int
}

type ControllerReply struct {
	ClerkId  int64
	SeqNum   int64
	LeaderId int

	Succeeded    bool
	SizeExceeded bool
	Config       Config
}

/************ end of clerk-controller RPC definition *************/

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//
