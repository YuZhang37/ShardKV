package kvraft

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	PUT               = "Put"
	APPEND            = "Append"
	GET               = "Get"
	CHECKTIMEOUT      = 200
	MAXKVCOMMANDSIZE  = 500
	LEASTMAXRAFTSTATE = 1000
)

/************************ clerk definition *********************/
type Clerk struct {
	// mu       sync.Mutex
	servers  []*labrpc.ClientEnd
	clerkId  int64
	seqNum   int64
	leaderId int
}

/************************ end of clerk *********************/

/************************ server definition *********************/
type KVCommand struct {
	ClerkId   int64
	SeqNum    int64
	Key       string
	Value     string
	Operation string
}
type KVServer struct {
	mu           sync.Mutex
	me           int
	dead         int32 // set by Kill()
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxRaftState int // snapshot if log grows this big

	kvStore map[string]string
	// clerk id to reply
	cachedReplies map[int64]RequestReply
	clerkChans    map[int64]chan RequestReply

	latestAppliedIndex int
	latestAppliedTerm  int
}

/*
	maxRaftState int
	kvStore map[string]string
	cachedReplies map[int64]RequestReply
	need to be persistent
*/

/************************ end of server *********************/

/*************** clerk-kvServer RPC definition ***************/
type RequestArgs struct {
	ClerkId int64
	SeqNum  int64

	Operation string
	Key       string
	// only used for Put and Append
	Value string
}

type RequestReply struct {
	ClerkId  int64
	SeqNum   int64
	LeaderId int

	Succeeded    bool
	SizeExceeded bool
	// only used for Get
	Value string
	// the key exists in kvStore, only used for get and append
	Exists bool
}

/************** end of clerk-kvServer RPC definition **************/
