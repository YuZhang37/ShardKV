package kvraft

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raft"
)

const (
	PUT                = "Put"
	APPEND             = "Append"
	GET                = "Get"
	CHECKLEADERTIMEOUT = 100
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
	mu sync.Mutex
	// used to signal request handlers to check replies
	cond         *sync.Cond
	me           int
	dead         int32 // set by Kill()
	SignalKilled chan int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big

	kvStore map[string]string
	// clerk id to reply
	cachedReplies map[int64]RequestReply
	clerkChans    map[int64]chan RequestReply

	latestAppliedIndex int
	latestAppliedTerm  int
}

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

	Succeeded bool
	// only used for Get
	Value string
	// the key exists in kvStore, only used for get and append
	Exists bool
}

/************** end of clerk-kvServer RPC definition **************/
