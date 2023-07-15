package shardkv

import (
	"sync"

	"6.5840/labrpc"
	"6.5840/raft"
	"6.5840/shardController"
)

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	MAXKVCOMMANDSIZE  = 4000
	MAXTRANSMITSIZE   = 4500
	LEASTMAXRAFTSTATE = 1000
)

// const MAXLOGENTRYSIZE = raft.MAXLOGENTRYSIZE
// 1000 bytes is reserved
const MAXSHARDCHUNKSIZE = 4000

const (
	PUT           = "Put"
	APPEND        = "Append"
	GET           = "Get"
	TRANSMITSHARD = "TransmitShard"
	REMOVESHARD   = "RemoveShard"
	UPDATECONFIG  = "UpdateConfig"
	FORWORDSHARD  = "ForwardShard"
)

/************************ clerk definition *********************/

const (
	CHECKTIMEOUT            = 200
	CHECKCOMMITTEDTIMEOUT   = 20
	CHECKCONFIGTIMEOUT      = 100
	CHECKSHARDFINISHTIMEOUT = 200
	INSPECTSHADOWTIMEOUT    = 200
	RETRYTRANSMITTIMEOUT    = 200
)

type Clerk struct {
	clerkId  int64
	seqNum   int64
	sc       *shardController.Clerk
	config   shardController.Config
	make_end func(string) *labrpc.ClientEnd
}

/************************ end of clerk *********************/

/************************ server definition *********************/

/*
for the send end
*/
type ShadowShardGroup struct {
	TargetGID   int
	Servernames []string
	TransmitNum int
	ShardIDs    []int
	ConfigNums  []int
	Processing  bool

	// when a shard finishes the move, issue a new command to remove that shard from shadowShards
	// pitfall: the server may sending this shard when this shard is moved, the send thread needs to check if the shard still exists, if not, stop sending and treat it as the sending is finished and remove the shard from group
	ShadowShards        [][]ChunkKVStore
	ShadowCachedReplies [][]ChunkedCachedReply
}

type TransmitInfo struct {
	FromGID     int
	TransmitNum int
	ChunkNum    int
}

type ChunkKVStore struct {
	Size    uintptr
	KVStore map[string]string
}

type ChunkedCachedReply struct {
	Size          uintptr
	CachedReplies map[int64]RequestReply
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	maxRaftState int   // snapshot if log grows this big
	dead         int32 // set by Kill()
	make_end     func(string) *labrpc.ClientEnd
	gid          int

	controllerClerk *shardController.Clerk
	config          shardController.Config

	applyCh chan raft.ApplyMsg
	rf      *raft.Raft

	// shard -> mutex
	shardLocks [shardController.NShards]sync.Mutex
	// command is executed one by one, no need for multiple locks for commandExecutor
	// useful for RequestHandler to handle multiple requests for different shards the same time, for checking cachedReply

	// shards in current serveShards to avoid race condition
	serveShardIDs map[int]bool
	// shard -> list of key: value store
	serveShards map[int][]ChunkKVStore
	// does this field need to be persistent?
	// need to be persistent, keep consistent with finishedTransmit
	receivingShards       map[int][]ChunkKVStore
	futureServeConfigNums map[int]int
	futureServeShards     map[int][]ChunkKVStore

	// must be persisted, when applying snapshot, all shard in shadow need to re-send
	// the shards within one group should be in order
	// the assignment of transmitNum -> transmit is ordered as well
	// target gid -> shards and transmitNum
	shadowShardGroups []ShadowShardGroup

	// shard -> clerk id: latest cached reply to avoid command execute twice
	serveCachedReplies     map[int][]ChunkedCachedReply
	receivingCachedReplies map[int][]ChunkedCachedReply
	futureCachedReplies    map[int][]ChunkedCachedReply

	// only in leader
	// shard -> clerk id: reply channel
	clerkChans [shardController.NShards]map[int64]chan RequestReply

	/*
		when config updates,
		all map above need to update the shards which are not managed for current gid,
		for shards which need to be added as well
		Atomically
	*/

	latestAppliedIndex int
	latestAppliedTerm  int

	// send gid -> transmitNumber, need to be persisted
	// the transmit handler will need to check this map to find out
	// if the command is committed or not
	// used in the receive end, per entry for each group at most
	finishedTransmit map[int]TransmitInfo

	// only exists in leader, new leader will need to re-construct
	// cleaned if the raft is no longer the leader
	// used to de-dup requests from the same transmit group
	// just an optimization method, doesn't affect correctness
	onGoingTransmit map[int]TransmitInfo
}

// for raft command
type ShardKVCommand struct {
	ClerkId   int64
	SeqNum    int64
	ConfigNum int

	Shard     int
	Operation string
	Key       string
	Value     string
}

type ConfigUpdateCommand struct {
	Operation string
	Config    shardController.Config
}

type TransmitShardCommand struct {
	Operation string
	// each shard has a unique TransmitNum
	TransmitNum int
	// each chunk data within TransmitNum has a unique number
	ChunkNum int
	// KVstore or chunked Cached replies
	IsKVData bool
	// GID of the transmit end
	GID int
	// configNum of the config of the transmit end
	ConfigNum int
	// the transmit shard
	Shard                 int
	ShardKVStoreChunk     ChunkKVStore
	ShardCachedReplyChunk ChunkedCachedReply
	// the kv data is sent first, then the replies data
	IsLastChunk bool
}

/*


shard -> clerk id: reply channel
serveClerkChans map[int]map[int64]chan RequestReply
does this need a shard map ?
not necessary, the only purpose of the map is to use
different locks for different shards

better to be a list of NShards,
the configs when the request arrives and the request is applied
may differ,
the shard when the request is applied may not exist when the request arrives
so we can't use serveClerkChans,
must be allShardClerkChans

which states need to be persistent:
	config          shardController.Config
	// shard -> key: value store
	serveShards       map[int]map[string]string
	futureServeShards map[int]map[string]string
	shadowShards      map[int]map[string]string

	// shard -> clerk id: latest cached reply
	serveCachedReplies  map[int]map[int64]RequestReply
	futureCachedReplies map[int]map[int64]RequestReply
	shadowCachedReplies map[int]map[int64]RequestReply
	need to be persistent
*/

/************************ end of server *********************/

/*************** clerk-kvServer RPC definition ***************/
// requests and replies via RequestHandler
type RequestArgs struct {
	ClerkId   int64
	SeqNum    int64
	ConfigNum int

	Shard     int
	Operation string
	Key       string
	// only used for Put and Append
	Value string
}

type RequestReply struct {
	ClerkId int64
	SeqNum  int64

	ConfigNum int
	Shard     int

	// whether this operation has been committed and applied successfully, if not Succeeded, retry is needed for the client
	Succeeded bool
	// wrong group for the shard
	WrongGroup bool
	// the leader hasn't update the config to the request configNum yet
	// or the leader hasn't received the shard yet
	WaitForUpdate bool

	// if the operation is too large
	SizeExceeded bool
	// only used for Get
	Value string
	// the key exists in kvStore, only used for get and append
	Exists bool
}

/************** end of clerk-kvServer RPC definition **************/

/*************** kvServer-kvServer shard transmit definition ***************/
type TransmitShardArgs struct {
	Operation string
	// each shard has a unique TransmitNum
	TransmitNum int
	// each chunk data within TransmitNum has a unique number
	ChunkNum int
	// KVstore or chunked Cached replies
	IsKVData bool
	// GID of the transmit end
	GID int
	// configNum of the config of the transmit end
	ConfigNum int
	// the transmit shard
	Shard      int
	ShardChunk map[string]string
	ReplyChunk map[int64]RequestReply
	// the kv data is sent first, then the replies data
	IsLastChunk bool
}
type TransmitShardReply struct {
	Operation   string
	TransmitNum int
	ChunkNum    int
	IsKVData    bool
	GID         int
	ConfigNum   int
	Shard       int

	Succeeded    bool
	SizeExceeded bool
}

/************** end of kvServer-kvServer shard transmit definition **************/
