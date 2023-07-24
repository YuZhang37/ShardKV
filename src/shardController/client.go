package shardController

import (
	"crypto/rand"
	"log"
	"math/big"
	mathRand "math/rand"

	"6.5840/labrpc"
)

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.fromGroup = -1
	ck.seqNum = 0
	ck.leaderId = mathRand.Intn(len(ck.servers))
	return ck
}

func MakeQueryClerk(servers []*labrpc.ClientEnd, opts ...interface{}) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clerkId = nrand()
	ck.seqNum = -1
	ck.leaderId = mathRand.Intn(len(ck.servers))
	if len(opts) >= 1 {
		var ok bool
		ck.fromGroup, ok = opts[0].(int)
		if !ok {
			log.Fatal("Fatal: MakeClerk() error for shardController. opts[0] expects int")
		}
	}
	if len(opts) >= 2 {
		var ok bool
		ck.clerkId, ok = opts[1].(int64)
		if !ok {
			log.Fatal("Fatal: MakeClerk() error for shardController. opts[1] expects int64")
		}
	}
	return ck
}

/*
This function sends request to kvServer, and handles retries
*/
func (ck *Clerk) sendRequest(args *ControllerRequestArgs) *ControllerReply {
	tempDPrintf("sendRequest() is called with %v\n", args)
	var reply ControllerReply
	quit := false
	for !quit {
		tempReply := ControllerReply{}
		ok := ck.servers[ck.leaderId].Call("ShardController.RequestHandler", args, &tempReply)
		tempDPrintf("sendRequest() sent to %v, got tempReply: %v for args: %v\n", ck.leaderId, tempReply, args)
		if !ok {
			tempDPrintf("sendRequest() sent to %v, got tempReply: %v for args: %v got disconnected\n", ck.leaderId, tempReply, args)
			// server failed or disconnected
			ck.leaderId = mathRand.Intn(len(ck.servers))
			continue
		}
		if tempReply.Succeeded {
			// the raft server commits and kvServer applies
			quit = true
			reply = tempReply
		} else {
			if tempReply.SizeExceeded {
				log.Fatalf("command is too large, max allowed command size is %v\n", MAXCONTROLLERCOMMANDSIZE)
			}
			tempDPrintf("sendRequest() sent to leader %v, got tempReply: %v for args: %v not successful\n", ck.leaderId, tempReply, args)
			// the raft server or the kv server is killed or no longer the leader
			ck.leaderId = mathRand.Intn(len(ck.servers))
		}
	}
	tempDPrintf("sendRequest() finishes with %v\n", reply)
	return &reply
}

func (ck *Clerk) QueryWithSeqNum(queryNum int, seqNum int64) Config {
	if ck.seqNum != -1 {
		log.Fatalf("Fatal: QueryWithSeqNum() is only supported for QueryClerk!")
	}
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  seqNum,

		FromGroup: ck.fromGroup,

		Operation: QUERY,
		QueryNum:  queryNum,
	}
	reply := ck.sendRequest(&args)
	return reply.Config
}

func (ck *Clerk) Query(num int) Config {
	if ck.seqNum == -1 {
		log.Fatalf("Fatal: QueryWithSeqNum() is not supported for QueryClerk!")
	}
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		FromGroup: ck.fromGroup,

		Operation: QUERY,
		QueryNum:  num,
	}
	reply := ck.sendRequest(&args)
	return reply.Config
}

func (ck *Clerk) Join(groups map[int][]string) {
	if ck.seqNum == -1 {
		log.Fatalf("Fatal: QueryWithSeqNum() is not supported for QueryClerk!")
	}
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		FromGroup: ck.fromGroup,

		Operation:    JOIN,
		JoinedGroups: groups,
	}
	ck.sendRequest(&args)
}

func (ck *Clerk) Leave(gids []int) {
	if ck.seqNum == -1 {
		log.Fatalf("Fatal: QueryWithSeqNum() is not supported for QueryClerk!")
	}
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		FromGroup: ck.fromGroup,

		Operation: LEAVE,
		LeaveGIDs: gids,
	}
	ck.sendRequest(&args)
}

func (ck *Clerk) Move(shard int, gid int) {
	if ck.seqNum == -1 {
		log.Fatalf("Fatal: QueryWithSeqNum() is not supported for QueryClerk!")
	}
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		FromGroup: ck.fromGroup,

		Operation:  MOVE,
		MovedShard: shard,
		MovedGID:   gid,
	}
	ck.sendRequest(&args)
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
