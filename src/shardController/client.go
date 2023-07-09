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
	ck.seqNum = 1
	ck.leaderId = mathRand.Intn(len(ck.servers))
	return ck
}

/*
This function sends request to kvServer, and handles retries
*/
func (ck *Clerk) sendRequest(args *ControllerRequestArgs) *ControllerReply {
	TempDPrintf("sendRequest() is called with %v\n", args)
	var reply ControllerReply
	quit := false
	for !quit {
		tempReply := ControllerReply{}
		ok := ck.servers[ck.leaderId].Call("ShardController.RequestHandler", args, &tempReply)
		TempDPrintf("sendRequest() sent to %v, got tempReply: %v for args: %v\n", ck.leaderId, tempReply, args)
		if !ok {
			TempDPrintf("sendRequest() sent to %v, got tempReply: %v for args: %v got disconnected\n", ck.leaderId, tempReply, args)
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
			TempDPrintf("sendRequest() sent to leader %v, got tempReply: %v for args: %v not successful\n", ck.leaderId, tempReply, args)
			// the raft server or the kv server is killed or no longer the leader
			ck.leaderId = mathRand.Intn(len(ck.servers))
		}
	}
	TempDPrintf("sendRequest() finishes with %v\n", reply)
	return &reply
}

func (ck *Clerk) Query(num int) Config {
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation: QUERY,
		QueryNum:  num,
	}
	reply := ck.sendRequest(&args)
	return reply.Config
}

func (ck *Clerk) Join(groups map[int][]string) {
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation:    JOIN,
		JoinedGroups: groups,
	}
	ck.sendRequest(&args)
}

func (ck *Clerk) Leave(gids []int) {
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation: LEAVE,
		LeaveGIDs: gids,
	}
	ck.sendRequest(&args)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seqNum++
	args := ControllerRequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

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
