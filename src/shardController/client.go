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
fetch the current value for a key.
returns "" if the key does not exist.
keeps trying forever in the face of all other errors.
*/
func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	args := RequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation: GET,
		Key:       key,
	}
	reply := ck.sendRequest(&args)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.seqNum++
	args := RequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation: PUT,
		Key:       key,
		Value:     value,
	}
	ck.sendRequest(&args)
}
func (ck *Clerk) Append(key string, value string) {
	ck.seqNum++
	args := RequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Operation: APPEND,
		Key:       key,
		Value:     value,
	}
	ck.sendRequest(&args)
}

/*
This function sends request to kvServer, and handles retries
*/
func (ck *Clerk) sendRequest(args *RequestArgs) *RequestReply {
	TempDPrintf("sendRequest() is called with %v\n", args)
	var reply RequestReply
	quit := false
	for !quit {
		tempReply := RequestReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.RequestHandler", args, &tempReply)
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
				log.Fatalf("command is too large, max allowed command size is %v\n", MAXKVCOMMANDSIZE)
			}
			TempDPrintf("sendRequest() sent to leader %v, got tempReply: %v for args: %v not successful\n", ck.leaderId, tempReply, args)
			// the raft server or the kv server is killed or no longer the leader
			// if tempReply.LeaderId != -1 {
			// 	ck.leaderId = tempReply.LeaderId
			// } else {
			ck.leaderId = mathRand.Intn(len(ck.servers))
			// }
		}
	}
	TempDPrintf("sendRequest() finishes with %v\n", reply)
	return &reply
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
