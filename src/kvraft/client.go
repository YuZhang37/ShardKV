package kvraft

import (
	"crypto/rand"
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
		if !ok {
			// server failed or disconnected
			ck.leaderId = mathRand.Intn(len(ck.servers))
			continue
		}
		if tempReply.Succeeded {
			// the raft server commits and kvServer applies
			quit = true
			reply = tempReply
		} else {
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

/*
The kvserver can reply or not:
 1. if the kvserver doesn't reply:
    network partition or failed server,
    the clerk will time out
 2. the kvserver replies:
    a. the kvserver applies successfully
    the clerk get the right result, continue
    b. the kvserver is not the leader
    the clerk will try the new leaderId if reply contains one
    c. can't commit, stuck in infinite loop
    retry a new kvserver? yes, seems ok
    d. can't commit, due to losing the leader role
    randomly pick a new kvserver and retry
 3. the kvserver will reply but taking too long to process the request
    will this time out? I think so
    how to differentiate this case with 2.c? can't
    how to differentiate this case with 1? can't
    just retry a new server, at some point, the follower will redirect the request to this server again, if the reply is cached, it can respond instantly

The function has a loop which may retry to get the result infinitely
func (ck *Clerk) sendRequest(args *RequestArgs) *RequestReply
*/
