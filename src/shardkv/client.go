package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardController to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	mathRand "math/rand"
	"time"

	"6.5840/labrpc"
	"6.5840/shardController"
)

// which shard is a key in?
// please use this function,
// and please do not change it.
/*
need to use a string hashing function
*/

func key2shard(key string) int {
	hashCode := 0
	hashSize := shardController.NShards
	for _, c := range key {
		hashCode = (int(c)*33 + hashCode) % hashSize
	}
	hashCode = hashCode % hashSize
	return hashCode
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// the tester calls MakeClerk.
//
// ctrlers[] is needed to call shardController.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	TempDPrintf("MakeClerk() is called()\n")
	ck := new(Clerk)
	ck.clerkId = nrand()
	ck.seqNum = 1
	ck.sc = shardController.MakeClerk(ctrlers)
	ck.make_end = make_end
	TempDPrintf("MakeClerk() finished with ck: %v\n", ck)
	return ck
}

/*
the server receives configNum == 0?
*/

func (ck *Clerk) Get(key string) string {
	ck.seqNum++
	args := RequestArgs{
		ClerkId: ck.clerkId,
		SeqNum:  ck.seqNum,

		Shard:     key2shard(key),
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

		Shard:     key2shard(key),
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

		Shard:     key2shard(key),
		Operation: APPEND,
		Key:       key,
		Value:     value,
	}
	ck.sendRequest(&args)
}

func (ck *Clerk) sendRequest(args *RequestArgs) *RequestReply {
	replyChan := make(chan RequestReply)
	go ck.sendRequestWithChan(args, replyChan)
	var reply RequestReply
	quit := false
	for !quit {
		select {
		case reply = <-replyChan:
			quit = true
		case <-time.After(1 * time.Second):
			ck.mu.Lock()
			TempDPrintf("sendRequest() clerk's request hasn't finished for args: %v", args)
			ck.mu.Unlock()
		}
	}
	return &reply
}

/*
This function sends request to kvServer, and handles retries
The ConfigNum is not set in the request, since it may need to query the controller and change
*/
func (ck *Clerk) sendRequestWithChan(args *RequestArgs, replyChan chan RequestReply) {
	TempDPrintf("sendRequestWithChan() Clerk: %v is called with %v\n", ck.clerkId, args)
	for ck.config.Num == 0 {
		// init config
		ck.config = ck.sc.Query(-1)
		if ck.config.Num == 0 {
			time.Sleep(time.Duration(CHECKCONFIGTIMEOUT) * time.Millisecond)
		}
	}
	var reply RequestReply
	quit := false
	for !quit {
		gid := ck.config.Shards[args.Shard]
		ck.mu.Lock()
		args.ConfigNum = ck.config.Num
		ck.mu.Unlock()
		var servers []*labrpc.ClientEnd
		if servernames, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servernames); si++ {
				srv := ck.make_end(servernames[si])
				servers = append(servers, srv)
			}
		} else {
			log.Fatalf("sendRequestWithChan() Clerk Config %v doesn't have gid: %v\n", ck.config, gid)
		}

		TempDPrintf("sendRequestWithChan() sends to group: %v, receives args: %v with config: %v\n", gid, args, ck.config)
		reply = ck.sendToServers(args, servers)
		if !reply.Succeeded {
			// ask controler for the latest configuration.
			ck.config = ck.sc.Query(-1)
			TempDPrintf("sendRequestWithChan() Args: %v, sends to the wrong group: %v, get reply: %v, new config: %v\n", args, gid, reply, ck.config)
		} else {
			// Succeeded
			quit = true
		}

	}
	TempDPrintf("sendRequestWithChan() Clerk: %v, finished with %v for args: %v\n", ck.clerkId, reply, args)
	replyChan <- reply
}

/*
this function is identical to kvraft sendRequest,
it sends request to the servers in a group
when returns, it must be the case that the request is succeeded
or the request sends to a wrong group
*/
func (ck *Clerk) sendToServers(args *RequestArgs, servers []*labrpc.ClientEnd) RequestReply {
	var reply RequestReply
	for server := 0; server < len(servers); server++ {
		tempReply := RequestReply{}
		TempDPrintf("sendToServers() calls server ShardKV.RequestHandler: %v, with args: %v\n", server, args)
		ok := servers[server].Call("ShardKV.RequestHandler", args, &tempReply)
		TempDPrintf("sendToServers() sent to %v, got tempReply: %v for args: %v\n", server, tempReply, args)
		if !ok {
			// failed server or network disconnection
			TempDPrintf("sendToServers() sent to %v, got tempReply: %v for args: %v got disconnected\n", server, tempReply, args)
		}

		if tempReply.WrongGroup || tempReply.Succeeded {
			// contact wrong group or
			// the raft server commits and kvServer applies
			reply = tempReply
			break
		} else if tempReply.WaitForUpdate {
			// the server is a leader server, but the server hasn't updated the config to args.ConfigNum yet
			time.Sleep(time.Duration(CHECKCONFIGTIMEOUT) * time.Millisecond)
			server--
		} else {
			// contact a non-leader server
			// or a leader server with size too large
			if tempReply.SizeExceeded {
				log.Fatalf("sendToServers() with args: %v, command is too large, max allowed command size is %v\n", args, MAXKVCOMMANDSIZE)
			}
			TempDPrintf("sendToServers() sent to leader %v, got tempReply: %v for args: %v not successful\n", server, tempReply, args)
			server = mathRand.Intn(len(servers))
		}
		TempDPrintf("sendToServers() finishes server ShardKV.RequestHandler: %v, for args: %v with reply: %v\n", server, args, reply)
	}
	TempDPrintf("sendToServers() finishes with %v\n", reply)
	return reply
}
