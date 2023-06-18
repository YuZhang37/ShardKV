package raft

import (
	"math/rand"
	"time"
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Server = rf.me
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
	}
	logSize := len(rf.log)
	up_to_date := true
	if logSize > 0 {
		up_to_date = args.LastLogTerm > rf.log[logSize-1].Term || (args.LastLogTerm == rf.log[logSize-1].Term && args.LastLogIndex >= logSize)
		DPrintf(" %v up-to-date as %v: %v\n", args.CandidateId, rf.me, up_to_date)
	}

	if up_to_date && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.msgReceived = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		DPrintf(" %v already votes for %v at term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	}
	/*
		before the reply is sent back to the candidate,
		if the server crashes, it behaves just like the server never receives the request.
	*/
	rf.persist()
}

/*
this function is guaranteed to return
and send exact one reply to ch, the max time latency is RPC call timeout
max RPC call time must be << election timeout, otherwise,
before previous election returns result, new election is issued,
the leader won't be able to be selected.
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan RequestVoteReply) {
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		reply.Server = server
		reply.Term = 0 // the election will take max()
		reply.VoteGranted = false
	}
	ch <- reply
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// rand.Seed(time.Now().Unix())
	rand.Seed(int64(rf.me))
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		randomTime := rand.Intn(rf.randomRange)
		time.Sleep(time.Duration((randomTime + rf.eleTimeOut)) *
			time.Millisecond)
		rf.mu.Lock()
		if rf.role == LEADER {
			rf.mu.Unlock()
			continue
		}
		if rf.msgReceived {
			rf.msgReceived = false
			rf.mu.Unlock()
			continue
		}
		// follower or candidate, not receiving msg
		go rf.Election()

		DPrintf("%v timeout: %v at term %v\n", rf.me, randomTime+rf.eleTimeOut, rf.currentTerm)
		DPrintf("\n%v will start election at term %v\n", rf.me, rf.currentTerm+1)

		// may need to add some logic to timinate the election if not finished before election timeout
		rf.mu.Unlock()
	}
}

// election needs to be terminated
// 1. when election timeout goes off
// 2. a new leader is discovered (majority will deny this request)
// (a new candidate with higher term is not a problem)
/*
does the election need to be terminated?
when a new leader is established:
if the current term is ≤ leader term, the votes will be denied by majority
if the current term is > leader term, may revoke the leader if the leader hasn't committed any entry
otherwise, the leader will turn to follower,
but the voting will still be denied by the majority due to missing committed entries from the leader, a new election will be held, and the leader server very likely be selected leader again.

no need to be terminated for new leader establishing.

do need to terminate for timeout?
if the rpcs require too long to complete, then the election thread
could accumulate
the outdated voting may establish a stale leader,
not a problem, when it sends out requests, it will contacted by higher term and it turns to follower.
for correctness, no need to terminate,
but for memory efficiency, probably should

ELection function is long, may need to refactor to small pieces
*/

func (rf *Raft) Election() {

	rf.mu.Lock()
	/*
		between detecting msgReceived false and Election(), there can be other candidates request to vote, and the server votes for that candidate, updating currentTerm and votedFor
	*/
	if rf.msgReceived {
		rf.mu.Unlock()
		return
	}

	// change all relevant states to be candidate
	rf.role = CANDIDATE
	rf.currentLeader = -1
	rf.currentTerm++
	rf.votedFor = rf.me
	lastTerm := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: len(rf.log),
		LastLogTerm:  lastTerm,
	}
	term := rf.currentTerm

	// not necessary to persist here, if the server crashes, it can start over the election with previous term just like crash before starting election
	rf.persist()
	rf.mu.Unlock()

	// receive vote replies from all sending goroutines
	ch := make(chan RequestVoteReply)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, ch)
	}

	highestTerm := args.Term
	countVotes := 1
	i := 0
	for i = 0; i < len(rf.peers)-1; i++ {
		reply := <-ch
		if reply.Term > highestTerm {
			// request vote for reply.Term failed
			highestTerm = reply.Term
			break
		}
		if reply.VoteGranted {
			countVotes++
			DPrintf("%v grants vote to %v at term %v\n", reply.Server, rf.me, term)
		} else {
			DPrintf("%v doesn't grant vote to %v at term %v\n", reply.Server, rf.me, term)
		}
		if countVotes >= len(rf.peers)/2+1 {
			break
		}
	}

	//unblock the sendRequestVote goroutines
	go func(count int) {
		for i := 0; i < count; i++ {
			<-ch
		}
	}(len(rf.peers) - 1 - i - 1)

	DPrintf("total votes: %v to %v at term %v\n", countVotes, rf.me, term)
	rf.mu.Lock()
	/*
		if role is not candidate, it means some higher term candidate
		has requested vote from this server, and the server grants the vote
		and the current role is follower
	*/
	if rf.role == CANDIDATE && countVotes >= len(rf.peers)/2+1 {
		rf.role = LEADER
		rf.currentLeader = rf.me
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndices[i] = len(rf.log) + 1
			rf.matchIndices[i] = 0
			rf.issuedEntryIndices[i] = 0
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.HeartBeat(i)
		}

		// stop the previous rf.trailingReplyChan if this server was a leader
		close(rf.trailingReplyChan)

		rf.trailingReplyChan = make(chan AppendEntriesReply)
		go rf.HandleTrailingReply()
		DPrintf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
	} else {
		if highestTerm > rf.currentTerm {
			rf.currentTerm = highestTerm
		}
		DPrintf("%v failed the election at term %v\n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}
