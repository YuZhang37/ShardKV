package raft

import (
	//	"bytes"
	"log"
	"math/rand"
	"time"
	//	"6.824/labgob"
)

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Server = rf.me
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.role = FOLLOWER
	}
	logSize := len(rf.log)
	up_to_date := true
	if logSize > 0 {
		up_to_date = args.LastLogTerm > rf.log[logSize-1].Term ||
			(args.LastLogTerm == rf.currentTerm && args.LastLogIndex >= logSize)
	}

	if up_to_date && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.msgReceived = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		log.Printf(" %v already votes for %v at term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, ch chan RequestVoteReply) {
	// may need to repeat
	reply := RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, &reply)
	if !ok {
		reply.Server = server
		reply.Term = 0
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
		go rf.Election(randomTime + rf.eleTimeOut)
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
*/

func (rf *Raft) Election(timeout int) {

	rf.mu.Lock()
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
	rf.mu.Unlock()
	log.Printf("\n%v starts election at term %v\n", rf.me, term)
	log.Printf("%v timeout: %v at term %v\n", rf.me, timeout, term)

	ch := make(chan RequestVoteReply)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, ch)
	}

	highestTerm := args.Term // rf.Term will only be updated when request voting failed and got higher term
	countVotes := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-ch
		if reply.Term > highestTerm {
			highestTerm = reply.Term
		}
		if reply.VoteGranted {
			countVotes++
			log.Printf("%v grants vote to %v at term %v\n", reply.Server, rf.me, term)
		} else {
			log.Printf("%v doesn't grant vote to %v at term %v\n", reply.Server, rf.me, term)
		}
		if countVotes >= len(rf.peers)/2+1 {
			//unblock the sendRequestVote gorountines
			go func(count int) {
				for i := 0; i < count; i++ {
					<-ch
				}
			}(len(rf.peers) - 1 - i - 1)
			break
		}
	}
	log.Printf("total votes: %v to %v at term %v\n", countVotes, rf.me, term)
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
		rf.trailingReplyChan = make(chan AppendEntriesReply)
		go rf.HandleTrailingReply()
		log.Printf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
	} else {
		/*
			only when the candidate can't get the majority of the votes
			it needs to update the term with the highest one ever seen,
			when the leader issues the first hearbeats, and get response from the servers with higher term, this leader will be back to follower.
		*/
		if highestTerm > rf.currentTerm {
			rf.currentTerm = highestTerm
		}
		log.Printf("%v failed the election at term %v\n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}
