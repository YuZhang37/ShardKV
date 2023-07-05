package raft

import (
	"math/rand"
	"time"
)

// RequestVote RPC handler.
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
		rf.onReceiveHigherTerm(args.Term)
	}
	logSize := len(rf.log) + rf.snapshotLastIndex
	lastLogTerm := 0
	if len(rf.log) == 0 {
		lastLogTerm = rf.snapshotLastTerm
	} else {
		lastLogTerm = rf.log[len(rf.log)-1].Term
	}
	up_to_date := true
	if logSize > 0 {
		up_to_date = args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= logSize)
		ElectionDPrintf(" %v up-to-date as %v: %v\n", args.CandidateId, rf.me, up_to_date)
	}

	if up_to_date && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.msgReceived = true
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		ElectionDPrintf(" %v already votes for %v at term %v\n", rf.me, rf.votedFor, rf.currentTerm)
	}
	/*
		before the reply is sent back to the candidate,
		if the server crashes, it behaves just like the server never receives the request.
	*/
	rf.persistState("server %v responds to request vote from %v on term %v", rf.me, args.CandidateId, args.Term)
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
	rand.Seed(int64(rf.me) + time.Now().Unix())
	for !rf.killed() {
		/*
			pause for a random amount of time between 50 and 350
			milliseconds.
			ms := 50 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		*/
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
		// rf.msgReceived remain false to allow re-election
		go rf.Election()

		ElectionDPrintf("%v timeout: %v at term %v\n", rf.me, randomTime+rf.eleTimeOut, rf.currentTerm)
		ElectionDPrintf("\n%v will start election at term %v\n", rf.me, rf.currentTerm+1)
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
	rf.upgradeToCandidate()
	rf.persistState("server %v leader election", rf.me)
	// not necessary to persist here, if the server crashes, it can start over the election with previous term just like crash before starting election
	rf.mu.Unlock()

	countVotes, reply := rf.sendAndReceiveVotes()

	rf.mu.Lock()
	/*
		if role is not candidate, it means some higher term candidate
		has requested vote from this server, and the server grants the vote
		and the current role is follower
	*/
	if rf.role == CANDIDATE && countVotes >= len(rf.peers)/2+1 {
		rf.upgradeToLeader()
	} else {
		if reply.Term > rf.currentTerm {
			originalTerm := rf.onReceiveHigherTerm(reply.Term)
			rf.persistState("server %v leader election has higher term reply: %v, original term: %v", rf.me, reply.Term, originalTerm)
		}
		ElectionDPrintf("%v failed the election at term %v\n", rf.me, rf.currentTerm)
	}

	rf.mu.Unlock()
}

func (rf *Raft) upgradeToCandidate() {
	// change all relevant states to be candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.role = CANDIDATE
	rf.currentLeader = -1
	rf.currentAppended = 0
	rf.msgReceived = false
}

func (rf *Raft) upgradeToLeader() {
	rf.role = LEADER
	rf.currentLeader = rf.me
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		// nextIndices are guaranteed to ≥ 1
		rf.nextIndices[i] = rf.snapshotLastIndex + len(rf.log) + 1
		rf.matchIndices[i] = 0
		rf.latestIssuedEntryIndices[i] = 0
	}

	// stop the previous rf.trailingReplyChan if this server was a leader
	close(rf.trailingReplyChan)
	// wait for old trailingReply handler to finish
	// ******* May need to unlock and re-lock ************* //
	<-rf.quitTrailingReplyChan
	rf.trailingReplyChan = make(chan AppendEntriesReply)
	go rf.HandleTrailingReply()

	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.HeartBeat(i)
	}

	rf.commitNoop()

	ElectionDPrintf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
	TestDPrintf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
	KVStoreDPrintf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
}

func (rf *Raft) getRequestVoteArgs() RequestVoteArgs {
	lastTerm := 0
	lastIndex := 0
	if len(rf.log) > 0 {
		lastTerm = rf.log[len(rf.log)-1].Term
		lastIndex = rf.log[len(rf.log)-1].Index
	} else {
		lastTerm = rf.snapshotLastTerm
		lastIndex = rf.snapshotLastIndex
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastIndex,
		LastLogTerm:  lastTerm,
	}
	return args
}

// return granted vote count and last reply
func (rf *Raft) sendAndReceiveVotes() (int, RequestVoteReply) {
	rf.mu.Lock()
	args := rf.getRequestVoteArgs()
	numOfPeers := len(rf.peers)
	rf.mu.Unlock()
	// receive vote replies from all sending goroutines
	ch := make(chan RequestVoteReply)
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendRequestVote(i, &args, ch)
	}

	countVotes := 1
	i := 0
	reply := RequestVoteReply{}
	for i = 0; i < numOfPeers; i++ {
		reply = <-ch
		if reply.Term > args.Term {
			// request vote for reply.Term failed when contacting servers with higher term
			break
		}
		if reply.VoteGranted {
			countVotes++
			TestDPrintf("%v grants vote to %v at term %v\n", reply.Server, rf.me, args.Term)
			ElectionDPrintf("%v grants vote to %v at term %v\n", reply.Server, rf.me, args.Term)
		} else {
			ElectionDPrintf("%v doesn't grant vote to %v at term %v\n", reply.Server, rf.me, args.Term)
		}
		if countVotes >= numOfPeers/2+1 {
			break
		}
	}

	//unblock the sendRequestVote goroutines
	go func(count int) {
		for i := 0; i < count; i++ {
			<-ch
		}
	}(numOfPeers - 1 - i - 1)

	ElectionDPrintf("total votes: %v to %v at term %v\n", countVotes, rf.me, args.Term)
	return countVotes, reply
}
