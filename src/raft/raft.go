package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"log"
	"math/rand"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	// currentTerm := atomic.LoadUint64(&rf.currentTerm)
	// role := atomic.LoadInt32(&rf.role)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm
	role := rf.role
	return currentTerm, role == LEADER
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) isLeader() bool {
	// ans := atomic.LoadInt32(&rf.role)
	// return ans == LEADER
	rf.mu.Lock()
	defer rf.mu.Unlock()
	ans := rf.role
	return ans == LEADER
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	//heartbeat: not handling appending logic
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// the reply's term may be less than leader's
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.Success = false
		reply.HigherTerm = true
		// ignore the stale leader
	} else {
		//TODO: needs to check matching for appending
		// valid appendEntries
		reply.Success = true
		// follower or candidate or leader with smaller term (≤ args.Term)
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.currentLeader = args.LeaderId
		rf.msgReceived = true
	}
}

func (rf *Raft) SendAppendEntries(server int, ch chan AppendEntriesReply) {
	rf.mu.Lock()
	// higher leader may send request to this server and change
	// some status
	// prevLogIndex := 0
	// prevLogTerm := 0
	// var entries []LogEntry = nil
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// PrevLogIndex:      prevLogIndex,
		// PrevLogTerm:       prevLogTerm,
		// Entries:           entries,
		// LeaderCommitIndex: rf.commitIndex,
	}
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// log.Printf("Server %v RPC request vote to %v failed!\n", rf.me, server)
	}
	ch <- reply
}

func (rf *Raft) HarvestAppendEntriesReply(replyChan chan AppendEntriesReply) {
	reply := <-replyChan
	if rf.killed() || !rf.isLeader() {
		return
	}
	// only deals with heartbeats
	if !reply.Success && reply.HigherTerm {
		rf.mu.Lock()
		rf.currentTerm = reply.Term
		rf.role = FOLLOWER
		rf.msgReceived = false
		rf.currentLeader = -1
		rf.mu.Unlock()

	}
}

func (rf *Raft) HeartBeat(server int) {
	replyChan := make(chan AppendEntriesReply)
	for !rf.killed() && rf.isLeader() {
		go rf.SendAppendEntries(server, replyChan)
		go rf.HarvestAppendEntriesReply(replyChan)
		time.Sleep(time.Duration(rf.hbTimeOut) * time.Millisecond)
	}

}

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
		// log.Printf("Server %v RPC request vote to %v failed!\n", rf.me, server)
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
	randomTime := rand.Intn(rf.randomRange)
	for !rf.killed() {
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

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
		for i := 1; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			rf.nextIndices[i] = len(rf.log) + 1
			rf.matchIndices[i] = 0
		}
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go rf.HeartBeat(i)
		}
		log.Printf("%v wins the election at term %v\n", rf.me, rf.currentTerm)
	} else {
		if highestTerm > rf.currentTerm {
			rf.currentTerm = highestTerm
		}
		log.Printf("%v failed the election at term %v\n", rf.me, rf.currentTerm)
	}
	rf.mu.Unlock()
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	rf.role = FOLLOWER
	rf.msgReceived = false
	rf.currentLeader = -1

	rf.nextIndices = make([]int, len(rf.peers))
	rf.matchIndices = make([]int, len(rf.peers))

	rf.hbTimeOut = HBTIMEOUT
	rf.eleTimeOut = ELETIMEOUT
	rf.randomRange = RANDOMRANGE

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}
