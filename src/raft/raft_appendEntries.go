package raft

import (
	//	"bytes"
	"log"
	"time"
	//	"6.824/labgob"
)

// for heartbeat: send and harvest is 1 on 1, and send part definitely sends to the channel, no need for stopChan
func (rf *Raft) HeartBeat(server int) {
	replyChan := make(chan AppendEntriesReply)
	for !rf.killed() && rf.isLeader() {
		go rf.SendAppendEntries(server, -1, replyChan)
		go rf.HarvestHeartbeatReply(replyChan)
		time.Sleep(time.Duration(rf.hbTimeOut) * time.Millisecond)
	}

}

/*
heartbeat will only update the followers's commitIndex
and leader's term
*/
func (rf *Raft) HarvestHeartbeatReply(replyChan chan AppendEntriesReply) {
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

/*
the service using Raft (e.g. a k/v server) wants to start
agreement on the next command to be appended to Raft's log. if this
server isn't the leader, returns false. otherwise start the
agreement and return immediately. there is no guarantee that this
command will ever be committed to the Raft log, since the leader
may fail or lose an election. even if the Raft instance has been killed,
this function should return gracefully.

the first return value is the index that the command will appear at
if it's ever committed. the second return value is the current
term. the third return value is true if this server believes it is
the leader.

if the command sends to the valid leader, entry will be appended to the leader's local log and returns index, term, true,
an AppendCommand goroutine is issued to commit the command
otherwise returns 0, 0, false
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.role != LEADER {
		AppendEntriesDPrintf("Command sends to %v, which is not a leader, the leader is %v\n", rf.me, rf.currentLeader)
		return -1, -1, false
	}

	AppendEntriesDPrintf("Command sends to %v, which is a leader\n", rf.me)
	AppendEntriesDPrintf("Start processing...\n")

	//append to the leader's local log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log) + 1,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	AppendEntriesDPrintf("Command is appended on %v at index of %v\n", rf.me, len(rf.log))

	go rf.AppendCommand(entry.Index)
	return entry.Index, entry.Term, true
}

func (rf *Raft) AppendCommand(index int) {

	rf.mu.Lock()
	entry := rf.log[index-1]
	numOfPeers := len(rf.peers)
	AppendEntriesDPrintf("....AppendCommand at server %v: %v\n", rf.me, entry)
	/*
		the reason we need a stopChan for each harvesting thread, is that the thread
		is not guaranteed to return, may loop for sending RPC, and when the loop
		exit for the condition that the server is not valid, ch0 may not receive
		messages, and the harvesting thread is halted.

		on the other hand, harvest is guaranteed to send exactly
		one message to AppendCommand() when the leader becomes invalid,

		1. harvest may detect this before AppendCommand, and don't send the reply on ch0, it can pass with timerChan
		2. harvest may detect this after AppendCommand, and wait for receiving on ch0, it blocks

		add quitChan to unblock situation 2
		to avoid block block stopChan, record the servers we have
		received replies, don't send messages to their stopChans

		infinite loop:
		when the leader is valid, and the issued entry is the last entry, and the corresponding server failed,
		then this request is sending infinitely
	*/

	// channel for harvesting threads communicating with AppendCommand() thread
	ch0 := make(chan AppendEntriesReply)
	// channels for AppendCommand() thread to terminate harvesting threads
	stopChans := make([]chan int, numOfPeers)
	for i := 0; i < len(stopChans); i++ {
		stopChans[i] = make(chan int)
	}

	for i := 0; i < numOfPeers; i++ {
		if i == rf.me {
			continue
		}
		// channel for send thread to send the reply to harvest thread
		ch := make(chan AppendEntriesReply)

		// there could be many outstanding appendEntries to the
		// same server issued by AppendCommand(comm1) and AppendCommand(comm2)
		// the args.entries to send may be longer than this new entry
		// doesn't matter, AppendEntries will handle them
		go rf.SendAppendEntries(i, entry.Index, ch)
		go rf.HarvestAppendEntriesReply(entry.Index, ch0, stopChans[i], ch)

	}

	rf.mu.Unlock()

	/*
		periodically time out to check commitIndex
		accommodate the case where AppendCommand(comm2) may commit comm1,
		also it's possible AppendCommand(comm1) can commit comm2
	*/
	timerChan := make(chan int)
	quitTimerChan := make(chan int)
	go rf.CheckCommitTimeOut(quitTimerChan, timerChan)

	harvestedServers := rf.TryCommit(ch0, timerChan, numOfPeers, entry)

	// stop the timer thread
	go func(ch chan int) {
		ch <- 1
	}(quitTimerChan)

	rf.QuitBlockedHarvests(numOfPeers, harvestedServers, stopChans)

	go rf.ApplyCommand()
}

/*
returns a map containing all handled harvests,
all handled harvests are guaranteed to be not blocked,
all unhandled harvests can send requests at most one more time (RPC timeout time) and get blocked, then all replies will be redirected to global harvest.

blocked
it can have infinite loop:
when the leader keeps valid, and the last entry
fails to append on the majority of the followers

when tryCommit returns, the command may or may not committed
the leader may become a follower

the higherTerm reply may redirected to global harvest,
but it will be dropped and won't be processed.
*/
func (rf *Raft) TryCommit(ch0 chan AppendEntriesReply, timerChan chan int, numOfPeers int, entry LogEntry) map[int]bool {

	// the leader has appended the entry
	successCount := 1
	tryCommit := true
	harvestedServers := make(map[int]bool)

	for tryCommit && !rf.killed() && rf.isLeader() {
		select {
		case reply := <-ch0:
			harvestedServers[reply.Server] = true
			rf.mu.Lock()
			if reply.Success {
				successCount++
				// to cover the case AppendCommand(comm2) finishes before AppendCommand(comm1)
				if rf.matchIndices[reply.Server] < reply.LastAppendIndex {
					rf.matchIndices[reply.Server] = reply.LastAppendIndex
				}
				if rf.nextIndices[reply.Server] < reply.LastAppendIndex+1 {
					rf.nextIndices[reply.Server] = reply.LastAppendIndex + 1
				}
			} else if reply.HigherTerm {
				// contact a server with higher term
				// this server may be the new leader or just a follower or a candidate
				rf.role = FOLLOWER
				rf.currentTerm = reply.Term
				// all threads need to be stopped
				tryCommit = false
			}
			// the reply when failed with higher issue index is dropped

			if successCount > numOfPeers/2 {
				if rf.commitIndex < entry.Index {
					rf.commitIndex = entry.Index
				}
				tryCommit = false
			}
			rf.mu.Unlock()
		case <-timerChan:
			rf.updateCommit()
			rf.mu.Lock()
			// start(comm2) may commit a higher index
			if rf.commitIndex >= entry.Index {
				tryCommit = false
			}
			rf.mu.Unlock()
		}
	}
	return harvestedServers
}

/*
for all harvest goroutines which don't reply, send out a message to redirect the replies to global harvest or just drop all replies
*/
func (rf *Raft) QuitBlockedHarvests(numOfPeers int, harvestedServers map[int]bool, stopChans []chan int) {
	AppendEntriesDPrintf("....harvestedServers: %v\n", harvestedServers)
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me || harvestedServers[i] {
			continue
		}
		go func(ch chan int, server int) {
			quitMessage := 1
			AppendEntriesDPrintf("....quitMessage: %v is sent to %v\n", quitMessage, server)
			ch <- quitMessage
		}(stopChans[i], i)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	funct := 1
	if len(args.Entries) == 0 {
		funct = 2
	}

	AppendEntries2DPrintf(funct, "Command from %v received by %v at index of %v\n", args.LeaderId, rf.me, args.PrevLogIndex+1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// the reply's term may be less than leader's
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.IssueEntryIndex = args.IssueEntryIndex

	if rf.currentTerm > args.Term {
		// leader contacts one server with higher term
		reply.Success = false
		reply.HigherTerm = true
		reply.MisMatched = false
		reply.LastAppendIndex = 0
		return
	}

	// follower or candidate or leader with smaller term (≤ args.Term)
	if rf.currentLeader != args.LeaderId || rf.currentTerm != args.Term {
		rf.currentTerm = args.Term
		rf.role = FOLLOWER
		rf.currentLeader = args.LeaderId
		rf.currentReceived = 0
	}

	rf.msgReceived = true

	reply.HigherTerm = false

	if len(rf.log) < args.PrevLogIndex {
		// log doesn't contain an entry at PrevLogIndex
		reply.Success = false
		reply.MisMatched = true
		return
	}

	if args.PrevLogIndex > 1 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// log entry at PrevLogIndex doesn't equal to PrevLogTerm
		reply.Success = false
		reply.MisMatched = true
		return
	}

	reply.Success = true
	reply.HigherTerm = false
	reply.MisMatched = false

	// not a heartbeat
	if len(args.Entries) > 0 {
		if args.Entries[len(args.Entries)-1].Index <= rf.currentReceived {
			// no newly appended entries in args
			// suppose AppendCommand(comm1), AppendCommand(comm2)
			// comm2 may get to the server earlier than comm1
			// comm1 carries comm2
			reply.LastAppendIndex = len(args.Entries)
		} else {
			// have new entries, append
			// the server's log matches leader's log up through PrevLogIndex
			// now appends new entries to the server's log
			i, j := args.PrevLogIndex, 0
			for i < len(rf.log) && j < len(args.Entries) {
				if rf.log[i].Term != args.Entries[j].Term {
					rf.log[i] = args.Entries[j]
				}
				i, j = i+1, j+1
			}

			if i < len(rf.log) {
				// removing unmatched trailing log entries in the server
				rf.log = rf.log[:i]
			}

			for j < len(args.Entries) {
				// append new log entries to the server's log
				rf.log = append(rf.log, args.Entries[j])
				j++
			}
			rf.currentReceived = args.Entries[len(args.Entries)-1].Index
			reply.LastAppendIndex = len(rf.log)
		}
	}

	//update commitIndex both for heartbeat and appendEntries
	leaderCommitIndex := args.LeaderCommitIndex
	if rf.currentReceived < leaderCommitIndex {
		leaderCommitIndex = rf.currentReceived
	}
	if rf.commitIndex < leaderCommitIndex {
		rf.commitIndex = leaderCommitIndex
		go rf.ApplyCommand()
	}

	AppendEntries2DPrintf(funct, "Command from %v is appended by %v at index of %v\n", args.LeaderId, rf.me, len(rf.log))
	AppendEntries2DPrintf(funct, "logs on server %v: %v\n", rf.me, rf.log)

}

/*
this function will guarantee to send to ch exact one reply, the longest time it takes is the RPC timeout.
for heartbeat entries will be entries
for appendEntries, entries will log[rf.nextIndices[server]-1:]
*/
func (rf *Raft) SendAppendEntries(server int, issueEntryIndex int, ch chan AppendEntriesReply) {
	rf.mu.Lock()

	if rf.issuedEntryIndices[server] < issueEntryIndex {
		rf.issuedEntryIndices[server] = issueEntryIndex
	}
	funct := 1
	if issueEntryIndex < 0 {
		funct = 2
	}
	prevLogIndex := 0
	prevLogTerm := 0
	entries := make([]LogEntry, 0)
	next := rf.nextIndices[server]
	AppendEntries2DPrintf(funct, "next index to %v is %v \n", server, next)
	if next < 1 {
		log.Fatalf("fatal: next index to %v is %v \n", server, next)
	}
	if next > 1 {
		// next is at least to be 1, >1 means there is a previous entry
		prevLogIndex = rf.log[next-2].Index
		prevLogTerm = rf.log[next-2].Term
	}

	if issueEntryIndex != -1 {
		// not a heartbeat
		entries = rf.log[next-1:]
	}

	args := AppendEntriesArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		IssueEntryIndex:   issueEntryIndex,
		PrevLogIndex:      prevLogIndex,
		PrevLogTerm:       prevLogTerm,
		Entries:           entries,
		LeaderCommitIndex: rf.commitIndex,
	}
	AppendEntries2DPrintf(funct, "args: %v at %v to %v\n", args, rf.me, server)
	AppendEntries2DPrintf(funct, "log: %v at %v \n", rf.log, rf.me)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// AppendEntriesDPrintf("Server %v RPC request vote to %v failed!\n", rf.me, server)
		reply.Server = server
		reply.IssueEntryIndex = 0
		reply.LastAppendIndex = 0
		reply.Term = 0
		reply.Success = false
		reply.HigherTerm = false
		reply.MisMatched = false
	}
	ch <- reply
}

// although harvest may issue lots of sends
// but at any point in time, there is exactly one send and one harvest
// and send part are ensured to send to the channel exactly once
func (rf *Raft) HarvestAppendEntriesReply(issuedIndex int, sendReplyChan chan AppendEntriesReply, stopChan chan int, getReplyChan chan AppendEntriesReply) {
	/*
		get reply and handle retries
		assume we can always get exactly one reply from getReplyChan
		if rf is killed or no longer the leader,
			the reply is dropped, no retries
			won't block AppendCommand(), it has timerChan
		if issued index > current index, no retries
		if the index is committed, send the result to global harvest channel

		send to sendReplyChan only if:
			1. reply.success is true
			2. reply.success is false but HigherTerm is true
		if reply.success is false and HigherTerm is false
			retries

	*/
	for {
		// getReplyChan may be stuck on sending channel
		reply := <-getReplyChan
		/*
			suppose harvest waits on getReplyChan and the server becomes the follower, then when we try to send
			the message, quitMsg must be 2

		*/
		rf.mu.Lock()
		hasHigherIndex := rf.issuedEntryIndices[reply.Server] > reply.IssueEntryIndex
		isLeader := rf.role == LEADER
		rf.mu.Unlock()
		quitMsg := 0
		if reply.Success || reply.HigherTerm || hasHigherIndex || rf.killed() || !isLeader {
			// include to
			// detect the server is not a valid leader any more
			// return a false reply, which will be discarded by AppendCommand
			AppendEntriesDPrintf("reply from server %v: %v waiting for sending", reply.Server, reply)
			select {
			case sendReplyChan <- reply:
				AppendEntriesDPrintf("reply from server %v sends to replyChan", reply.Server)
			case quitMsg = <-stopChan:
				AppendEntriesDPrintf("...quitMsg: %v, reply from server %v sends to trailing", quitMsg, reply.Server)
				if reply.Success {
					/*
						if reply.false, the longest time it takes from sending to this point is when RPC call fails,
						the timeout time of that.
						even reply.true, it can take that long as well
						so we can't close the chan before that.
						We close this channel when this server gets elected again, but if this happens too soon,
						reply can still send to a closed channel, the server will panic, other servers can continue election.
						RPC timeout should smaller than election timeout to avoid closed channel causing false fails.
					*/
					rf.trailingReplyChan <- reply
				}
			}
			break
		}

		// retry
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)

		// need to decrement nextIndices[server] if mismatched
		if reply.MisMatched {
			/*
				suppose AppendCommand(comm1), AppendCommand(comm2)
				rf.nextIndices[server] can be higher or lower than issuedIndex-1
			*/
			rf.mu.Lock()
			if rf.nextIndices[reply.Server] <= reply.IssueEntryIndex {
				rf.nextIndices[reply.Server]--
			}
			rf.mu.Unlock()
		}
		go rf.SendAppendEntries(reply.Server, issuedIndex, getReplyChan)
	}
}

/*
just update matchIndices and nextIndices of the server,
created when the server becomes the leader,
and destroyed exactly before the server becomes the leader by close(rf.trailingReplyChan)
matchIndices and nextIndices are volatile, it doesn't matter to update them,
it can update the commitIndex, but it's not necessary since this state is non-volatile and writing to a failed server may have problems
false replies are ignored, including the ones with higherTerm
*/
func (rf *Raft) HandleTrailingReply() {
	AppendEntriesDPrintf("HandleTrailingReply gets running....")
	for reply := range rf.trailingReplyChan {
		AppendEntriesDPrintf("HandleTrailingReply got Reply: %v", reply)
		if !reply.Success {
			// ignore highIndex which can invalidate the current leader
			continue
		}
		rf.mu.Lock()

		if rf.matchIndices[reply.Server] < reply.LastAppendIndex {
			rf.matchIndices[reply.Server] = reply.LastAppendIndex
		}

		if rf.nextIndices[reply.Server] < reply.LastAppendIndex+1 {
			rf.nextIndices[reply.Server] = reply.LastAppendIndex + 1
		}

		rf.mu.Unlock()

	}

}

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand() {
	rf.mu.Lock()
	lastApplied := rf.lastApplied
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	i := lastApplied
	for i < commitIndex {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i].Command, //committed logs will never be changed, no need for locks
			CommandIndex: i + 1,
		}
		rf.applyCh <- msg
		rf.mu.Lock()
		// i + 1
		if rf.lastApplied < i+1 {
			rf.lastApplied = i + 1
			i++
		} else {
			i = rf.lastApplied
		}
		rf.mu.Unlock()
	}
}

/*
even leader turns to follower or server is killed
we can still update the commitIndex since they are volatile
before a new leader is elected
*/
func (rf *Raft) updateCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	smallestIndex := len(rf.log)
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndices[i] < smallestIndex {
			smallestIndex = rf.matchIndices[i]
		}
	}
	if smallestIndex != 0 && rf.log[smallestIndex-1].Term == rf.currentTerm && rf.commitIndex < smallestIndex {
		rf.commitIndex = smallestIndex
	}
}

/*
don't check !rf.killed() && rf.isLeader()
the AppendCommand() thread checks the leader validation,
timer should quit after receiving loop in AppendCommand()
*/
func (rf *Raft) CheckCommitTimeOut(quitChan chan int, timerChan chan int) {

	quit := false
	for !quit {
		time.Sleep(time.Duration(CHECKCOMMITTIMEOUT) * time.Microsecond)
		select {
		case <-quitChan:
			quit = true
		case timerChan <- 1:

		}
	}
}
