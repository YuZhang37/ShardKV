package raft

import (
	"log"
	"time"
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
and update the term on leader if a reply with higher term
don't handle the failed RPC calls and mismatched logs
*/
func (rf *Raft) HarvestHeartbeatReply(replyChan chan AppendEntriesReply) {
	reply := <-replyChan
	if rf.killed() || !rf.isLeader() {
		return
	}
	// only deals with replies with higher terms
	if reply.HigherTerm {
		rf.mu.Lock()
		rf.msgReceived = false
		originalTerm := rf.onReceiveHigherTerm(reply.Term)
		rf.persist("server %v heartbeat replies on %v with higher term: %v, original term: %v", rf.me, reply.Server, reply.Term, originalTerm)
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
otherwise returns -1, -1, false
*/
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.killed() || rf.role != LEADER {
		AppendEntriesDPrintf("Command %v sends to %v, which is not a leader, the leader is %v\n", command, rf.me, rf.currentLeader)
		return -1, -1, false
	}

	AppendEntriesDPrintf("Command %v sends to %v, which is a leader for term: %v\n", command, rf.me, rf.currentTerm)
	AppendEntriesDPrintf("Start processing...\n")

	//append to the leader's local log
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log) + 1,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist("server %v appends entry %v to its log", rf.me, entry)

	AppendEntriesDPrintf("Command %v is appended on %v at index of %v\n", command, rf.me, len(rf.log))

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
				if rf.matchIndices[reply.Server] < reply.LastAppendedIndex {
					rf.matchIndices[reply.Server] = reply.LastAppendedIndex
				}
				if rf.nextIndices[reply.Server] < reply.LastAppendedIndex+1 {
					rf.nextIndices[reply.Server] = reply.LastAppendedIndex + 1
				}
			} else if reply.HigherTerm && reply.Term > rf.currentTerm {
				// contact a server with higher term
				// this server may be the new leader or just a follower or a candidate
				originalTerm := rf.onReceiveHigherTerm(reply.Term)
				rf.persist("server %v try to commit %v replies on %v with higher term: %v, original term: %v", rf.me, entry, reply.Server, reply.Term, originalTerm)
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
			rf.updateCommit(entry.Index)
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

/*
	follower or candidate server will execute this function for heartbeat or appendEntries,

args.IssueEntryIndex is -1 no entries for heartbeat
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	funct := 1
	if args.IssueEntryIndex == -1 {
		funct = 2
	}

	AppendEntries2DPrintf(funct, "Command from %v received by %v at index of %v\n", args.LeaderId, rf.me, args.PrevLogIndex+1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// the reply's term may be less than leader's
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.IssueEntryIndex = args.IssueEntryIndex
	reply.NextIndex = args.PrevLogIndex + 1

	if rf.currentTerm > args.Term {
		// leader contacts one server with higher term
		reply.Success = false
		reply.HigherTerm = true
		reply.MisMatched = false
		reply.LastAppendedIndex = 0
		return
	}

	/**** process both heartbeats and appendEntries ****/

	// 1. follower or candidate or leader with smaller term (< args.Term)
	// 2. candidate with term = args.Term, if -1, then it's the first time this server contacts with the leader, updates all states just like encountering a higher term.
	// => when leader sends out the first heartbeat, all followers's leader will be set to be it, and votedFor will be reset to be -1
	// 2 is wrong, can lead to split brain

	persist_state := false
	if rf.currentTerm < args.Term {
		rf.onReceiveHigherTerm(args.Term)
		persist_state = true
		rf.currentLeader = args.LeaderId
	} else if rf.currentLeader == -1 {
		// must be a candidate with term == leader's term and currentLeader being -1,
		rf.role = FOLLOWER
		rf.currentLeader = args.LeaderId
		rf.currentAppended = 0
	}

	// valid heartbeat or appendEntries
	rf.msgReceived = true

	reply.HigherTerm = false

	if len(rf.log) < args.PrevLogIndex {
		// 1. log doesn't contain an entry at PrevLogIndex
		reply.Success = false
		reply.MisMatched = true
		if persist_state {
			rf.persist("server %v log (shorter) mismatch with the leader", rf.me)
		}
		return
	}

	if args.PrevLogIndex > 1 && rf.log[args.PrevLogIndex-1].Term != args.PrevLogTerm {
		// 2. log entry at PrevLogIndex doesn't equal to PrevLogTerm
		reply.Success = false
		reply.MisMatched = true
		if persist_state {
			rf.persist("server %v log (term) mismatch with the leader", rf.me)
		}
		return
	}

	reply.Success = true
	reply.HigherTerm = false
	reply.MisMatched = false

	// not a heartbeat
	// if args.IssueEntryIndex != -1 {

	// for appendEntries: args.Entries can still be empty if the nextIndices[Server] gets updated before preparing the args
	// len(args.Entries) == 0 includes heartbeat and appendEntries
	if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.currentAppended {
		/*
			no new entries in args needed to append on the server
			suppose AppendCommand(comm1), AppendCommand(comm2)
			comm2 may get to the server earlier than comm1
			comm1 carries comm2
		*/
		reply.LastAppendedIndex = rf.currentAppended
	} else {
		/*
			new entries in args which need to append,
			the server's log matches leader's log at least up through PrevLogIndex
			now appends new entries to the server's log, the new entries may not start from PreLogIndex + 1
		*/
		i := args.PrevLogIndex
		j := 0
		for i < len(rf.log) && j < len(args.Entries) {
			if rf.log[i].Term != args.Entries[j].Term {
				rf.log[i] = args.Entries[j]
			}
			i++
			j++
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
		rf.currentAppended = args.Entries[len(args.Entries)-1].Index
		reply.LastAppendedIndex = len(rf.log)
		rf.persist("server %v appends new entries to %v", rf.me, len(rf.log))
	}

	// }

	//update commitIndex both for heartbeat and appendEntries
	leaderCommitIndex := args.LeaderCommitIndex
	if rf.currentAppended < leaderCommitIndex {
		leaderCommitIndex = rf.currentAppended
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
for heartbeat entries will be empty, issueEntryIndex is -1
for appendEntries, entries will log[rf.nextIndices[server]-1:]
it doesn't check if a higher issueEntryIndex is sent out
*/
func (rf *Raft) SendAppendEntries(server int, issueEntryIndex int, ch chan AppendEntriesReply) {
	rf.mu.Lock()

	if rf.latestIssuedEntryIndices[server] < issueEntryIndex {
		rf.latestIssuedEntryIndices[server] = issueEntryIndex
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
		// RPC call failed
		reply.Server = server
		reply.IssueEntryIndex = args.IssueEntryIndex
		reply.LastAppendedIndex = 0
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
// handle retries
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
		hasHigherIndex := rf.latestIssuedEntryIndices[reply.Server] > reply.IssueEntryIndex
		isLeader := rf.role == LEADER

		quitMsg := 0
		if reply.Success || reply.HigherTerm || hasHigherIndex || rf.killed() || !isLeader {
			rf.mu.Unlock()
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
		} else {
			rf.mu.Unlock()
		}

		// retry
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)

		// need to decrement nextIndices[server] if mismatched
		if reply.MisMatched {
			/*
				suppose AppendCommand(comm1), AppendCommand(comm2)
				rf.nextIndices[server] can be higher or lower than issuedIndex-1
				if rf.nextIndices[reply.Server] != reply.NextIndex,
				it means comm0 or comm2 may have updated nextIndices[Server]
				this reply conveys outdated info, can't be used to update nextIndices[Server]
			*/
			rf.mu.Lock()
			if rf.nextIndices[reply.Server] == reply.NextIndex {
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

		if rf.matchIndices[reply.Server] < reply.LastAppendedIndex {
			rf.matchIndices[reply.Server] = reply.LastAppendedIndex
		}

		if rf.nextIndices[reply.Server] < reply.LastAppendedIndex+1 {
			rf.nextIndices[reply.Server] = reply.LastAppendedIndex + 1
		}

		rf.mu.Unlock()

	}
	rf.quitTrailingReplyChan <- 1
}

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand() {
	rf.mu.Lock()
	i := rf.lastApplied + 1
	commitIndex := rf.commitIndex
	rf.mu.Unlock()
	for i <= commitIndex {
		rf.mu.Lock()
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[i-1].Command, //committed logs will never be changed, no need for locks
			CommandIndex: rf.log[i-1].Index,
		}
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		if rf.lastApplied < i {
			rf.lastApplied = i
			i++
		} else {
			i = rf.lastApplied + 1
		}
		commitIndex = rf.commitIndex
		rf.mu.Unlock()
	}
}

/*
even leader turns to follower or server is killed
we can still update the commitIndex since they are volatile
before a new leader is elected
*/
func (rf *Raft) updateCommit(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	count := 1
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.matchIndices[i] >= index {
			count++
		}
	}
	if count > len(rf.peers)/2 && rf.commitIndex < index {
		rf.commitIndex = index
	}
}

// func (rf *Raft) updateCommit(index int) {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	smallestIndex := len(rf.log)
// 	for i := 0; i < len(rf.peers); i++ {
// 		if i == rf.me {
// 			continue
// 		}
// 		if rf.matchIndices[i] < smallestIndex {
// 			smallestIndex = rf.matchIndices[i]
// 		}
// 	}
// 	if smallestIndex != 0 && rf.log[smallestIndex-1].Term == rf.currentTerm && rf.commitIndex < smallestIndex {
// 		rf.commitIndex = smallestIndex
// 	}
// }

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
