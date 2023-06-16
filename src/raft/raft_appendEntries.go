package raft

import (
	//	"bytes"
	"log"
	"time"
	//	"6.824/labgob"
)

/* tests
TestBasicAgree2B			:	pass
TestRPCBytes2B				:	pass
TestFailAgree2B				: 	pass
TestFailNoAgree2B			:	fail-infinite loop
TestConcurrentStarts2B		:	pass
TestRejoin2B				:	fail-infinite loop
TestBackup2B				:	fail-infinite loop
TestCount2B					:	pass
*/

// for heartbeat: send and harvest is 1 on 1, and send part definitely sends to the channel, no need for stopChan
func (rf *Raft) HeartBeat(server int) {
	replyChan := make(chan AppendEntriesReply)
	for !rf.killed() && rf.isLeader() {
		go rf.SendAppendEntries(server, -1, replyChan)
		go rf.HarvestHeartbeatReply(replyChan)
		time.Sleep(time.Duration(rf.hbTimeOut) * time.Millisecond)
	}

}

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
	isLeader := false
	if !rf.isLeader() {
		rf.mu.Lock()
		currentLeader := rf.currentLeader
		rf.mu.Unlock()
		log.Printf("Command sends to %v, which is not a leader, the leader is %v\n", rf.me, currentLeader)
		return index, term, isLeader
	}

	rf.mu.Lock()
	nextIndices := rf.nextIndices
	rf.mu.Unlock()
	log.Printf("Command sends to %v, which is a leader\n", rf.me)
	log.Printf("the next indices are: %v\n", nextIndices)
	log.Printf("Start processing...\n")
	//append to the leader's local log
	rf.mu.Lock()
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   len(rf.log) + 1,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	numOfPeers := len(rf.peers)

	log.Printf("Command is appended on %v at index of %v\n", rf.me, len(rf.log))
	// channel for harvesting threads communicating with Start() thread
	ch0 := make(chan AppendEntriesReply)
	// channels for Start() thread to terminate harvesting threads
	stopChans := make([]chan int, numOfPeers)
	for i := 0; i < len(stopChans); i++ {
		stopChans[i] = make(chan int)
	}

	for i := 0; i < numOfPeers; i++ {
		if i == rf.me {
			continue
		}
		// channel for sending thread send the reply to harvesting thread
		ch := make(chan AppendEntriesReply)

		// there could be many outstanding appendEntries to the
		// same server issued by Start(comm1) and Start(comm2)
		// the args.entries to send may be longer than this new entry
		// doesn't matter, AppendEntries will handle them
		go rf.SendAppendEntries(i, entry.Index, ch)
		go rf.HarvestAppendEntriesReply(entry.Index, ch0, stopChans[i], ch)

	}

	index = entry.Index
	term = rf.currentTerm
	isLeader = true

	rf.mu.Unlock()
	successCount := 1

	/*
		accommodate the case where Start(comm2) may commit comm1,
		also it's possible Start(comm1) can commit comm2
	*/
	timerChan := make(chan int)
	quitTimerChan := make(chan int)
	go rf.CheckCommitTimeOut(quitTimerChan, timerChan)

	tryCommit := true
	harvestedServers := make(map[int]bool)
	/*
		indicate what the harvesting threads should do when Start() stops
		receiving replies, drop the reply or send to global harvesting
	*/
	quitMessage := QUITWITHVALIDLEADER

	for tryCommit && !rf.killed() && rf.isLeader() {
		select {
		case reply := <-ch0:
			harvestedServers[reply.Server] = true
			rf.mu.Lock()
			if reply.Success {
				successCount++
				// to cover the case Start(comm2) finishes before Start(comm1)
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
				quitMessage = QUITWITHINVALIDLEADER
			}

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

	/*
		at this point, the leader either commits the entry or the leader becomes a follower

		could it be possible that the leader commits the entry and becomes a follower?
		not possible, the loop deals with the reply one by one,
		either it reaches majority or touches a higher term reply

		although it's possible that the higher term reply goes to global reply harvest, but it just ignores the reply
	*/

	// stop the timer thread
	go func(ch chan int) {
		ch <- 1
	}(quitTimerChan)

	/*
		for all not replied harvesting threads, send out a message to redirect to global harvest replies or just drop all replies
	*/
	log.Printf("....harvestedServers: %v\n", harvestedServers)
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me || harvestedServers[i] {
			continue
		}
		go func(ch chan int, server int) {
			log.Printf("....quitMessage: %v is sent to %v\n", quitMessage, server)
			ch <- quitMessage
		}(stopChans[i], i)
	}

	// close global havesting thread if the leader turns to a follower
	if quitMessage == QUITWITHINVALIDLEADER {
		/*
			when we close traiglingReplyChan at this moment, are there messages wait on this channel?
			yes,
			for Start(comm1), majority has return success:
			follower i is sent to redirect,
			Start(comm2), contacts a higher term follower j,
			the leader turns to a follower,
			the retry to follower i with comm1 is a success,
			it will be sent to trailingReplyChan,
			and the channel may have been closed

			if we get success reply for comm2, this reply will definitely be dropped, since both channels are closed for it.

			can we discard the success reply if there is an outgoing request with high issuedIndex ?
			won't solve the problem. There can be a period:
			success reply for comm1 from follower i is returned, but comm2 is not sent to follower i yet and reply follower j
			for comm2 is returned.

			what is the window time for success reply of comm1 to be sent to a closed trailingReplyChan?

			reply from follower j for comm2 must be returned.
			request to follower i for comm1 must be issued before the one for comm2

			if there are new entries beyond comm1, success replies
			don't be sent to trailingReplyChan,
			sacrifice some sync info

			if leader turns to follower:
			we need to wait for all threads to stop
			sending threads,
			harvesting threads,
			handling trailing threads
			heartbeat threads <stops when detecting invalid leader>
		*/

		/*
			comm1 to server i must be a success request which is issued before comm2 to server i, it would take at most RPC call timeout time.
			choose ELETIMEOUT, because it's ok to update matchedIndices and nextIndices before a new leader is established.
		*/
		time.Sleep(time.Duration(ELETIMEOUT) * time.Millisecond)
		close(rf.trailingReplyChan)
		isLeader = false
	}

	go rf.ApplyCommand()
	return index, term, isLeader
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	log.Printf("Command from %v received by %v at index of %v\n", args.LeaderId, rf.me, args.PrevLogIndex+1)
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
			// suppose Start(comm1), Start(comm2)
			// comm2 may get to the server earlier than comm1
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

	leaderCommitIndex := args.LeaderCommitIndex
	if rf.currentReceived < leaderCommitIndex {
		leaderCommitIndex = rf.currentReceived
	}
	if rf.commitIndex < leaderCommitIndex {
		rf.commitIndex = leaderCommitIndex
		go rf.ApplyCommand()
	}

	if len(args.Entries) > 0 {
		log.Printf("Command from %v is appended by %v at index of %v\n", args.LeaderId, rf.me, len(rf.log))
		log.Printf("logs on server %v: %v\n", rf.me, rf.log)
	}
}

func (rf *Raft) SendAppendEntries(server int, issueEntryIndex int, ch chan AppendEntriesReply) {
	// heartbeat will be empty entries
	// appendEntries will log[rf.nextIndices[server]-1:]
	rf.mu.Lock()

	if rf.issuedEntryIndices[server] < issueEntryIndex {
		rf.issuedEntryIndices[server] = issueEntryIndex
	}

	prevLogIndex := 0
	prevLogTerm := 0
	entries := make([]LogEntry, 0)
	next := rf.nextIndices[server]
	log.Printf("next index to %v is %v \n", server, next)
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
	log.Printf("args: %v at %v to %v\n", args, rf.me, server)
	log.Printf("log: %v at %v \n", rf.log, rf.me)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// log.Printf("Server %v RPC request vote to %v failed!\n", rf.me, server)
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
			won't block Start(), it has timerChan
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
		rf.mu.Unlock()

		// may have overlap with higher issued index but won't matter
		if reply.Success || (!reply.Success && reply.HigherTerm) || hasHigherIndex {
			log.Printf("reply from server %v: %v waiting for sending", reply.Server, reply)
			select {
			case sendReplyChan <- reply:
				log.Printf("reply from server %v sends to replyChan", reply.Server)
			case quitMsg := <-stopChan:
				log.Printf("...quitMsg: %v, reply from server %v sends to trailing", quitMsg, reply.Server)
				if reply.Success && quitMsg == QUITWITHVALIDLEADER {
					rf.trailingReplyChan <- reply
				}
			}
			break
		}

		// retry
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)
		if rf.killed() || !rf.isLeader() {
			break
		}

		// need to decrement nextIndices[server] if mismatched
		if reply.MisMatched {
			/*
				suppose Start(comm1), Start(comm2)
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

func (rf *Raft) HandleTrailingReply() {
	// just update matchIndices and nextIndices
	log.Printf("HandleTrailingReply gets running....")
	for reply := range rf.trailingReplyChan {
		log.Printf("HandleTrailingReply got Reply: %v", reply)
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

// func (rf *Raft) HandleTrailingReply() {
// 	// just update matchIndices and nextIndices
// 	log.Printf("HandleTrailingReply gets running....")
// 	for reply := range rf.trailingReplyChan {
// 		log.Printf("HandleTrailingReply got Reply: %v", reply)
// 		if !reply.Success {
// 			// ignore highIndex which can invalidate the current leader
// 			continue
// 		}
// 		rf.mu.Lock()

// 		if rf.matchIndices[reply.Server] < reply.LastAppendIndex {
// 			rf.matchIndices[reply.Server] = reply.LastAppendIndex
// 		}

// 		if rf.nextIndices[reply.Server] < reply.LastAppendIndex+1 {
// 			rf.nextIndices[reply.Server] = reply.LastAppendIndex + 1
// 		}

// 		rf.mu.Unlock()

// 	}

// }

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
the Start() thread checks the leader validation,
timer should quit after receiving loop in Start()
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
