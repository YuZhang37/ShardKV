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
		rf.persistState("server %v heartbeat replies on %v with higher term: %v, original term: %v", rf.me, reply.Server, reply.Term, originalTerm)
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
	index := 0
	if len(rf.log) == 0 {
		index = rf.snapshotLastIndex + 1
	} else {
		index = rf.log[len(rf.log)-1].Index + 1
	}
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   index,
		Command: command,
	}
	rf.log = append(rf.log, entry)

	rf.persistState("server %v appends entry %v to its log", rf.me, entry)
	AppendEntriesDPrintf("Command %v is appended on %v at index of %v\n", command, rf.me, len(rf.log))

	go rf.ReachConsensus(entry.Index)
	return entry.Index, entry.Term, true
}

func (rf *Raft) ReachConsensus(index int) {
	rf.mu.Lock()
	if rf.killed() || rf.role != LEADER {
		rf.mu.Unlock()
		return
	}
	if rf.commitIndex >= index {
		rf.mu.Unlock()
		return
	}

	numOfPeers := len(rf.peers)
	// channel for harvesters to transmit results to ReachConsensus
	ch0 := make(chan AppendEntriesReply)
	// channels for ReachConsensus thread to terminate harvesters
	stopChans := make([]chan int, numOfPeers)
	for i := 0; i < len(stopChans); i++ {
		stopChans[i] = make(chan int)
	}
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me {
			continue
		}
		// channel for sender to transmit the result to harvesters
		ch := make(chan AppendEntriesReply)
		go rf.SendAppendEntries(i, index, ch)
		go rf.HarvestAppendEntriesReply(index, ch0, stopChans[i], ch)
	}
	rf.mu.Unlock()

	timerChan := make(chan int)
	quitTimerChan := make(chan int)
	go rf.CheckCommitTimeOut(quitTimerChan, timerChan)

	harvestedServers := rf.Commit(ch0, timerChan, index)
	// stop the timer thread
	go func(ch chan int) {
		ch <- 1
	}(quitTimerChan)
	go rf.QuitBlockedHarvests(numOfPeers, harvestedServers, stopChans)
	go rf.ApplyCommand(index)
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
func (rf *Raft) Commit(ch0 chan AppendEntriesReply, timerChan chan int, entryIndex int) map[int]bool {

	// the leader has appended the entry
	successCount := 1
	tryCommit := true
	harvestedServers := make(map[int]bool)

	for tryCommit {
		select {
		case reply := <-ch0:
			harvestedServers[reply.Server] = true
			rf.mu.Lock()
			if rf.killed() || rf.role != LEADER {
				tryCommit = false
			} else {
				tryCommit, successCount = rf.OnReceivingAppendEntriesReply(&reply, successCount, entryIndex)
			}
			rf.mu.Unlock()
		case <-timerChan:
			rf.mu.Lock()
			if rf.killed() || rf.role != LEADER {
				tryCommit = false
			} else {
				rf.updateCommit(entryIndex)
				// start(comm2) may commit a higher index
				if rf.commitIndex >= entryIndex {
					tryCommit = false
				}
			}
			rf.mu.Unlock()
		}
	}
	return harvestedServers
}

func (rf *Raft) OnReceivingAppendEntriesReply(reply *AppendEntriesReply, successCount int, entryIndex int) (bool, int) {
	tryCommit := true
	if reply.Success {
		successCount++
		if rf.matchIndices[reply.Server] < reply.LastAppendedIndex {
			rf.matchIndices[reply.Server] = reply.LastAppendedIndex
		}
		if rf.nextIndices[reply.Server] < reply.LastAppendedIndex+1 {
			rf.nextIndices[reply.Server] = reply.LastAppendedIndex + 1
		}
	} else if reply.HigherTerm && reply.Term > rf.currentTerm {
		originalTerm := rf.onReceiveHigherTerm(reply.Term)
		rf.persistState("server %v try to commit %v replies on %v with higher term: %v, original term: %v", rf.me, entryIndex, reply.Server, reply.Term, originalTerm)
		tryCommit = false
	}
	// the reply when failed with higher issue index is dropped

	if successCount > len(rf.peers)/2 {
		if rf.commitIndex < entryIndex {
			rf.commitIndex = entryIndex
		}
		tryCommit = false
	}
	return tryCommit, successCount
}

/*
for all harvest goroutines which don't reply, send out a message to redirect the replies to global harvest or just drop all replies
*/
func (rf *Raft) QuitBlockedHarvests(numOfPeers int, harvestedServers map[int]bool, stopChans []chan int) {
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me || harvestedServers[i] {
			continue
		}
		go func(ch chan int, server int) {
			quitMessage := 1
			ch <- quitMessage
		}(stopChans[i], i)
	}
}

/*
follower or candidate server will execute this function for heartbeat or appendEntries,
for heartbeat: args.IssueEntryIndex is -1 and no entries
*/
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	funct := 1
	if args.IssueEntryIndex == -1 {
		funct = 2
	}
	AppendEntries2DPrintf(funct, "Command from %v received by %v at index of %v\n", args.LeaderId, rf.me, args.PrevLogIndex+1)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.FillArgsInReply(args, reply)
	if rf.currentTerm > args.Term {
		rf.HigherTermReply(reply)
		return
	}

	/**** process both heartbeats and appendEntries ****/

	reply.HigherTerm = false
	reply.LogLength = rf.snapshotLastIndex + len(rf.log)
	rf.UpdateStateOnReceivingAppendEntries(args)
	mismatch, indexInLiveLog := rf.CheckPrevLogMisMatch(args, reply)
	if mismatch {
		return
	}
	/*
		at this point, the prev log entry matches the leader,
		or the entry has been merged,
		for prev log entry is index=0 and term=0, it is merged into this case
	*/
	reply.Success = true
	reply.HigherTerm = false
	reply.MisMatched = false

	// for appendEntries: args.Entries can still be empty if the nextIndices[Server] gets updated before preparing the args
	// len(args.Entries) == 0 includes heartbeat and appendEntries
	if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.currentAppended {
		// no new entries in args needed to append on the server
		reply.LastAppendedIndex = rf.currentAppended
	} else {
		rf.AppendNewEntriesFromArgs(indexInLiveLog, args, reply)
	}
	rf.UpdateCommitIndexOnReceivingAppendEntries(args)
	AppendEntries2DPrintf(funct, "Command from %v is appended by %v at index of %v\n", args.LeaderId, rf.me, len(rf.log))
	AppendEntries2DPrintf(funct, "logs on server %v: %v\n", rf.me, rf.log)

}

func (rf *Raft) UpdateCommitIndexOnReceivingAppendEntries(args *AppendEntriesArgs) {
	//update commitIndex both for heartbeat and appendEntries
	leaderCommitIndex := args.LeaderCommitIndex
	if rf.currentAppended < leaderCommitIndex {
		leaderCommitIndex = rf.currentAppended
	}
	if rf.commitIndex < leaderCommitIndex {
		rf.commitIndex = leaderCommitIndex
		go rf.ApplyCommand(rf.commitIndex)
	}
}

func (rf *Raft) FillArgsInReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// the reply's term may be less than leader's
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.IssueEntryIndex = args.IssueEntryIndex
	reply.NextIndex = args.PrevLogIndex + 1
}

// leader contacts one server with higher term
func (rf *Raft) HigherTermReply(reply *AppendEntriesReply) {
	reply.Success = false
	reply.HigherTerm = true
	reply.MisMatched = false
	reply.LastAppendedIndex = 0
}

// check if args prevLogEntry matches the server
// if matched, return the indexInLiveLog for the prevLogEntry
func (rf *Raft) CheckPrevLogMisMatch(args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, int) {
	if reply.LogLength < args.PrevLogIndex {
		// 1. log doesn't contain an entry at PrevLogIndex
		// indexInLiveLog == length of the log
		reply.Success = false
		reply.MisMatched = true
		reply.ConflictTerm = -1
		reply.ConflictStartIndex = -1
		return true, 0
	}
	/*
		the server has an entry at args.PrevLogIndex:
		1. this entry has been merged into snapshot
		2. this entry is alive
	*/
	indexInLiveLog := rf.findEntryWithIndexInLog(args.PrevLogIndex, rf.log, rf.snapshotLastIndex)
	SnapshotDPrintf("server: %v, indexInLiveLog: %v, args.PrevLogIndex: %v\n", rf.me, indexInLiveLog, args.PrevLogIndex)
	/*
		if indexInLiveLog == -1, then the prev log entry has been committed and merged, must be matching
	*/
	if indexInLiveLog != -1 && rf.log[indexInLiveLog].Term != args.PrevLogTerm {
		// 2. log entry at PrevLogIndex is alive and doesn't equal to PrevLogTerm
		reply.Success = false
		reply.MisMatched = true
		reply.ConflictTerm = rf.log[indexInLiveLog].Term
		reply.ConflictStartIndex = rf.findStartIndex(reply.ConflictTerm)
		return true, 0
	}
	return false, indexInLiveLog
}

func (rf *Raft) UpdateStateOnReceivingAppendEntries(args *AppendEntriesArgs) {
	// 1. follower or candidate or leader with smaller term (< args.Term)
	// 2. candidate with term = args.Term, if -1, then it's the first time this server contacts with the leader, updates all states just like encountering a higher term.
	// => when leader sends out the first heartbeat, all followers's leader will be set to be it, and votedFor will be reset to be -1
	// 2 is wrong, can lead to split brain

	if rf.currentTerm < args.Term {
		rf.onReceiveHigherTerm(args.Term)
		rf.currentLeader = args.LeaderId
		rf.persistStateWithSnapshot("AppendEntries()")
	} else if rf.currentLeader == -1 {
		// must be a candidate with term == leader's term and currentLeader being -1, don't change votedFor
		rf.role = FOLLOWER
		rf.currentLeader = args.LeaderId
		rf.currentAppended = 0
	}

	// valid heartbeat or appendEntries
	rf.msgReceived = true
}

func (rf *Raft) AppendNewEntriesFromArgs(indexInLiveLog int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	/*
		new entries in args which need to be appended,
		the server's log matches leader's log at least up through PrevLogIndex
		now appends new entries to the server's log, the new entries may not start from PreLogIndex + 1
	*/
	i := 0
	j := 0
	if indexInLiveLog == -1 {
		// some head log entries in args have been merged
		j = rf.findEntryWithIndexInLog(rf.snapshotLastIndex+1, args.Entries, rf.snapshotLastIndex)
	} else {
		// matching start from indexInLiveLog+1
		i = indexInLiveLog + 1
	}
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
	reply.LastAppendedIndex = rf.currentAppended
	rf.persistState("server %v appends new entries %v to %v", rf.me, args, rf.currentAppended)
}

/*
this function will guarantee to send to ch exact one reply, the longest time it takes is the RPC timeout.
for heartbeat entries will be empty, issueEntryIndex is -1
for appendEntries, entries will log[rf.nextIndices[server]-1:]
it doesn't check if a higher issueEntryIndex is sent out
but it needs to check if the server still the leader or if it's killed
*/
func (rf *Raft) SendAppendEntries(server int, issueEntryIndex int, ch chan AppendEntriesReply) {
	fakeReply := rf.GetFakeAppendEntriesReply(server, issueEntryIndex)
	rf.mu.Lock()
	if rf.killed() || rf.role != LEADER {
		// this reply will be dropped in tryCommit or in HarvestAppendEntriesReply
		rf.mu.Unlock()
		ch <- fakeReply
		return
	}

	if rf.latestIssuedEntryIndices[server] < issueEntryIndex {
		rf.latestIssuedEntryIndices[server] = issueEntryIndex
	}
	funct := 1
	if issueEntryIndex < 0 {
		funct = 2
	}
	next := rf.nextIndices[server]
	AppendEntries2DPrintf(funct, "next index to %v is %v \n", server, next)
	if next < 1 {
		rf.mu.Unlock()
		log.Fatalf("fatal: next index to %v is %v \n", server, next)
	}

	if next <= rf.snapshotLastIndex {
		// will be retried later
		rf.mu.Unlock()
		ch <- fakeReply
		return
	}
	args := rf.GetAppendEntriesArgs(server, next, issueEntryIndex)
	AppendEntries2DPrintf(funct, "args: %v at %v to %v\n", args, rf.me, server)
	AppendEntries2DPrintf(funct, "log: %v at %v \n", rf.log, rf.me)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// RPC call failed
		ch <- fakeReply
		return
	}
	ch <- reply
}

func (rf *Raft) GetFakeAppendEntriesReply(server int, issueEntryIndex int) AppendEntriesReply {
	fakeReply := AppendEntriesReply{
		Server:            server,
		IssueEntryIndex:   issueEntryIndex,
		LastAppendedIndex: 0,
		Term:              0,
		Success:           false,
		HigherTerm:        false,
		MisMatched:        false,
	}
	return fakeReply
}

func (rf *Raft) GetAppendEntriesArgs(server int, next int, issueEntryIndex int) AppendEntriesArgs {
	prevLogIndex := 0
	prevLogTerm := 0
	entries := make([]LogEntry, 0)
	indexInLiveLog := rf.findEntryWithIndexInLog(next, rf.log, rf.snapshotLastIndex)

	if next == rf.snapshotLastIndex+1 {
		// there is no previous entry
		// rf.snapshotLastIndex and rf.snapshotLastTerm are initialized to be 0, so for next = 1, both variables will be 0
		prevLogIndex = rf.snapshotLastIndex
		prevLogTerm = rf.snapshotLastTerm
	} else {
		// there is a previous entry
		TestDPrintf("Server %v next index at Server %v is %v\n", server, rf.me, next)
		prevLogIndex = rf.log[indexInLiveLog-1].Index
		prevLogTerm = rf.log[indexInLiveLog-1].Term
	}

	if issueEntryIndex != -1 {
		// not a heartbeat
		entries = rf.log[indexInLiveLog:]
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
	return args
}

/*
although harvest may issue lots of senders
but at any point in time, there is exactly one sender and one harvester
and sender is ensured to send to the channel exactly once
handle retries

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

reply.sucess:

	if reply.false, the longest time it takes from sending to this point is when RPC call fails,
	the timeout time of that.
	even reply.true, it can take that long as well
	so we can't close the chan before that.
	We close this channel when this server gets elected again, but if this happens too soon,
	reply can still send to a closed channel, the server will panic, other servers can continue election.
	RPC timeout should smaller than election timeout to avoid closed channel causing false fails.

if reply.MisMatched:

suppose AppendCommand(comm1), AppendCommand(comm2)
rf.nextIndices[server] can be higher or lower than issuedIndex-1
if rf.nextIndices[reply.Server] != reply.NextIndex,
it means comm0 or comm2 may have updated nextIndices[Server]
this reply conveys outdated info, can't be used to update nextIndices[Server]
*/
func (rf *Raft) HarvestAppendEntriesReply(issuedIndex int, sendReplyChan chan AppendEntriesReply, stopChan chan int, getReplyChan chan AppendEntriesReply) {
	for {
		reply := <-getReplyChan
		rf.mu.Lock()
		hasHigherIndex := rf.latestIssuedEntryIndices[reply.Server] > reply.IssueEntryIndex
		isLeader := rf.role == LEADER

		if reply.Success || reply.HigherTerm || hasHigherIndex || rf.killed() || !isLeader {
			rf.mu.Unlock()
			AppendEntriesDPrintf("reply from server %v: %v waiting for sending", reply.Server, reply)
			select {
			case sendReplyChan <- reply:
				AppendEntriesDPrintf("reply from server %v sends to replyChan", reply.Server)
			case <-stopChan:
				rf.mu.Lock()
				AppendEntriesDPrintf("reply from server %v sends to trailing", reply.Server)
				if reply.Success && rf.currentTerm == reply.Term && rf.role == LEADER && !rf.killed() {
					rf.mu.Unlock()
					rf.trailingReplyChan <- reply
				} else {
					rf.mu.Unlock()
				}
			}
			break
		} else {
			// retry
			// need to change nextIndices[server] if mismatched
			if reply.MisMatched {
				rf.UpdateNextIndicesOnMisMatch(reply)
			}
			if rf.nextIndices[reply.Server] <= rf.snapshotLastTerm {
				log.Fatalf("No implementation for endAndHarvestSnapshot")
				// go rf.SendAndHarvestSnapshot(reply.Server)
			}
			go rf.SendAppendEntries(reply.Server, issuedIndex, getReplyChan)
			rf.mu.Unlock()
		}
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)
	}
}

func (rf *Raft) UpdateNextIndicesOnMisMatch(reply AppendEntriesReply) {
	if rf.nextIndices[reply.Server] == reply.NextIndex && rf.snapshotLastIndex < reply.NextIndex {
		/*
			next index for this server is not updated yet
			and the entry is not merged into snapshot
			for mismatched index
		*/
		if reply.NextIndex == rf.snapshotLastIndex+1 {
			// no previous log entry
			// at least there should be one previous entry between the snapshot and log entry of nextIndex
			rf.nextIndices[reply.Server]--
		} else if reply.ConflictTerm != -1 {
			// conflicting term
			// there is at least previous entry
			indexInLiveLog := rf.findEntryWithIndexInLog(reply.NextIndex-1, rf.log, rf.snapshotLastIndex)
			if reply.ConflictTerm < rf.log[indexInLiveLog].Term {
				// case 1: conflicting term smaller than log[prevIndex]
				rf.nextIndices[reply.Server] = rf.log[rf.findLargerEntryIndex(reply.ConflictTerm)].Index
			} else {
				// case 2: conflicting term larger than log[prevIndex]
				rf.nextIndices[reply.Server] = reply.ConflictStartIndex
			}
		} else {
			// the server's log is shorter
			// has no entry at prevIndex
			rf.nextIndices[reply.Server] = reply.LogLength + 1
		}
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
even leader turns to follower or server is killed
we can still update the commitIndex since they are volatile
before a new leader is elected
calling function needs to hold rf.mu
*/
func (rf *Raft) updateCommit(index int) {
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
