package raft

import (
	// "log"
	"time"
)

/*
for all harvest goroutines which don't reply, send out a message to redirect the replies to global harvest or just drop all replies
*/
func (rf *Raft) quitBlockedHarvests(numOfPeers int, harvestedServers map[int]bool, stopChans []chan int) {
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

		rf.lockMu("HandleTrailingReply()")
		if rf.matchIndices[reply.Server] < reply.LastAppendedIndex {
			rf.matchIndices[reply.Server] = reply.LastAppendedIndex
		}
		if rf.nextIndices[reply.Server] < reply.LastAppendedIndex+1 {
			rf.nextIndices[reply.Server] = reply.LastAppendedIndex + 1
		}
		rf.unlockMu()
	}
	rf.quitTrailingReplyChan <- 1
}

/*
don't check !rf.killed() && rf.isLeader()
the AppendCommand() thread checks the leader validation,
timer should quit after receiving loop in AppendCommand()
*/
func (rf *Raft) checkCommitTimeOut(quitChan chan int, timerChan chan int) {

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

func (rf *Raft) onReceivingAppendEntriesReply(reply *AppendEntriesReply, successCount int, entryIndex int) (bool, bool, int) {
	tryCommit := true
	committed := false
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
		committed = true
	}
	return tryCommit, committed, successCount
}

func (rf *Raft) fillArgsInReply(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// the reply's term may be less than leader's
	reply.Server = rf.me
	reply.Term = rf.currentTerm
	reply.IssueEntryIndex = args.IssueEntryIndex
	reply.NextIndex = args.PrevLogIndex + 1
}

// leader contacts one server with higher term
func (rf *Raft) higherTermReply(reply *AppendEntriesReply) {
	reply.Success = false
	reply.HigherTerm = true
	reply.MisMatched = false
	reply.LastAppendedIndex = 0
}

func (rf *Raft) getFakeAppendEntriesReply(server int, issueEntryIndex int) AppendEntriesReply {
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

func (rf *Raft) getAppendEntriesArgs(server int, next int, issueEntryIndex int) AppendEntriesArgs {
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

func (rf *Raft) updateNextIndicesOnMisMatch(reply AppendEntriesReply) {
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
