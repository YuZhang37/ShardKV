package raft

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
	rf.fillArgsInReply(args, reply)
	if rf.currentTerm > args.Term {
		rf.higherTermReply(reply)
		return
	}

	/**** process both heartbeats and appendEntries ****/

	reply.HigherTerm = false
	reply.LogLength = rf.snapshotLastIndex + len(rf.log)
	rf.updateStateOnReceivingAppendEntries(args)
	mismatch, indexInLiveLog := rf.checkPrevLogMisMatch(args, reply)
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
		rf.appendNewEntriesFromArgs(indexInLiveLog, args, reply)
	}
	// need to update commitIndex on receiving heartbeat
	rf.updateCommitIndexOnReceivingAppendEntries(args)
	AppendEntries2DPrintf(funct, "Command from %v is appended by %v at index of %v\n", args.LeaderId, rf.me, len(rf.log))
	AppendEntries2DPrintf(funct, "logs on server %v: %v\n", rf.me, rf.log)

}

/****************** helper functions *******************/

func (rf *Raft) updateStateOnReceivingAppendEntries(args *AppendEntriesArgs) {
	// 1. follower or candidate or leader with smaller term (< args.Term)
	// 2. candidate with term = args.Term, if -1, then it's the first time this server contacts with the leader, updates all states just like encountering a higher term.
	// => when leader sends out the first heartbeat, all followers's leader will be set to be it, and votedFor will be reset to be -1
	// 2 is wrong, can lead to split brain

	if rf.currentTerm < args.Term {
		rf.onReceiveHigherTerm(args.Term)
		rf.currentLeader = args.LeaderId
		rf.persistState("AppendEntries()")
	} else if rf.currentLeader == -1 {
		// must be a candidate with term == leader's term and currentLeader being -1, don't change votedFor
		rf.role = FOLLOWER
		rf.currentLeader = args.LeaderId
		rf.currentAppended = 0
	}

	// valid heartbeat or appendEntries
	rf.msgReceived = true
}

// check if args prevLogEntry matches the server
// if matched, return the indexInLiveLog for the prevLogEntry
func (rf *Raft) checkPrevLogMisMatch(args *AppendEntriesArgs, reply *AppendEntriesReply) (bool, int) {
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

func (rf *Raft) appendNewEntriesFromArgs(indexInLiveLog int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
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
		// find the unmatched entry or goes to the end of the server's log
		if rf.log[i].Term != args.Entries[j].Term {
			break
		}
		i++
		j++
	}
	if i < len(rf.log) {
		// removing unmatched trailing log entries in the server
		rf.log = rf.log[:i]
	}

	for ; j < len(args.Entries); j++ {
		// append new log entries to the server's log
		entry := args.Entries[j]
		newLog := append(rf.log, entry)
		size := rf.getLogSize(newLog)
		if rf.maxFollowerLogSize != -1 && size >= rf.maxFollowerLogSize && rf.commitIndex > rf.snapshotLastIndex {
			// there log entries to compact, compact them
			rf.insideApplyCommand(rf.commitIndex, true)
			rf.signalSnapshot()
			newLog = append(rf.log, entry)
			size = rf.getLogSize(newLog)
			rf.persistState("server %v appends new entries %v to %v",
				rf.me, args, rf.currentAppended)
		}

		if rf.maxFollowerLogSize == -1 || size < rf.maxFollowerLogSize {
			rf.log = newLog
			rf.persistState("server %v appends new entries %v to %v", rf.me, args, rf.currentAppended)
			if entry.Index <= args.LeaderCommitIndex {
				rf.commitIndex = entry.Index
				go rf.ApplyCommand(rf.commitIndex)
			}
			rf.currentAppended = args.Entries[j].Index
		} else {
			// log.Printf("Server: %v, log size %v exceeds max: %v,"+
			// 	"log len: %v, can't append new entries, last appended: %v",
			// 	rf.me, size, rf.maxFollowerLogSize, len(rf.log), rf.currentAppended)
			break
		}
	}
	reply.LastAppendedIndex = rf.currentAppended
}

func (rf *Raft) updateCommitIndexOnReceivingAppendEntries(args *AppendEntriesArgs) {
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
