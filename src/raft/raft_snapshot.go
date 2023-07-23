package raft

import (
	"time"
)

/*
the service says it has created a snapshot that has
all info up to and including index. this means the
service no longer needs the log through (and including)
that index. Raft should now trim its log as much as possible.
index is always ≤ the commit index on this server.
*/
func (rf *Raft) Snapshot(lastIncludedIndex int, snapshot []byte) {

	rf.lockMu("Snapshot(): with lastIncludedIndex: %v\n", lastIncludedIndex)
	defer rf.unlockMu()
	rf.snapshot2DPrintf("Snapshot(): called with lastIncludedIndex %v,\n", lastIncludedIndex)
	indexInLiveLog := rf.insideSnapshot(lastIncludedIndex, snapshot)
	if indexInLiveLog == -1 {
		return
	}
	rf.persistState("Snapshot(): lastIncludedIndex: %v, indexInLiveLog in log: %v", lastIncludedIndex, indexInLiveLog)
}

/*
the same as Snapshot, except that
1. rf.mu is held in the calling function
2. state and snapshot is not persisted
*/
func (rf *Raft) insideSnapshot(lastIncludedIndex int, snapshot []byte) int {
	rf.snapshot2DPrintf("insideSnapshot(called with lastIncludedIndex %v,\n", lastIncludedIndex)

	// lastIncludedIndex can be <= 0, if the command has been sent to the application but has been applied when taking the snapshot
	if lastIncludedIndex > rf.commitIndex {
		rf.logFatal("insideSnapshot(): Raft Snapshot Error: rf.gid: %v, rf.me: %v, rf.role: %v, lastIncludedIndex %v, commitIndex: %v\n", rf.gid, rf.me, rf.role, lastIncludedIndex, rf.commitIndex)
	}
	/*
		stale snapshot: < rf.snapshotLastIndex
		when the server receives a new snapshot from the leader, and updated its state, and before this new snapshot is applied to the service, the service takes a snapshot, this snapshot is stale
		repeated snapshot: = rf.snapshotLastIndex
		the server takes snapshot too soon
	*/
	if lastIncludedIndex <= rf.snapshotLastIndex {
		return -1
	}
	indexInLiveLog := rf.findEntryWithIndexInLog(lastIncludedIndex, rf.log, rf.snapshotLastIndex)
	if indexInLiveLog >= len(rf.log) || indexInLiveLog < 0 {
		rf.logFatal("insideSnapshot(): Raft Snapshot Error: rf.gid: %v, rf.me: %v, rf.role: %v,indexInLiveLog: %v, len(log): %v, log %v doesn't contain index: %v\n", rf.gid, rf.me, rf.role, indexInLiveLog, rf.log, len(rf.log), lastIncludedIndex)
	}
	rf.snapshotLastIndex = lastIncludedIndex
	rf.snapshotLastTerm = rf.log[indexInLiveLog].Term
	rf.snapshot = snapshot
	rf.log = rf.log[indexInLiveLog+1:]
	/*
		should we save to disk then update states in memory?
		not necessary, if saving to disk failed, then the server should panic
	*/
	rf.snapshot2DPrintf("insideSnapshot(): finished : lastIndex: %v, log: %v\n", lastIncludedIndex, rf.log)
	return indexInLiveLog
}

/*
log compaction can only delete the applied log entries,
if the leader's log has too many unapplied log entries,
log compaction won't help, we need a mechanism to slow down the server so that log doesn't grow with no boundaries.
need
a size limit for uncommitted log entries, (TODO later)
a size limit for unapplied log entries.	(TODO later)
a size limit for applied log entries,
*/

/*
when sneding snapshot, do we need to send the logs following the snapshot? NO, the logic is repeated with sendAppendEntries

SendSnapshot() will be called by a harvester
when the nextIndices[server] goes below (≤) lastIndex in snapshot,

SendSnapshot() is called by a harvester, the harvester is waiting for a reply.
SendSnapshot() guarantees to have exactly one reply back to the caller.
SendSnapshot() will check leader condition,
if not met, returns false reply,
if RPC failed, including failed server, retries infinitely.
if ok, returns the reply for further processing
*/
func (rf *Raft) SendSnapshot(server int, replyChan chan<- SendSnapshotReply) {
	rf.snapshot2DPrintf("SendSnapshot(): called with %v\n", server)
	msg := SendSnapshotReply{
		Installed: false,
	}
	for {

		rf.lockMu("SendSnapshot() with server: %v\n", server)
		if rf.killed() || rf.role != LEADER {
			rf.unlockMu()
			break
		}
		args := SendSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapshotLastIndex,
			LastIncludedTerm:  rf.snapshotLastTerm,
			Snapshot:          rf.snapshot,
		}
		rf.unlockMu()
		reply := SendSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		if ok {
			msg = reply
			break
		}
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)
	}
	replyChan <- msg
	rf.snapshot2DPrintf("SendSnapshot(): finished with msg: %v\n", msg)
}

func (rf *Raft) InstallSnapshot(args *SendSnapshotArgs, reply *SendSnapshotReply) {
	rf.tempDPrintf("InstallSnapshot(): called with args: %v\n", *args)

	rf.lockMu("InstallSnapshot() with args: %v\n", args)
	defer rf.unlockMu()
	reply.Term = rf.currentTerm
	reply.Server = rf.me

	// if args.Term < server.Term:
	// installed=false reply with server.Term
	if args.Term < rf.currentTerm {
		reply.Installed = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.onReceiveHigherTerm(args.Term)
		rf.currentLeader = args.LeaderId
	}
	// if args doesn't have more up-to-date snapshot:
	// installed=false reply with the server's LastIncludedIndex and LastIncludedTerm
	if rf.snapshotLastIndex >= args.LastIncludedIndex {
		reply.Installed = false
		reply.LastIncludedIndex = rf.snapshotLastIndex
		reply.LastIncludedTerm = rf.snapshotLastTerm
		rf.persistState("InstallSnapshot() args: %v", args)
		return
	}

	// the snapshot is accepted
	rf.tempDPrintf("InstallSnapshot(): snapshot is accepted: args.LeaderId: %v, args.Term: %v, args.LastIncludedIndex: %v, args.LastIncludedTerm: %v\n", args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	reply.Installed = true
	reply.LastIncludedIndex = args.LastIncludedIndex
	reply.LastIncludedTerm = args.LastIncludedTerm

	lastIndex := rf.findEntryWithIndexInLog(args.LastIncludedIndex, rf.log, rf.snapshotLastIndex)
	// lastIndex can't be -1
	rf.snapshot = args.Snapshot
	rf.snapshotLastIndex = args.LastIncludedIndex
	rf.snapshotLastTerm = args.LastIncludedTerm

	if lastIndex == len(rf.log) || rf.log[lastIndex].Term != args.LastIncludedTerm {
		// 	if server has no entry or has conflicting entry at LastIncludedIndex:
		// installed=true, delete all log entries and previous snapshot, reset with args.snapshot
		// update commitIndex
		rf.log = make([]LogEntry, 0)
	} else {
		// 	if server has the same entry at LastIncludedIndex:
		// keep all log entries following LastIncludedIndex
		// installed=true
		rf.log = rf.log[lastIndex+1:]
	}
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.persistState("InstallSnapshot(): args: %v", args)
	go rf.ApplySnapshot()
	rf.tempDPrintf("InstallSnapshot(): finished: log: %v\n", rf.log)
}

/*
send the snapshot,
this function doesn't check leader condition,
the send function will do it, and promise to give back a reply
this function checks the reply,
if contacting a server with higher term, becomes a follower
if installed, updating nextIndices[Server]
*/
func (rf *Raft) SendAndHarvestSnapshot(server int) {
	rf.snapshot2DPrintf("SendAndHarvestSnapshot(): called with %v\n", server)
	replyChan := make(chan SendSnapshotReply)
	go rf.SendSnapshot(server, replyChan)
	reply := <-replyChan
	rf.snapshot2DPrintf("SendAndHarvestSnapshot(): %v reply: %v\n", server, reply)

	rf.lockMu("SendAndHarvestSnapshot() with server: %v\n", server)
	defer rf.unlockMu()
	if !reply.Installed && reply.Term > rf.currentTerm {
		rf.onReceiveHigherTerm(reply.Term)
		rf.persistState("SendAndHarvestSnapshot(): %v", server)
		return
	}
	if reply.Installed {
		if rf.nextIndices[server] <= reply.LastIncludedIndex {
			rf.nextIndices[server] = reply.LastIncludedIndex + 1
		}
	}
}

func (rf *Raft) signalSnapshot() bool {
	rf.snapshotDPrintf("SendSnapshot(): is called\n")
	killed := false
	received := false
	sent := false
	for !killed && !sent {
		select {
		case rf.SignalSnapshot <- 1:
			sent = true
		case <-time.After(time.Duration(HBTIMEOUT) * time.Millisecond):
			if rf.killed() {
				killed = true
			}
		}
	}
	rf.snapshotDPrintf("SendSnapshot(): sent the signal\n")
	for !killed && !received {
		select {
		case snapshot := <-rf.SnapshotChan:
			rf.insideSnapshot(snapshot.LastIncludedIndex, snapshot.Data)
			received = true
		case <-time.After(time.Duration(HBTIMEOUT) * time.Millisecond):
			if rf.killed() {
				killed = true
			}
		}
	}
	rf.snapshotDPrintf("SendSnapshot(): received the snapshot\n")
	rf.snapshot2DPrintf("SendSnapshot(): finished: %v\n", sent && received)
	return sent && received
}
