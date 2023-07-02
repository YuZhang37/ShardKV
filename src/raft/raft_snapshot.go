package raft

import (
	"log"
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	Snapshot2DPrintf("server: %v, Snapshot() is called with lastIncludedIndex %v,\n", rf.me, lastIncludedIndex)
	indexInLiveLog := rf.insideSnapshot(lastIncludedIndex, snapshot)
	if indexInLiveLog == -1 {
		return
	}
	rf.persistState("Server %v, Snapshot() lastIncludedIndex: %v, indexInLiveLog in log: %v", rf.me, lastIncludedIndex, indexInLiveLog)
}

/*
the same as Snapshot, except that
1. rf.mu is held in the calling function
2. state and snapshot is not persisted
*/
func (rf *Raft) insideSnapshot(lastIncludedIndex int, snapshot []byte) int {
	Snapshot2DPrintf("server: %v, insideSnapshot() is called with lastIncludedIndex %v,\n", rf.me, lastIncludedIndex)

	if lastIncludedIndex <= 0 || lastIncludedIndex > rf.commitIndex {
		log.Fatalf("Snapshot Error: lastIndex %v, commitIndex: %v\n", lastIncludedIndex, rf.commitIndex)
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
		log.Fatalf("Snapshot Error: indexInLiveLog: %v, log %v doesn't contain index: %v\n", indexInLiveLog, rf.log, lastIncludedIndex)
	}
	rf.snapshotLastIndex = lastIncludedIndex
	rf.snapshotLastTerm = rf.log[indexInLiveLog].Term
	rf.snapshot = snapshot
	rf.log = rf.log[indexInLiveLog+1:]
	/*
		should we save to disk then update states in memory?
		not necessary, if saving to disk failed, then the server should panic
	*/
	Snapshot2DPrintf("Server %v, insideSnapshot() finished : lastIndex: %v, log: %v\n", rf.me, lastIncludedIndex, rf.log)
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
	Snapshot2DPrintf("server: %v, SendSnapshot() is called with %v\n", rf.me, server)
	msg := SendSnapshotReply{
		Installed: false,
	}
	for {
		rf.mu.Lock()
		if rf.killed() || rf.role != LEADER {
			rf.mu.Unlock()
			break
		}
		args := SendSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapshotLastIndex,
			LastIncludedTerm:  rf.snapshotLastTerm,
			Snapshot:          rf.snapshot,
		}
		rf.mu.Unlock()
		reply := SendSnapshotReply{}
		ok := rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)
		if ok {
			msg = reply
			break
		}
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)
	}
	replyChan <- msg
	Snapshot2DPrintf("server: %v, SendSnapshot() finished with msg: %v\n", rf.me, msg)
}

func (rf *Raft) InstallSnapshot(args *SendSnapshotArgs, reply *SendSnapshotReply) {
	Snapshot2DPrintf("server: %v, InstallSnapshot() is called with args: %v\n", rf.me, *args)
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
	Snapshot2DPrintf("server: %v, snapshot is accepted: args.LeaderId: %v, args.Term: %v, args.LastIncludedIndex: %v, args.LastIncludedTerm: %v\n", rf.me, args.LeaderId, args.Term, args.LastIncludedIndex, args.LastIncludedTerm)
	// rf.appliedLock.Lock()
	// Snapshot2DPrintf("server: %v, lastApplied: %v, commitIndex: %v\n", rf.me, rf.lastApplied, rf.commitIndex)
	// rf.appliedLock.Unlock()
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
		// rf.appliedLock.Lock()
		// Snapshot2DPrintf("server: %v log matching at %v\n", rf.me, args.LastIncludedIndex)

		// rf.appliedLock.Unlock()
	}
	if args.LastIncludedIndex > rf.commitIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	rf.persistState("InstallSnapshot() args: %v", args)
	go rf.ApplySnapshot()
	Snapshot2DPrintf("server: %v, InstallSnapshot() finished: log: %v\n", rf.me, rf.log)
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
	Snapshot2DPrintf("server: %v, SendAndHarvestSnapshot() is called with %v\n", rf.me, server)
	replyChan := make(chan SendSnapshotReply)
	go rf.SendSnapshot(server, replyChan)
	reply := <-replyChan
	Snapshot2DPrintf("server: %v, SendAndHarvestSnapshot() %v reply: %v\n", rf.me, server, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
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
