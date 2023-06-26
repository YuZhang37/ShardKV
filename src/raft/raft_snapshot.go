package raft

import (
	"log"
	"time"
)

/*
TestSnapshotBasic2D
TestSnapshotInstall2D
TestSnapshotInstallUnreliable2D
TestSnapshotInstallCrash2D
TestSnapshotInstallUnCrash2D
TestSnapshotAllCrash2D
TestSnapshotInit2D
*/

/*
the service says it has created a snapshot that has
all info up to and including index. this means the
service no longer needs the log through (and including)
that index. Raft should now trim its log as much as possible.
index is always ≤ the commit index on this server.
*/
func (rf *Raft) Snapshot(lastIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if lastIndex <= 0 || lastIndex > rf.commitIndex {
		log.Fatalf("Snapshot Error: lastIndex %v, commitIndex: %v\n", lastIndex, rf.commitIndex)
	}
	/*
		stale snapshot: < rf.snapshotLastIndex
		when the server receives a new snapshot from the leader, and updated its state, and before this new snapshot is applied to the service, the service takes a snapshot, this snapshot is stale
		repeated snapshot: = rf.snapshotLastIndex
		the server takes snapshot too soon
	*/
	if lastIndex <= rf.snapshotLastIndex {
		return
	}
	indexInLiveLog := rf.findEntryWithIndexInLog(lastIndex, rf.log, rf.snapshotLastIndex)
	if indexInLiveLog >= len(rf.log) || indexInLiveLog < 0 {
		log.Fatalf("Snapshot Error: indexInLiveLog: %v, log %v doesn't contain index: %v\n", indexInLiveLog, rf.log, lastIndex)
	}
	rf.snapshotLastIndex = lastIndex
	rf.snapshotLastTerm = rf.log[indexInLiveLog].Term
	rf.snapshot = snapshot
	rf.log = rf.log[indexInLiveLog+1:]
	/*
		should we save to disk then update states in memory?
		not necessary, if saving to disk failed, then the server should panic
	*/
	rf.persistStateWithSnapshot("Snapshot() lastIncludedIndex: %v, indexInLiveLog in log: %v", lastIndex, indexInLiveLog)
	TempDPrintf("Snapshot(): lastIndex: %v, log: %v\n", lastIndex, rf.log)
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
}

func (rf *Raft) InstallSnapshot(args *SendSnapshotArgs, reply *SendSnapshotReply) {
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
		rf.persistStateWithSnapshot("InstallSnapshot() args: %v", args)
		return
	}

	rf.snapshot = args.Snapshot
	lastIndex := rf.findEntryWithIndexInLog(args.LastIncludedIndex, rf.log, rf.snapshotLastIndex)
	if lastIndex == len(rf.log) || rf.log[lastIndex].Term != args.LastIncludedTerm {
		// 	if server has no entry or has conflicting entry at LastIncludedIndex:
		// installed=true, delete all log entries and previous snapshot, reset with args.snapshot
		// update commitIndex
		rf.log = make([]LogEntry, 0)
	} else {
		// 	if server has the same entry at LastIncludedIndex:
		// keep all log entries following LastIncludedIndex
		// installed=true
		rf.log = rf.log[args.LastIncludedIndex+1:]
	}
	rf.persistStateWithSnapshot("InstallSnapshot() args: %v", args)
	reply.Installed = true
	reply.LastIncludedIndex = args.LastIncludedIndex
	reply.LastIncludedTerm = args.LastIncludedTerm
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
	replyChan := make(chan SendSnapshotReply)
	go rf.SendSnapshot(server, replyChan)
	reply := <-replyChan
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !reply.Installed && reply.Term > rf.currentTerm {
		rf.onReceiveHigherTerm(reply.Term)
		rf.persistStateWithSnapshot("SendAndHarvestSnapshot(): %v", server)
		return
	}
	if reply.Installed {
		if rf.nextIndices[server] <= reply.LastIncludedIndex {
			rf.nextIndices[server] = reply.LastIncludedIndex + 1
		}
	}
}
