package raft

import "fmt"

func (rf *Raft) ApplySnapshot() {

	rf.lockMu("ApplySnapshot()")
	rf.logRaftStateForInstallSnapshot("ApplySnapshot()")
	Snapshot2DPrintf("server: %v, ApplySnapshot() is called with lastIncludedIndex: %v, lastIncludedTerm: %v\n", rf.me, rf.snapshotLastIndex, rf.snapshotLastTerm)
	KVStoreDPrintf("server: %v, ApplySnapshot() is called with lastIncludedIndex: %v, lastIncludedTerm: %v\n", rf.me, rf.snapshotLastIndex, rf.snapshotLastTerm)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastTerm,
		SnapshotIndex: rf.snapshotLastIndex,
	}
	rf.unlockMu()

	rf.lockApplied("ApplySnapshot()")
	defer rf.unlockApplied()
	Snapshot2DPrintf("server: %v, lastApplied %v\n before applying", rf.me, rf.lastApplied)
	if rf.lastApplied >= int32(msg.SnapshotIndex) {
		return
	}
	// clear pending msg
	rf.pendingMsg = make(map[int]ApplyMsg)
	rf.applyCh <- msg
	Snapshot2DPrintf("server: %v, lastApplied %v, after applying\n", rf.me, rf.lastApplied)
	if rf.lastApplied < int32(msg.SnapshotIndex) {
		value := rf.lastApplied
		rf.lastApplied = int32(msg.SnapshotIndex)
		Snapshot2DPrintf("server: %v, lastApplied %v is updated to %v \n", rf.me, value, rf.lastApplied)
	}
	Snapshot2DPrintf("*****server: %v, ApplySnapshot() finished******\n", rf.me)
	KVStoreDPrintf("*****server: %v, ApplySnapshot() finished******\n", rf.me)
}

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand(issuedIndex int) {
	rf.insideApplyCommand(issuedIndex, false)
}

/*
locked indicates if this function is called within rf.mu lock
*/
func (rf *Raft) insideApplyCommand(issuedIndex int, locked bool) {
	rf.lockApplied("1 insideApplyCommand() with issuedIndex: %v, locked: %v", issuedIndex, locked)
	nextAppliedIndex := int(rf.lastApplied + 1)
	KVStoreDPrintf("rf.gid: %v, rf.me: %v, insideApplyCommand() is called with issuedIndex: %v, locked: %v, nextAppliedIndex: %v\n", rf.gid, rf.me, issuedIndex, locked, nextAppliedIndex)
	rf.unlockApplied()
	for ; nextAppliedIndex <= issuedIndex; nextAppliedIndex++ {
		// prepare msg
		if !locked {
			rf.lockMu("insideApplyCommand() with issuedIndex: %v, locked: %v\n", issuedIndex, locked)

		}
		indexInLiveLog := rf.findEntryWithIndexInLog(nextAppliedIndex, rf.log, rf.snapshotLastIndex)
		if indexInLiveLog < 0 {
			// this entry has been merged, but not applied: error
			if !locked {
				rf.unlockMu()
			}
			continue
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[indexInLiveLog].Command,
			CommandIndex: rf.log[indexInLiveLog].Index,
			CommandTerm:  rf.log[indexInLiveLog].Term,
		}
		rf.logRaftStateForInstallSnapshot(fmt.Sprintf("insideApplyCommand(): msg: %v\n", msg))
		if !locked {
			rf.unlockMu()
		}
		rf.lockApplied("2 insideApplyCommand() with issuedIndex: %v, locked: %v", issuedIndex, locked)
		if _, exists := rf.pendingMsg[msg.CommandIndex]; !exists {
			Snapshot2DPrintf("server %v adds %v\n", rf.me, msg)
			ApplyCommandDPrintf("server %v adds %v\n", rf.me, msg)
			rf.debugInstallSnapshot("sends command to delivery: %v\n", msg)
			rf.pendingMsg[msg.CommandIndex] = msg
			go func() {
				rf.orderedDeliveryChan <- ApplyMsg{}
			}()
		}
		rf.unlockApplied()
	}
	KVStoreDPrintf("rf.gid: %v, rf.me: %v, insideApplyCommand() is finished with issuedIndex: %v, locked: %v\n", rf.gid, rf.me, issuedIndex, locked)
}

func (rf *Raft) OrderedCommandDelivery() {
	ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, OrderedCommandDelivery gets running....\n", rf.gid, rf.me)
	for range rf.orderedDeliveryChan {
		rf.lockApplied("OrderedCommandDelivery()")
		nextApplied := int(rf.lastApplied + 1)
		msg, exists := rf.pendingMsg[nextApplied]
		ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, msg: %v, exists: %v\n", rf.gid, rf.me, msg, exists)
		ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, pendingMsg: %v\n", rf.gid, rf.me, rf.pendingMsg)
		for exists {
			rf.lastApplied++
			delete(rf.pendingMsg, nextApplied)
			Snapshot2DPrintf("server: %v, applies index: %v.\n", rf.me, msg.CommandIndex)
			ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, applies index: %v.\n", rf.gid, rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			nextApplied = int(rf.lastApplied + 1)
			msg, exists = rf.pendingMsg[nextApplied]
			ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, msg: %v, exists: %v\n", rf.gid, rf.me, msg, exists)
			ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, pendingMsg: %v\n", rf.gid, rf.me, rf.pendingMsg)
		}
		ApplyCommandDPrintf("rf.gid: %v, rf.me: %v, rf.lastApplied: %v, OrderedCommandDelivery finished one round.\n", rf.gid, rf.me, rf.lastApplied)
		rf.unlockApplied()
		rf.lockMu("OrderedCommandDelivery()")
		rf.logRaftStateForInstallSnapshot("OrderedCommandDelivery()")
		rf.unlockMu()
	}
}
