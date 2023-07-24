package raft

import "time"

func (rf *Raft) ApplySnapshot() {

	rf.lockMu("ApplySnapshot()")
	rf.logRaftStateForInstallSnapshot("ApplySnapshot()")
	rf.snapshot2DPrintf("ApplySnapshot(): called with lastIncludedIndex: %v, lastIncludedTerm: %v\n", rf.snapshotLastIndex, rf.snapshotLastTerm)
	rf.kvStoreDPrintf("ApplySnapshot(): ApplySnapshot() is called with lastIncludedIndex: %v, lastIncludedTerm: %v\n", rf.snapshotLastIndex, rf.snapshotLastTerm)
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastTerm,
		SnapshotIndex: rf.snapshotLastIndex,
	}
	rf.unlockMu()

	rf.lockApplied("ApplySnapshot()")
	defer rf.unlockApplied()
	rf.snapshot2DPrintf("ApplySnapshot(): lastApplied %v\n before applying", rf.lastApplied)
	if rf.lastApplied >= int32(msg.SnapshotIndex) {
		return
	}
	// clear pending msg
	rf.pendingMsg = make(map[int]ApplyMsg)
	rf.applyCh <- msg
	rf.snapshot2DPrintf("ApplySnapshot():  lastApplied %v, after applying\n", rf.lastApplied)
	if rf.lastApplied < int32(msg.SnapshotIndex) {
		value := rf.lastApplied
		rf.lastApplied = int32(msg.SnapshotIndex)
		rf.snapshot2DPrintf("ApplySnapshot(): lastApplied %v is updated to %v \n", value, rf.lastApplied)
	}
	rf.snapshot2DPrintf("***** ApplySnapshot(): finished******\n")
	rf.kvStoreDPrintf("***** ApplySnapshot(): finished******\n")
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
	rf.kvStoreDPrintf("insideApplyCommand(): called with issuedIndex: %v, locked: %v, nextAppliedIndex: %v\n", issuedIndex, locked, nextAppliedIndex)
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
		if !locked {
			rf.unlockMu()
		}
		rf.lockApplied("2 insideApplyCommand() with issuedIndex: %v, locked: %v", issuedIndex, locked)
		if _, exists := rf.pendingMsg[msg.CommandIndex]; !exists {
			rf.tempDPrintf("insideApplyCommand(): adds %v\n", msg)
			rf.pendingMsg[msg.CommandIndex] = msg
			go func() {
				rf.orderedDeliveryChan <- ApplyMsg{}
			}()
		}
		rf.unlockApplied()
	}
	rf.kvStoreDPrintf("insideApplyCommand(): finished with issuedIndex: %v, locked: %v\n", issuedIndex, locked)
}

func (rf *Raft) OrderedCommandDelivery() {
	rf.applyCommandDPrintf("OrderedCommandDelivery(): gets running....\n")
	for range rf.orderedDeliveryChan {
		rf.lockApplied("OrderedCommandDelivery()")
		nextApplied := int(rf.lastApplied + 1)
		msg, exists := rf.pendingMsg[nextApplied]
		rf.applyCommandDPrintf("OrderedCommandDelivery(): msg: %v, exists: %v, endingMsg: %v\n", msg, exists, rf.pendingMsg)
		for exists {
			rf.lastApplied++
			delete(rf.pendingMsg, nextApplied)
			rf.applyCommandDPrintf("OrderedCommandDelivery():  applies index: %v.\n", msg.CommandIndex)
			if !rf.sendMsgToChan(&msg) {
				break
			}
			nextApplied = int(rf.lastApplied + 1)
			msg, exists = rf.pendingMsg[nextApplied]
			rf.applyCommandDPrintf("OrderedCommandDelivery(): msg: %v, exists: %v, pendingMsg: %v\n", msg, exists, rf.pendingMsg)
		}
		rf.applyCommandDPrintf("OrderedCommandDelivery(): rf.lastApplied: %v, OrderedCommandDelivery finished one round.\n", rf.lastApplied)
		rf.unlockApplied()
		rf.lockMu("OrderedCommandDelivery()")
		rf.logRaftStateForInstallSnapshot("OrderedCommandDelivery()")
		rf.unlockMu()
	}
}

func (rf *Raft) sendMsgToChan(msg *ApplyMsg) bool {
	for !rf.killed() {
		select {
		case rf.applyCh <- *msg:
			rf.applyCommandDPrintf("sendMsgToChan() with msg: %v is sent to application\n", msg)
			return true
		case <-time.After(time.Duration(CHECKAPPLIEDTIMEOUT) * time.Millisecond):
		}
	}
	rf.applyCommandDPrintf("sendMsgToChan() with msg: %v is killed\n", msg)
	return true
}
