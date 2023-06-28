package raft

// import "log"

func (rf *Raft) ApplySnapshot() {
	Snapshot2DPrintf("server: %v, ApplySnapshot() is called with lastIncludedIndex: %v, lastIncludedTerm: %v\n", rf.me, rf.snapshotLastIndex, rf.snapshotLastTerm)
	rf.mu.Lock()
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastTerm,
		SnapshotIndex: rf.snapshotLastIndex,
	}
	rf.mu.Unlock()

	rf.appliedLock.Lock()
	defer rf.appliedLock.Unlock()
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
}

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand(issuedIndex int) {
	rf.appliedLock.Lock()
	nextAppliedIndex := int(rf.lastApplied + 1)
	rf.appliedLock.Unlock()
	for ; nextAppliedIndex <= issuedIndex; nextAppliedIndex++ {
		rf.mu.Lock() // prepare msg
		indexInLiveLog := rf.findEntryWithIndexInLog(nextAppliedIndex, rf.log, rf.snapshotLastIndex)
		if indexInLiveLog < 0 {
			// this entry has been merged, but not applied: error
			rf.mu.Unlock()
			continue
		}
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[indexInLiveLog].Command,
			CommandIndex: rf.log[indexInLiveLog].Index,
			CommandTerm:  rf.log[indexInLiveLog].Term,
		}
		rf.mu.Unlock()
		rf.appliedLock.Lock()
		if _, exists := rf.pendingMsg[msg.CommandIndex]; !exists {
			Snapshot2DPrintf("server %v adds %v\n", rf.me, msg)
			ApplyCommandDPrintf("server %v adds %v\n", rf.me, msg)
			rf.pendingMsg[msg.CommandIndex] = msg
			go func() {
				rf.orderedDeliveryChan <- ApplyMsg{}
			}()
		}
		rf.appliedLock.Unlock()
	}
}

func (rf *Raft) OrderedCommandDelivery() {
	ApplyCommandDPrintf("server: %v, OrderedCommandDelivery gets running....\n", rf.me)
	for range rf.orderedDeliveryChan {
		rf.appliedLock.Lock()
		nextApplied := int(rf.lastApplied + 1)
		msg, exists := rf.pendingMsg[nextApplied]
		ApplyCommandDPrintf("server: %v, msg: %v, exists: %v\n", rf.me, msg, exists)
		ApplyCommandDPrintf("server: %v, pendingMsg: %v\n", rf.me, rf.pendingMsg)
		for exists {
			rf.lastApplied++
			delete(rf.pendingMsg, nextApplied)
			Snapshot2DPrintf("server: %v, applies index: %v.\n", rf.me, msg.CommandIndex)
			ApplyCommandDPrintf("server: %v, applies index: %v.\n", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			nextApplied = int(rf.lastApplied + 1)
			msg, exists = rf.pendingMsg[nextApplied]
			ApplyCommandDPrintf("server: %v, msg: %v, exists: %v\n", rf.me, msg, exists)
			ApplyCommandDPrintf("server: %v, pendingMsg: %v\n", rf.me, rf.pendingMsg)
		}
		rf.appliedLock.Unlock()
		ApplyCommandDPrintf("server: %v, OrderedCommandDelivery finished one round.\n", rf.me)
	}
}
