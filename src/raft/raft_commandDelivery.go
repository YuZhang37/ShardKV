package raft

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand(issuedIndex int) {
	for nextAppliedIndex := int(rf.GetLastApplied() + 1); nextAppliedIndex <= issuedIndex; nextAppliedIndex++ {
		rf.mu.Lock()
		indexInLiveLog := rf.findEntryWithIndexInLog(nextAppliedIndex, rf.log, rf.snapshotLastIndex)
		if indexInLiveLog < 0 || rf.log[indexInLiveLog].Index <= int(rf.GetLastApplied()) {
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
		if _, exists := rf.pendingMsg[msg.CommandIndex]; !exists {
			ApplyCommandDPrintf("server %v adds %v\n", rf.me, msg)
			rf.pendingMsg[msg.CommandIndex] = msg
			rf.mu.Unlock()
			rf.orderedDeliveryChan <- msg
			// rf.IncrementLastApplied(msg.CommandIndex - 1)
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) OrderedCommandDelivery() {
	ApplyCommandDPrintf("server: %v, OrderedCommandDelivery gets running....", rf.me)
	for range rf.orderedDeliveryChan {
		rf.mu.Lock()
		nextApplied := int(rf.GetLastApplied() + 1)
		msg, exists := rf.pendingMsg[nextApplied]
		for exists {
			rf.IncrementLastApplied(nextApplied - 1)
			delete(rf.pendingMsg, nextApplied)
			rf.mu.Unlock()
			ApplyCommandDPrintf("server: %v, applies index: %v.\n", rf.me, msg.CommandIndex)
			rf.applyCh <- msg
			rf.mu.Lock()
			nextApplied = int(rf.GetLastApplied() + 1)
			msg, exists = rf.pendingMsg[nextApplied]
		}
		rf.mu.Unlock()
	}
}
