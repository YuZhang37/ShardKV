package raft

// "log"

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
		TempDPrintf("server %v sends %v\n", rf.me, msg)
		// rf.pendingMsg[msg.CommandIndex] = msg
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.IncrementLastApplied(nextAppliedIndex - 1)
		// if _, exists := rf.pendingMsg[msg.CommandIndex]; !exists {
		// 	TempDPrintf("server %v sends %v\n", rf.me, msg)
		// 	rf.pendingMsg[msg.CommandIndex] = msg
		// 	rf.mu.Unlock()
		// 	rf.applyCh <- msg
		// 	rf.mu.Lock()
		// 	if rf.lastApplied == int32(msg.CommandIndex-1) {
		// 		rf.lastApplied++
		// 	}
		// 	delete(rf.pendingMsg, msg.CommandIndex)
		// 	// rf.IncrementLastApplied(nextAppliedIndex)

		// } else {
		// 	rf.mu.Unlock()
		// }
	}
}

func (rf *Raft) ApplyCommand2(issuedIndex int) {
	for nextAppliedIndex := int(rf.GetLastApplied() + 1); nextAppliedIndex <= issuedIndex; nextAppliedIndex++ {
		rf.mu.Lock()
		indexInLiveLog := rf.findEntryWithIndexInLog(nextAppliedIndex, rf.log, rf.snapshotLastIndex)
		if indexInLiveLog < 0 || rf.log[indexInLiveLog].Index <= int(rf.lastApplied) {
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
		TestDPrintf("server %v sends %v\n", rf.me, msg)
		rf.mu.Unlock()
		rf.orderedDeliveryChan <- msg
	}
}

func (rf *Raft) OrderedCommandDelivery() {
	TempDPrintf("server: %v, OrderedCommandDelivery gets running....", rf.me)
	// for msg := range rf.orderedDeliveryChan {
	// 	TempDPrintf("server: %v, OrderedCommandDelivery gets index: %v.\n", rf.me, msg.CommandIndex)
	// 	if msg.CommandIndex <= int(rf.GetLastApplied()) {
	// 		TempDPrintf("server: %v, index: %v is skipped\n.", rf.me, msg.CommandIndex)
	// 		continue
	// 	}
	// 	_, exists := rf.pendingMsg[msg.CommandIndex]
	// 	if exists {
	// 		continue
	// 	}
	// 	rf.pendingMsg[msg.CommandIndex] = msg
	// 	for nextMsg, nextMsgExists := rf.pendingMsg[int(rf.GetLastApplied())+1]; nextMsgExists; nextMsg, nextMsgExists = rf.pendingMsg[int(rf.GetLastApplied())+1] {
	// 		rf.applyCh <- nextMsg
	// 		delete(rf.pendingMsg, int(rf.GetLastApplied())+1)
	// 		rf.IncrementLastApplied()
	// 	}
	// }
}
