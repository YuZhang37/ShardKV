package raft

import (
	"log"
)

/*
send the commands which are committed but not applied to ApplyCh
the same command may be sent multiple times, the service will need to de-duplicate the commands when executing them
*/
func (rf *Raft) ApplyCommand(issuedIndex int) {
	TempDPrintf("server %v, issuedIndex: %v\n", rf.me, issuedIndex)
	if !rf.killed() {
		rf.orderedDeliveryChan <- issuedIndex
	}
	// rf.CheckAndDeliverCommand(issuedIndex)
	// timerChan := make(chan int)
	// quitTimerChan := make(chan int)
	// go func(timerChan, quitTimerChan chan int) {
	// 	quit := false
	// 	for !quit {
	// 		time.Sleep(2 * time.Millisecond)
	// 		select {
	// 		case timerChan <- 1:
	// 		case <-quitTimerChan:
	// 			quit = true
	// 		}
	// 	}
	// }(timerChan, quitTimerChan)
	// for !rf.killed() {
	// 	select {
	// 	case rf.orderedDeliveryChan <- issuedIndex:
	// 	case <-timerChan:
	// 	}
	// }
	// go func(quitTimerChan chan int) {
	// 	quitTimerChan <- 1
	// }(quitTimerChan)
}

func (rf *Raft) OrderedCommandDelivery() {
	TempDPrintf("server: %v, OrderedCommandDelivery gets running....", rf.me)
	pendingIndex := rf.lastApplied
	for index := range rf.orderedDeliveryChan {
		TempDPrintf("server: %v, OrderedCommandDelivery gets index: %v\n.", rf.me, index)
		if rf.killed() {
			TempDPrintf("server: %v is killed, OrderedCommandDelivery gets index: %v\n.", rf.me, index)
			break
		}
		if index <= pendingIndex {
			TempDPrintf("server: %v, index: %v is skipped\n.", rf.me, index)
			continue
		} else {
			pendingIndex = index
			rf.CheckAndDeliverCommand(index)
		}
	}
}

func (rf *Raft) CheckAndDeliverCommand(issuedIndex int) {
	rf.mu.Lock()
	nextAppliedIndex := rf.lastApplied + 1
	commitIndex := rf.commitIndex
	indexInLiveLog := rf.findEntryWithIndexInLog(nextAppliedIndex, rf.log, rf.snapshotLastIndex)
	if indexInLiveLog < 0 {
		// this entry has been merged, but not applied: error
		rf.mu.Unlock()
		log.Fatalf("ApplyCommand() Error: targetIndex: %v indexInLiveLog: %v\n", nextAppliedIndex, indexInLiveLog)
	}
	index := indexInLiveLog
	for index < len(rf.log) && nextAppliedIndex <= commitIndex {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.log[index].Command,
			CommandIndex: rf.log[index].Index,
			CommandTerm:  rf.log[index].Term,
		}
		TestDPrintf("server %v applies %v\n", rf.me, msg)
		rf.mu.Unlock()
		rf.applyCh <- msg
		rf.mu.Lock()
		if rf.lastApplied < nextAppliedIndex {
			rf.lastApplied = nextAppliedIndex
			index++
		} else {
			index = rf.findEntryWithIndexInLog(rf.lastApplied+1, rf.log, rf.snapshotLastIndex)
			// index can't be -1, since rf.lastApplied+1 has not been applied yet
		}
		if index < len(rf.log) {
			nextAppliedIndex = rf.log[index].Index
		}
		commitIndex = rf.commitIndex
	}
	rf.mu.Unlock()
}
