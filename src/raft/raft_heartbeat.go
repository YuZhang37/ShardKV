package raft

import (
	"time"
)

// for heartbeat: send and harvest is 1 on 1, and send part definitely sends to the channel, no need for stopChan
func (rf *Raft) HeartBeat(server int) {
	replyChan := make(chan AppendEntriesReply)
	for !rf.killed() && rf.isLeader() {
		go rf.sendAppendEntries(server, -1, replyChan)
		go rf.harvestHeartbeatReply(replyChan)
		time.Sleep(time.Duration(rf.hbTimeOut) * time.Millisecond)
	}
}

/*
heartbeat will only update the followers's commitIndex
and update the term on leader if a reply with higher term
don't handle the failed RPC calls and mismatched logs
*/
func (rf *Raft) harvestHeartbeatReply(replyChan chan AppendEntriesReply) {
	reply := <-replyChan
	if rf.killed() || !rf.isLeader() {
		return
	}
	// only deals with replies with higher terms
	if reply.HigherTerm {
		rf.mu.Lock()
		rf.msgReceived = false
		originalTerm := rf.onReceiveHigherTerm(reply.Term)
		rf.persistState("server %v heartbeat replies on %v with higher term: %v, original term: %v", rf.me, reply.Server, reply.Term, originalTerm)
		rf.mu.Unlock()
	}
}
