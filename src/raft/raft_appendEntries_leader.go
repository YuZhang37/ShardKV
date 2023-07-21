package raft

import (
	"log"
	"time"
)

func (rf *Raft) reachConsensus(index int) {

	rf.lockMu("1 reachConsensus() with index: %v\n", index)
	KVStoreDPrintf("Server: %v reachConsensus at index: %v,, log: %v, commitIndex: %v\n", rf.me, index, rf.log, rf.commitIndex)
	if rf.killed() || rf.role != LEADER {
		rf.unlockMu()
		return
	}
	if rf.commitIndex >= index {
		rf.unlockMu()
		return
	}

	numOfPeers := len(rf.peers)
	// channel for harvesters to transmit results to ReachConsensus
	ch0 := make(chan AppendEntriesReply)
	// channels for ReachConsensus thread to terminate harvesters
	stopChans := make([]chan int, numOfPeers)
	for i := 0; i < len(stopChans); i++ {
		stopChans[i] = make(chan int)
	}
	for i := 0; i < numOfPeers; i++ {
		if i == rf.me {
			continue
		}
		// channel for sender to transmit the result to harvesters
		ch := make(chan AppendEntriesReply)
		go rf.sendAppendEntries(i, index, ch)
		go rf.harvestAppendEntriesReply(index, ch0, stopChans[i], ch)
	}
	rf.unlockMu()

	timerChan := make(chan int)
	quitTimerChan := make(chan int)
	go rf.checkCommitTimeOut(quitTimerChan, timerChan)

	// rf.Commit may never reply
	committed, harvestedServers := rf.commit(ch0, timerChan, index)
	// stop the timer thread
	go func(ch chan int) {
		ch <- 1
	}(quitTimerChan)
	go rf.quitBlockedHarvests(numOfPeers, harvestedServers, stopChans)

	rf.lockMu("2 reachConsensus() with index: %v\n", index)
	if committed {
		KVStoreDPrintf("Server: %v succeeded the reachConsensus at index: %v, commitIndex: %v, log: %v.\n", rf.me, index, rf.commitIndex, rf.log)
		go rf.ApplyCommand(index)
	} else {
		KVStoreDPrintf("Server: %v failed the reachConsensus at index: %v, commitIndex: %v, log: %v.\n", rf.me, index, rf.commitIndex, rf.log)
	}
	rf.unlockMu()
}

/*
returns a map containing all handled harvests,
all handled harvests are guaranteed to be not blocked,
all unhandled harvests can send requests at most one more time (RPC timeout time) and get blocked, then all replies will be redirected to global harvest.
*/
func (rf *Raft) commit(ch0 chan AppendEntriesReply, timerChan chan int, entryIndex int) (bool, map[int]bool) {

	// the leader has appended the entry
	successCount := 1
	tryCommit := true
	committed := false
	harvestedServers := make(map[int]bool)

	for tryCommit {
		select {
		case reply := <-ch0:
			harvestedServers[reply.Server] = true

			rf.lockMu("1 commit() with entryIndex: %v\n", entryIndex)
			if rf.killed() || rf.role != LEADER {
				tryCommit = false
			} else {
				tryCommit, committed, successCount = rf.onReceivingAppendEntriesReply(&reply, successCount, entryIndex)
			}
			rf.unlockMu()
		case <-timerChan:

			rf.lockMu("2 commit() with entryIndex: %v\n", entryIndex)
			if rf.killed() || rf.role != LEADER {
				tryCommit = false
			} else {
				rf.updateCommit(entryIndex)
				// start(comm2) may commit a higher index
				if rf.commitIndex >= entryIndex {
					tryCommit = false
					committed = true
				}
			}
			rf.unlockMu()
		}
	}
	return committed, harvestedServers
}

/*
this function will guarantee to send to ch exact one reply, the longest time it takes is the RPC timeout.
for heartbeat entries will be empty, issueEntryIndex is -1
for appendEntries, entries will log[rf.nextIndices[server]-1:]
it doesn't check if a higher issueEntryIndex is sent out
but it needs to check if the server still the leader or if it's killed
*/
func (rf *Raft) sendAppendEntries(server int, issueEntryIndex int, ch chan AppendEntriesReply) {
	fakeReply := rf.getFakeAppendEntriesReply(server, issueEntryIndex)

	rf.lockMu("sendAppendEntries() with server: %v, issueEntryIndex: %v\n", server, issueEntryIndex)
	if rf.killed() || rf.role != LEADER {
		// this reply will be dropped in tryCommit or in HarvestAppendEntriesReply
		rf.unlockMu()
		ch <- fakeReply
		return
	}

	if rf.latestIssuedEntryIndices[server] < issueEntryIndex {
		rf.latestIssuedEntryIndices[server] = issueEntryIndex
	}
	funct := 1
	if issueEntryIndex < 0 {
		funct = 2
	}
	next := rf.nextIndices[server]
	AppendEntries2DPrintf(funct, "next index to %v is %v \n", server, next)
	if next < 1 {
		rf.unlockMu()
		log.Fatalf("fatal: next index to %v is %v \n", server, next)
	}

	if next <= rf.snapshotLastIndex {
		// will be retried later
		rf.unlockMu()
		ch <- fakeReply
		return
	}
	args := rf.getAppendEntriesArgs(server, next, issueEntryIndex)
	AppendEntries2DPrintf(funct, "args: %v at %v to %v\n", args, rf.me, server)
	AppendEntries2DPrintf(funct, "log: %v at %v \n", rf.log, rf.me)
	// targetServer := rf.peers[server]
	rf.unlockMu()
	reply := AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	// test shows race condition here once in 30 tests for TestOnePartition3A
	// ok := targetServer.Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		// RPC call failed
		ch <- fakeReply
		return
	}
	ch <- reply
}

/*
although harvest may issue lots of senders
but at any point in time, there is exactly one sender and one harvester
and sender is ensured to send to the channel exactly once
handle retries
*/
func (rf *Raft) harvestAppendEntriesReply(issuedIndex int, sendReplyChan chan AppendEntriesReply, stopChan chan int, getReplyChan chan AppendEntriesReply) {
	for {
		reply := <-getReplyChan

		rf.lockMu("1 harvestAppendEntriesReply() with issuedIndex: %v\n", issuedIndex)
		hasHigherIndex := rf.latestIssuedEntryIndices[reply.Server] > reply.IssueEntryIndex
		isLeader := rf.role == LEADER

		if reply.Success || reply.HigherTerm || hasHigherIndex || rf.killed() || !isLeader {
			rf.unlockMu()
			AppendEntriesDPrintf("reply from server %v: %v waiting for sending", reply.Server, reply)
			select {
			case sendReplyChan <- reply:
				AppendEntriesDPrintf("reply from server %v sends to replyChan", reply.Server)
			case <-stopChan:

				rf.lockMu("2 harvestAppendEntriesReply() with issuedIndex: %v\n", issuedIndex)
				AppendEntriesDPrintf("reply from server %v sends to trailing", reply.Server)
				if reply.Success && rf.currentTerm == reply.Term && rf.role == LEADER && !rf.killed() {
					rf.unlockMu()
					rf.trailingReplyChan <- reply
				} else {
					rf.unlockMu()
				}
			}
			break
		} else {
			// retry
			// need to change nextIndices[server] if mismatched
			if reply.MisMatched {
				rf.updateNextIndicesOnMisMatch(reply)
			}
			if rf.nextIndices[reply.Server] <= rf.snapshotLastIndex {
				go rf.SendAndHarvestSnapshot(reply.Server)
				// log.Printf("call rf.SendAndHarvestSnapshot(reply.Server), need to de-dup")
			}
			go rf.sendAppendEntries(reply.Server, issuedIndex, getReplyChan)
			rf.unlockMu()
		}
		time.Sleep(time.Duration(REAPPENDTIMEOUT) * time.Millisecond)
	}
}

/*
returns a map containing all handled harvests,
all handled harvests are guaranteed to be not blocked,
all unhandled harvests can send requests at most one more time (RPC timeout time) and get blocked, then all replies will be redirected to global harvest.

blocked
it can have infinite loop:
when the leader keeps valid, and the last entry
fails to append on the majority of the followers

when tryCommit returns, the command may or may not committed
the leader may become a follower

the higherTerm reply may redirected to global harvest,
but it will be dropped and won't be processed.

func (rf *Raft) commit(ch0 chan AppendEntriesReply, timerChan chan int, entryIndex int) (bool, map[int]bool)
*/

/*
although harvest may issue lots of senders
but at any point in time, there is exactly one sender and one harvester
and sender is ensured to send to the channel exactly once
handle retries

	get reply and handle retries
	assume we can always get exactly one reply from getReplyChan
	if rf is killed or no longer the leader,
		the reply is dropped, no retries
		won't block AppendCommand(), it has timerChan
	if issued index > current index, no retries
	if the index is committed, send the result to global harvest channel

	send to sendReplyChan only if:
		1. reply.success is true
		2. reply.success is false but HigherTerm is true
	if reply.success is false and HigherTerm is false
		retries

reply.sucess:

	if reply.false, the longest time it takes from sending to this point is when RPC call fails,
	the timeout time of that.
	even reply.true, it can take that long as well
	so we can't close the chan before that.
	We close this channel when this server gets elected again, but if this happens too soon,
	reply can still send to a closed channel, the server will panic, other servers can continue election.
	RPC timeout should smaller than election timeout to avoid closed channel causing false fails.

if reply.MisMatched:

suppose AppendCommand(comm1), AppendCommand(comm2)
rf.nextIndices[server] can be higher or lower than issuedIndex-1
if rf.nextIndices[reply.Server] != reply.NextIndex,
it means comm0 or comm2 may have updated nextIndices[Server]
this reply conveys outdated info, can't be used to update nextIndices[Server]
func (rf *Raft) harvestAppendEntriesReply(
*/
